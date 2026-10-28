using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>The pipeline launch of a coordinator start of <see cref="ReplicaGroupCommitter" />.</summary>
internal static class ReplicaGroupCommitterStarts
{
    extension(ReplicaGroupCommitter committer)
    {
        /// <summary>Recovers the uncommitted leader tail and creates the coordinator over the new pipeline, closing the pipeline when that fails.</summary>
        /// <param name="slots">The number of replicas of the group and the slot of this node, the leader.</param>
        /// <param name="launch">The pipeline, mutation factory, leader tail read, led term, and eligibility of the start.</param>
        /// <param name="log">The owned group log.</param>
        /// <returns>The coordinator.</returns>
        /// <exception cref="InvalidDataException">A tail record is undecodable or inconsistent, so the committer must not start.</exception>
        /// <remarks>The pipeline is not yet published, so a failure leaves the committer as it was before the attempt, with nothing left open.</remarks>
        internal async Task<ReplicaCommitCoordinator> CreateCoordinatorAsync(
            (int ReplicaCount, int LeaderReplicaIndex) slots,
            (ReplicaGroupCommitPipeline Pipeline, ReplicaMutationFactory Factory, FollowerLogTail Read, ulong Term, ReplicaEligibility Eligibility) launch,
            IFollowerLog log)
        {
            try
            {
                var status = launch.Read.Status;
                var tail = committer.Applier.RecoverTail(ReplicaLeaderTail.From(launch.Read), launch.Term, launch.Factory);
                return committer.CreateCoordinator(slots, launch.Pipeline, log, in status, launch.Eligibility, tail);
            }
            catch
            {
                await committer.CloseSendersAsync(launch.Pipeline).ConfigureAwait(false);
                throw;
            }
        }

        /// <summary>Brings memory up to the commit index, appends the leader-term entry of an elected leader, probes the followers, and creates the pipeline.</summary>
        /// <param name="log">The owned group log.</param>
        /// <param name="tenure">The leadership by election, or <see langword="null" /> for a static leader.</param>
        /// <param name="replacing">Whether the start replaces the coordinator of a previous start.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>
        /// The new pipeline, the mutation factory of the led term, the leader tail read last, the led term, the eligibility of the group, and
        /// the probe results to admit once the coordinator exists.
        /// </returns>
        /// <exception cref="StaticLeaderTermExceededException">A static leader whose log moved past term one; nothing was written.</exception>
        /// <exception cref="ReplicaUnseededException">The copy of the group is not complete; nothing was written.</exception>
        /// <remarks>
        /// Runs under the commit gate, after the coordinator of the previous start is retired. Every start while an elected leadership
        /// is not yet authorized, a retry after a fault included, stops probing once enough followers answered from their logs to form a
        /// majority with the slots that count, so a silent follower does not hold the gate for its whole probe timeout; the readiness loop
        /// admits the answering followers. Every other start awaits every probe.
        /// </remarks>
        internal async Task<(ReplicaGroupCommitPipeline Pipeline, ReplicaMutationFactory Factory, FollowerLogTail Read, ulong Term, ReplicaEligibility Eligibility, ReplicaProbeResult[] Results)>
            LaunchAsync(IFollowerLog log, ReplicaLeaderTenure? tenure, bool replacing, CancellationToken cancellationToken)
        {
            await AdvanceAnnouncedCommitAsync(log, cancellationToken).ConfigureAwait(false);

            // One read pairs the status with its tail: a commit left running by the disposed coordinator may still advance the log.
            var read = await log.GetLeaderTailAsync(cancellationToken).ConfigureAwait(false);
            var status = read.Status;

            // A copy that is not complete holds no data of the group, so nothing is led over it, and nothing was written.
            if (status.MemberState != GroupMemberState.Complete)
                throw new ReplicaUnseededException($"Replica group '{committer.GroupId}' copy on this node is not complete, so it cannot be led.");

            // Memory must hold every committed entry before anything new is prepared.
            await committer.Applier.CatchUpAsync(log, status.LastAppliedIndex, status.CommitIndex, cancellationToken).ConfigureAwait(false);

            // A static leader leads term one only: a log past it was raised by an election, so it refuses before anything is appended. An
            // elected one leads its won term and appends its leader-term entry before any follower is probed, so verification can admit the
            // followers that hold it; the entry commits like a recovered tail.
            var term = tenure?.TermFor(in status, committer.GroupId)
                       ?? (StaticLeaderTerm.TryResolve(in status, out var staticTerm) ? staticTerm : throw StaticTermExceeded(committer.GroupId, status.CurrentTerm));
            var factory = new ReplicaMutationFactory(committer.Local, committer.GroupId, term, committer.Clock, committer.Log);
            if (tenure != null)
            {
                read = await tenure.AppendNoopAsync(log, read, factory, committer.Probe.SelfId, cancellationToken).ConfigureAwait(false);
                status = read.Status;
            }

            var (members, header) = committer.Probe.BuildMembership(term);
            var leaderIndex = committer.Probe.LeaderReplicaIndex;

            // A restart with durable progress leaves every slot recovering. Verify the leader's own log and every follower against its
            // last entry before the first commit, so the quorum is built from verified slots only. A follower still ready under a replaced
            // coordinator is verified again: it may hold less than the commit index the new coordinator starts it at. An uncommitted tail is
            // recovered by the coordinator and commits once verified slots hold it; followers lacking it are caught up through their senders,
            // outside this gate.
            var eligibility = committer.Registry.EligibilityFor(committer.GroupId);
            if (replacing || tenure != null)
                ReplicaReadinessProbe.UnverifyFollowers(eligibility, leaderIndex);

            ReplicaReadinessProbe.MarkLeaderReady(eligibility, leaderIndex, in status, committer.Topology.Fingerprint, committer.Topology.Generation);
            var answersNeeded = tenure is { Authorized: false } ? eligibility.AnswersForMajority(leaderIndex) : int.MaxValue;
            var results = eligibility.CanCountInWriteQuorum(leaderIndex)
                ? await ReplicaReadinessProbe.ProbeAllAsync(committer.Gateway, ReplicaReadinessProbe.NonReadyFollowers(eligibility, leaderIndex), members, header, status, new ReplicaProbeBudget(ReplicaVerificationProbe.ProbeTimeout, answersNeeded), cancellationToken).ConfigureAwait(false)
                : [];
            ReplicaReadinessProbe.RecordContacts(committer.Election, results, term);

            // The coordinator pins the tail in the log's idempotency state, which durable truncation releases pins from.
            var lagging = new ReplicaLaggingFollowers(committer.GroupId, eligibility, committer.Probe.Repairs, committer.Log);

            // An elected leader under quorum reads confirms the read index of a leader read with the replies its senders get in the led term;
            // otherwise the senders use the follower transport as it is.
            var rounds = committer is { Election: not null, Registry.QuorumReads: true } ? new ReplicaReadIndexRound(term, members.Length, leaderIndex) : null;
            var senders = committer.CreateSenders(members, leaderIndex, in status, in header, rounds);
            var pipeline = new ReplicaGroupCommitPipeline(committer.Applier, log, senders, (header.LeaderNodeId, leaderIndex), lagging, in status, term)
            {
                ReadIndex = rounds,
                LocalAppends = committer.LocalAppends,
            };
            return (pipeline, factory, read, term, eligibility, results);
        }

        /// <summary>Rebuilds the outcomes of the committed log entries once, after the coordinator of the first start pinned the recovered tail.</summary>
        /// <param name="log">The owned group log.</param>
        /// <param name="pipeline">The pipeline of the new coordinator, closed together with it on failure.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>An asynchronous operation.</returns>
        /// <remarks>
        /// Runs under the commit gate. A failure drops the new coordinator and closes its pipeline before it serves anything: it would keep the recovered
        /// tail with no follower progress recorded, refusing the next writes. Its tail is durable, so the next start pins it again and retries the rebuild.
        /// </remarks>
        internal async Task RestoreOutcomesAsync(IFollowerLog log, ReplicaGroupCommitPipeline pipeline, CancellationToken cancellationToken)
        {
            try
            {
                var restored = await ReplicaOutcomeRecovery.RestoreAsync(log, committer.Clock, cancellationToken).ConfigureAwait(false);
                ServerLog.ReplicaOutcomesRestored(committer.Log, committer.GroupId, restored);
            }
            catch
            {
                var coordinator = committer.Coordinator;
                committer.ClearRun();
                await committer.CloseSendersAsync(pipeline).ConfigureAwait(false);
                if (coordinator != null)
                    await coordinator.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }

        /// <summary>Makes the commit index this node learned as a follower durable, so the tail of a start is only what no majority is known to hold.</summary>
        /// <param name="log">The owned group log.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>An asynchronous operation.</returns>
        /// <remarks>An entry the previous leader committed is then not recounted, and its outcome is not left unknown.</remarks>
        private static async Task AdvanceAnnouncedCommitAsync(IFollowerLog log, CancellationToken cancellationToken)
        {
            var announced = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
            if (announced.CommitHint > announced.CommitIndex)
                _ = await log.AdvanceCommitAsync(announced.CommitHint, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Creates the sender of every follower slot, seeded with the last entry of the leader log.</summary>
        /// <param name="members">Group members in slot order.</param>
        /// <param name="leaderIndex">The slot of this node, which gets no sender.</param>
        /// <param name="status">Durable log status of the leader.</param>
        /// <param name="header">Replication envelope identity for follower calls.</param>
        /// <param name="rounds">The read-index rounds that observe every follower reply; none for a static leader.</param>
        /// <returns>The senders of the follower slots, in slot order.</returns>
        /// <remarks>
        /// The commit budget bounds one request, and the shutdown budget bounds waiting for one that ignores its cancellation on dispose. An
        /// elected leader posts every reply to the election state, and queues a follower out of the write quorum for repair once it answers.
        /// Every leader, static or elected, records the memory pressure flag of every reply.
        /// </remarks>
        private ReplicaFollowerSender[] CreateSenders(string[] members, int leaderIndex, in FollowerLogStatus status, in ReplicaRpcHeader header, ReplicaReadIndexRound? rounds)
        {
            var pressure = committer.MemberPressure;
            Action<int, FollowerLogAppendResult> replies = (slot, reply) => pressure.Observe(slot, reply.MemoryPressure);
            if (committer.Election is { } election)
            {
                var answering = new ReplicaAnsweringFollowers(committer.Registry.EligibilityFor(committer.GroupId), committer.Probe.Repairs, election.Clock, election.Options.ElectionTimeout);
                replies = (slot, reply) =>
                {
                    pressure.Observe(slot, reply.MemoryPressure);
                    election.RecordFollowerReply(slot, in reply);
                    answering.Observe(slot, in reply);
                };
            }

            var log = committer.Log;
            return ReplicaFollowerSenders.Create(
                rounds?.Observing(committer.Gateway, members) ?? committer.Gateway,
                members,
                leaderIndex,
                in status,
                in header,
                new ReplicaFollowerSenders.SenderTiming(committer.CommitBudget, committer.ShutdownBudget, committer.BudgetTimeProvider) { Replies = replies },
                budget => ServerLog.ReplicaFollowerSenderLeakedOnShutdown(log, budget));
        }
    }

    private static StaticLeaderTermExceededException StaticTermExceeded(string groupId, ulong term) =>
        new($"Replica group '{groupId}' log is at term {term}, which only an election sets; this node leads it statically in term one only.");
}
