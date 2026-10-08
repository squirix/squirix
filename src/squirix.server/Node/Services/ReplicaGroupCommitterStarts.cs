using System;
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
        /// <summary>Brings memory up to the commit index, appends the leader-term entry of an elected leader, probes the followers, and creates the pipeline.</summary>
        /// <param name="log">The owned group log.</param>
        /// <param name="tenure">The leadership by election, or <see langword="null" /> for a static leader.</param>
        /// <param name="replacing">Whether the start replaces the coordinator of a previous start.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>
        /// The new pipeline, the mutation factory of the led term, the leader tail read last, the led term, the eligibility of the group, and
        /// the probe results to admit once the coordinator exists.
        /// </returns>
        /// <remarks>Runs under the commit gate, after the coordinator of the previous start is retired.</remarks>
        internal async Task<(ReplicaGroupCommitPipeline Pipeline, ReplicaMutationFactory Factory, FollowerLogTail Read, ulong Term, ReplicaEligibility Eligibility, ReplicaProbeResult[] Results)>
            LaunchAsync(IFollowerLog log, ReplicaLeaderTenure? tenure, bool replacing, CancellationToken cancellationToken)
        {
            // One read pairs the status with its tail: a commit left running by the disposed coordinator may still advance the log.
            var read = await log.GetLeaderTailAsync(cancellationToken).ConfigureAwait(false);
            var status = read.Status;

            // Memory must hold every committed entry before anything new is prepared.
            await committer.Applier.CatchUpAsync(log, status.LastAppliedIndex, status.CommitIndex, cancellationToken).ConfigureAwait(false);

            // A static leader leads in the term of its log. An elected one leads its won term and appends its leader-term entry before any
            // follower is probed, so verification can admit the followers that hold it; the entry commits like a recovered tail.
            var term = tenure?.TermFor(in status, committer.GroupId) ?? Math.Max(1UL, status.CurrentTerm);
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
            var results = eligibility.CanCountInWriteQuorum(leaderIndex)
                ? await ReplicaReadinessProbe.ProbeAllAsync(committer.Gateway, ReplicaReadinessProbe.NonReadyFollowers(eligibility, leaderIndex), members, header, status, ReplicaVerificationProbe.ProbeTimeout, cancellationToken).ConfigureAwait(false)
                : [];
            ReplicaReadinessProbe.RecordContacts(committer.Election, results, term);

            // The coordinator pins the tail in the log's idempotency state, which durable truncation releases pins from.
            var lagging = new ReplicaLaggingFollowers(committer.GroupId, eligibility, committer.Probe.Repairs, committer.Log);

            // An elected leader under quorum reads confirms the read index of a leader read with the replies its senders get in the led term;
            // otherwise the senders use the follower transport as it is.
            var rounds = committer.Election != null && committer.Registry.QuorumReads ? new ReplicaReadIndexRound(term, members.Length, leaderIndex) : null;
            var senders = committer.CreateSenders(members, leaderIndex, in status, in header, rounds);
            var pipeline = new ReplicaGroupCommitPipeline(committer.Applier, log, senders, (header.LeaderNodeId, leaderIndex), lagging, in status, term) { ReadIndex = rounds };
            return (pipeline, factory, read, term, eligibility, results);
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
        /// </remarks>
        private ReplicaFollowerSender[] CreateSenders(string[] members, int leaderIndex, in FollowerLogStatus status, in ReplicaRpcHeader header, ReplicaReadIndexRound? rounds)
        {
            Action<int, FollowerLogAppendResult>? replies = null;
            if (committer.Election is { } election)
            {
                var answering = new ReplicaAnsweringFollowers(committer.Registry.EligibilityFor(committer.GroupId), committer.Probe.Repairs, election.Clock, election.Options.ElectionTimeout);
                replies = (slot, reply) =>
                {
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
}
