using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>The read fence of <see cref="ReplicaGroupCommitter" />: a leader read under quorum reads observes every write committed before it.</summary>
internal static class ReplicaGroupCommitterReads
{
    /// <summary>The delay between two observations of the applied index while a read waits for its read index to be applied.</summary>
    private static readonly TimeSpan AppliedPollInterval = TimeSpan.FromMilliseconds(1);

    extension(ReplicaGroupCommitter committer)
    {
        /// <summary>Fences a leader read: confirms a read index with a majority in the led term and waits until memory applied it.</summary>
        /// <param name="cancellationToken">Cancellation token of the read.</param>
        /// <returns>A task that completes once the local read may be served.</returns>
        /// <exception cref="Grpc.Core.RpcException">
        /// The read may not be served now, and nothing was read: this node has no leader authority in the group in the led term (the leader
        /// refusal of its election state: stale-term, stale-owner naming a known leader, or Unavailable), or a follower answered in a higher
        /// term (stale-term); or, within one election timeout, no majority confirmed the read index (Unavailable, read
        /// quorum unconfirmed) or memory did not apply it (Unavailable, read index unapplied).
        /// </exception>
        /// <exception cref="InvalidOperationException">The committer leads statically; only an elected leader fences reads.</exception>
        /// <remarks>
        /// Runs outside the commit gate. The authority is checked before the read index is taken: authority in the term of the pipeline means
        /// its leader-term entry is committed, so the commit index covers every entry any earlier leader committed. The wait is bounded by
        /// the election timeout, which a leader whose majority stays silent that long does not outlive anyway.
        /// </remarks>
        internal async Task ConfirmReadAsync(CancellationToken cancellationToken)
        {
            var election = ThrowHelper.Required(committer.Election, "A replica group led statically has no read index; its reads are not fenced.");
            var pipeline = committer.RunningPipeline;
            var view = election.ReadRoute();
            var self = committer.Probe.SelfId;
            if (pipeline == null || !view.HasAuthority || view.Term != pipeline.Term)
                throw LeaderRefusal.Create(LeaderRefusal.Classify(in view, self), in view, self, true);

            using var bound = new CancellationTokenSource(election.Options.ElectionTimeout, election.Clock);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, bound.Token);
            var readIndex = 0UL;
            var confirmed = false;
            try
            {
                readIndex = await pipeline.ConfirmReadIndexAsync(linked.Token).ConfigureAwait(false);
                confirmed = true;
                if (committer.Applier.AppliedIndex < readIndex)
                    await LeaderReadBarrier.WaitUntilAppliedAsync(committer.Applier, static applier => applier.AppliedIndex, readIndex, (election.Clock, AppliedPollInterval), linked.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                throw confirmed ? ServerOpContract.ReadIndexUnapplied() : ServerOpContract.ReadQuorumUnconfirmed();
            }

            Verify((election, self), pipeline.Term, new LeaderReadState(true, committer.Applier.AppliedIndex, readIndex));
        }
    }

    /// <summary>Runs the quorum-read gate over the confirmed read: the leader still holds authority in the led term and applied the read index.</summary>
    /// <param name="leader">The election state of the group, and this node.</param>
    /// <param name="term">The led term the read index was confirmed in.</param>
    /// <param name="read">The confirmed read.</param>
    /// <exception cref="Grpc.Core.RpcException">
    /// The gate refuses the read, and nothing was read: the leader refusal of the election state, stale-term when the gate saw a higher
    /// term and the state names no other leader; otherwise Unavailable.
    /// </exception>
    private static void Verify((ReplicaGroupState Election, string Self) leader, ulong term, in LeaderReadState read)
    {
        var (election, self) = leader;
        var view = election.ReadRoute();
        var decision = FailoverActivationGate.CheckQuorumRead(
            true,
            election.ReplicaCount,
            true,
            view.HasAuthority && view.Term == term,
            view.Term,
            view.HighestObservedTerm,
            in read);
        if (decision.Allowed)
            return;

        if (decision.Denial is not (LeaderAuthorityDenial.StaleTerm or LeaderAuthorityDenial.NotLeader))
            throw ServerOpContract.ReadQuorumUnconfirmed();

        var kind = LeaderRefusal.Classify(in view, self);
        if (decision.Denial == LeaderAuthorityDenial.StaleTerm && kind is LeaderRefusalKind.None or LeaderRefusalKind.NoLeader)
            kind = LeaderRefusalKind.StaleTerm;

        throw LeaderRefusal.Create(kind, in view, self, true);
    }
}
