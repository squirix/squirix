using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>The follower verification of <see cref="ReplicaGroupCommitter" />, which regains the write quorum of a restarted group.</summary>
internal static class ReplicaGroupCommitterVerification
{
    extension(ReplicaGroupCommitter committer)
    {
        /// <summary>Verifies non-ready replica slots against the leader log so a restarted group regains its write quorum.</summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>
        /// The verification state: <see cref="ReplicaVerification.Pending" /> while some follower is not yet verified or an uncommitted
        /// tail is not yet committed; <see cref="ReplicaVerification.Blocked" /> while the log is not ready or its uncommitted tail holds
        /// no entry of the current term.
        /// </returns>
        /// <remarks>
        /// Followers are probed without holding the commit gate, so a dead or slow peer never delays writes; a follower that lacks entries,
        /// the uncommitted tail included, is caught up afterwards by the readiness service through its sender. Only when some follower answered does the gate get taken to start the coordinator (which
        /// recovers the tail), re-check that the leader tail did not move, admit the verified slots, and commit what they now cover.
        /// </remarks>
        internal async Task<ReplicaVerification> VerifyReplicasAsync(CancellationToken cancellationToken)
        {
            committer.ThrowIfDisposed();
            await committer.WaitForLocalRecoveryAsync(cancellationToken).ConfigureAwait(false);
            committer.ThrowIfDisposed();
            if (!committer.Registry.TryGetLog(committer.GroupId, out var log))
                return ReplicaVerification.Blocked;

            // A committer that leads by election verifies nothing once it no longer leads, and verifies in its led term while it does.
            var tenure = committer.Tenure;
            if (committer.Election != null && tenure == null)
                return ReplicaVerification.Blocked;

            var snapshot = await committer.Probe.ProbeAsync(log, tenure?.Term ?? 0UL, cancellationToken).ConfigureAwait(false);
            if (snapshot.Verdict is { } verdict)
                return verdict;

            using var guard = await committer.Gate.LockAsync(cancellationToken).ConfigureAwait(false);
            committer.ThrowIfDisposed();

            // A retirement may have run while the probe was out: the snapshot then belongs to a leadership that is over.
            return committer.Election != null && !ReferenceEquals(committer.Tenure, tenure) ? ReplicaVerification.Blocked
                : await committer.AdmitVerifiedAsync(log, snapshot, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Admits the followers verified outside the gate and commits what the verified slots now cover.</summary>
        /// <param name="log">The owned group log.</param>
        /// <param name="snapshot">The follower probing taken outside the gate.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The verification state.</returns>
        /// <remarks>Runs under the commit gate.</remarks>
        private async Task<ReplicaVerification> AdmitVerifiedAsync(
            IFollowerLog log,
            ReplicaVerificationSnapshot snapshot,
            CancellationToken cancellationToken)
        {
            // A coordinator that still retains entries is never replaced (its restart refuses): the verified slots are admitted into it,
            // and its resolver commits and applies what they now cover. Otherwise the coordinator starts here, recovering the log tail.
            var coordinator = !await committer.TryApplyPendingAsync().ConfigureAwait(false) && committer.Coordinator is { } retained ? retained
                : (await committer.EnsureStartedAsync(false, cancellationToken).ConfigureAwait(false)).Coordinator;
            var eligibility = await committer.Probe.AdmitVerifiedSlotsAsync(log, snapshot, coordinator, cancellationToken).ConfigureAwait(false);
            if (committer.RunningPipeline is { } pipeline)
                committer.Probe.OfferCatchUp(snapshot.Answered, pipeline.CatchUpTargetFor);

            var applied = await committer.TryApplyPendingAsync().ConfigureAwait(false);
            return applied && eligibility.AllCanCountInWriteQuorum() ? ReplicaVerification.AllReady : ReplicaVerification.Pending;
        }
    }
}
