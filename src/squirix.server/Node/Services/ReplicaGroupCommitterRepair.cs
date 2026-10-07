using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>The entry catch-up of lagging followers driven by <see cref="ReplicaGroupCommitter" />.</summary>
internal static class ReplicaGroupCommitterRepair
{
    extension(ReplicaGroupCommitter committer)
    {
        /// <summary>Runs one entry catch-up session, one after another, for each follower the last verification found answering but not verified.</summary>
        /// <param name="reporter">Logs and counts the sessions.</param>
        /// <param name="cancellationToken">Cancellation token; its cancellation ends the running session by throwing.</param>
        /// <returns><see langword="true" /> when a follower was admitted to the write quorum, so the caller verifies again at once.</returns>
        /// <remarks>
        /// Each session leases the follower's sender, so the follower never sees live appends interleaved with the catch-up. A session
        /// that caught the follower up admits it under the commit gate before the lease ends; the live entries that waited meanwhile are
        /// then acknowledged or sent in order by the resumed sender.
        /// </remarks>
        internal async Task<bool> CatchUpFollowersAsync(ReplicaCatchUpReporter reporter, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(reporter);
            var admitted = false;
            foreach (var target in committer.Probe.TakeCatchUpTargets())
                admitted |= await committer.CatchUpFollowerAsync(target, reporter, cancellationToken).ConfigureAwait(false);

            return admitted;
        }

        private async Task<bool> CatchUpFollowerAsync(ReplicaCatchUpTarget target, ReplicaCatchUpReporter reporter, CancellationToken cancellationToken)
        {
            ReplicaFollowerCatchUp lease;
            try
            {
                lease = await target.Sender.BeginCatchUpAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                // The pipeline is being replaced: the next verification runs against the new one.
                return false;
            }

            using (lease)
            {
                var session = new ReplicaEntryCatchUpSession(target.Log, committer.GroupId, target.Term);
                var result = await session.RunAsync(lease, cancellationToken).ConfigureAwait(false);
                var admitted = result.Outcome == ReplicaCatchUpOutcome.CaughtUp &&
                               await committer.TryAdmitAsync(target, result, cancellationToken).ConfigureAwait(false);
                reporter.Report(target.ReplicaIndex, lease.NodeId, in result, admitted);
                return admitted;
            }
        }

        /// <summary>Admits a caught-up follower, giving up when its sender starts draining meanwhile.</summary>
        /// <param name="target">The catch-up target.</param>
        /// <param name="result">The session result.</param>
        /// <param name="cancellationToken">Cancellation token of the pass.</param>
        /// <returns><see langword="true" /> when the follower was admitted.</returns>
        /// <remarks>
        /// A resync drains the sender under the commit gate and waits for the lease this pass still holds: waiting for the gate would hold
        /// that drain, and the write behind it, for the whole drain budget. The new pipeline would refuse the admission anyway.
        /// </remarks>
        private async Task<bool> TryAdmitAsync(ReplicaCatchUpTarget target, ReplicaCatchUpResult result, CancellationToken cancellationToken)
        {
            var drainStarted = target.Sender.DrainStarted;
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, drainStarted);
            try
            {
                return await committer.AdmitCaughtUpFollowerAsync(target.ReplicaIndex, result, target.Pipeline, linked.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (drainStarted.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                return false;
            }
        }
    }
}
