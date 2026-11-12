using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Threading;

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
        internal Task<bool> CatchUpFollowersAsync(ReplicaCatchUpReporter reporter, CancellationToken cancellationToken) => committer.CatchUpFollowersAsync(reporter, null, cancellationToken);

        /// <summary>Runs one catch-up session per follower the last verification found answering, repairing by state transfer a follower the log cannot catch up.</summary>
        /// <param name="reporter">Logs and counts the sessions.</param>
        /// <param name="transfers">Sends the state of the group to a follower the leader no longer retains the entries for, or whose earlier transfer was interrupted; none are sent when <see langword="null" />.</param>
        /// <param name="cancellationToken">Cancellation token; its cancellation ends the running session by throwing.</param>
        /// <returns><see langword="true" /> when a follower was admitted to the write quorum, so the caller verifies again at once.</returns>
        /// <remarks>
        /// A follower whose entry session ends compacted, or incomplete (its first answer routes it, before any entry is sent), gets a transfer
        /// and then a fresh entry session from the transferred index. Streaming releases the catch-up gate so writes can continue. The old
        /// sender lease is released before a fresh lease is acquired outside the gate. Waiting for a busy sender never excludes writes.
        /// The log pin stays held until the pass ends.
        /// </remarks>
        internal async Task<bool> CatchUpFollowersAsync(ReplicaCatchUpReporter reporter, ReplicaStateTransfers? transfers, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(reporter);
            var admitted = false;
            var targets = committer.Probe.TakeCatchUpTargets();
            for (var i = 0; i < targets.Count; i++)
                admitted |= await committer.CatchUpFollowerAsync(targets[i], (reporter, transfers, i < targets.Count - 1), cancellationToken).ConfigureAwait(false);

            return admitted;
        }

        /// <summary>Admits a follower slot a catch-up session verified, under the commit gate, and commits what the slot now covers.</summary>
        /// <param name="target">The catch-up target: the follower slot, the pipeline whose sender the session ran on, and the led term.</param>
        /// <param name="result">The session result.</param>
        /// <param name="cancellationToken">Cancellation token for queueing on the gate.</param>
        /// <returns><see langword="true" /> when the slot became ready.</returns>
        /// <remarks>
        /// Called while the session's lease still holds the sender, so no live entry past the held index reaches the follower, and no
        /// acknowledgement of one is lost to a slot that does not count yet, before the slot's match index is raised. A slot is never
        /// admitted into a newer pipeline, nor once the leader log left the led term: a newer leader may have rewritten the entries the
        /// session read, so what the follower holds proves nothing for this term. A held entry of a term above the led one is refused
        /// for the same reason.
        /// </remarks>
        internal async Task<bool> AdmitCaughtUpFollowerAsync(ReplicaCatchUpTarget target, ReplicaCatchUpResult result, CancellationToken cancellationToken)
        {
            committer.ThrowIfDisposed();
            using var guard = await committer.Gate.LockAsync(cancellationToken).ConfigureAwait(false);
            committer.ThrowIfDisposed();
            if (!ReferenceEquals(committer.RunningPipeline, target.Pipeline) || committer.Coordinator is not { } coordinator || !committer.Registry.TryGetLog(committer.GroupId, out var log))
                return false;

            var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
            if (status.CurrentTerm != target.Term || result.HeldTerm > target.Term)
                return false;

            var replicaIndex = target.ReplicaIndex;
            var eligibility = committer.Registry.EligibilityFor(committer.GroupId);
            var wasReady = eligibility.CanCountInWriteQuorum(replicaIndex);
            ReplicaReadinessProbe.AdmitCaughtUp(eligibility, replicaIndex, in result, status.CommitIndex, committer.Topology.Fingerprint, committer.Topology.Generation, coordinator);
            if (wasReady || !eligibility.CanCountInWriteQuorum(replicaIndex))
                return false;

            _ = await committer.TryApplyPendingAsync().ConfigureAwait(false);
            return true;
        }

        private async Task<bool> CatchUpFollowerAsync(
            ReplicaCatchUpTarget target,
            (ReplicaCatchUpReporter Reporter, ReplicaStateTransfers? Transfers, bool OthersWait) pass,
            CancellationToken cancellationToken)
        {
            using var pin = committer.TransferPin.Begin();
            var entries = 0;
            var rounds = 0;
            var attempts = 0;
            while (true)
            {
                var (result, admitted, retry) = await committer.CatchUpPassAsync(target, (pass.Transfers, pass.OthersWait), pin, cancellationToken).ConfigureAwait(false);
                entries += result.EntriesSent;
                rounds += result.Rounds;
                if (retry && ++attempts < 2)
                    continue;

                result = result with { EntriesSent = entries, Rounds = rounds, Outcome = retry ? ReplicaCatchUpOutcome.Yielded : result.Outcome };
                pass.Reporter.Report(target.ReplicaIndex, target.Sender.NodeId, in result, admitted);
                return admitted;
            }
        }

        private async Task<(ReplicaCatchUpResult Result, bool Admitted, bool Retry)> CatchUpPassAsync(
            ReplicaCatchUpTarget target,
            (ReplicaStateTransfers? Transfers, bool OthersWait) pass,
            ReplicaTransferPin.Session pin,
            CancellationToken cancellationToken)
        {
            var drainStarted = target.Sender.DrainStarted;
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, drainStarted);
            var catchUpToken = linked.Token;
            AsyncLockHolder guard = default;
            ReplicaFollowerCatchUp? lease = null;
            try
            {
                lease = await committer.BeginCatchUpBoundedAsync(target.Sender, catchUpToken).ConfigureAwait(false);
                if (lease == null)
                    return (new ReplicaCatchUpResult(ReplicaCatchUpOutcome.Yielded, 0, 0, 0, 0, 0), false, false);
                var session = new ReplicaEntryCatchUpSession(target.Log, committer.Probe.SelfId, target.Term);

                // The readiness loop selects only non-counting targets and verifies sequentially. An existing write cannot need
                // this sender for its majority. Pipeline replacement drains it before re-verification and cancels this wait.
                guard = await committer.CatchUpGate.LockAsync(catchUpToken).ConfigureAwait(false);
                var result = await session.FinalizeAsync(lease, (ReplicaVerificationProbe.ProbeTimeout, committer.BudgetTimeProvider), catchUpToken).ConfigureAwait(false);
                if (result.Outcome == ReplicaCatchUpOutcome.CaughtUp)
                    return (result, await committer.TryAdmitAsync(target, result, cancellationToken).ConfigureAwait(false), false);
                if (result.Outcome == ReplicaCatchUpOutcome.Yielded)
                {
                    // Bulk replay never needs the committer gate. Keep the sender leased but let healthy-quorum writes proceed.
                    guard.Dispose();
                    result = await session.CopyPrefixAsync(lease, catchUpToken).ConfigureAwait(false);
                }

                var (transfers, othersWait) = pass;
                if (transfers != null && result.Outcome is ReplicaCatchUpOutcome.Unseeded or ReplicaCatchUpOutcome.Compacted or ReplicaCatchUpOutcome.Incomplete)
                {
                    lease.Dispose();
                    guard.Dispose();
                    lease = await committer.BeginCatchUpBoundedAsync(target.Sender, catchUpToken).ConfigureAwait(false);
                    if (lease == null)
                        return (new ReplicaCatchUpResult(ReplicaCatchUpOutcome.Yielded, 0, 0, 0, 0, 0), false, false);
                    guard = await committer.CatchUpGate.LockAsync(catchUpToken).ConfigureAwait(false);
                    var transferred = await transfers.RepairAsync(committer, target, (lease, guard), pin, result.Outcome == ReplicaCatchUpOutcome.Unseeded, othersWait, catchUpToken).ConfigureAwait(false);
                    return (result, false, transferred);
                }

                return (result, false, result.Outcome is ReplicaCatchUpOutcome.PrefixCopied or ReplicaCatchUpOutcome.CaughtUp);
            }
            catch (OperationCanceledException) when (drainStarted.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                return (new ReplicaCatchUpResult(ReplicaCatchUpOutcome.Aborted, 0, 0, 0, 0, 0), false, false);
            }
            catch (ObjectDisposedException)
            {
                return (new ReplicaCatchUpResult(ReplicaCatchUpOutcome.Aborted, 0, 0, 0, 0, 0), false, false);
            }
            finally
            {
                lease?.Dispose();
                guard.Dispose();
            }
        }

        private async ValueTask<ReplicaFollowerCatchUp?> BeginCatchUpBoundedAsync(ReplicaFollowerSender sender, CancellationToken cancellationToken)
        {
            using var deadline = new CancellationTokenSource(ReplicaVerificationProbe.ProbeTimeout, committer.BudgetTimeProvider);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);
            try
            {
                return await sender.BeginCatchUpAsync(linked.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (deadline.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                return null;
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
                return await committer.AdmitCaughtUpFollowerAsync(target, result, linked.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (drainStarted.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                return false;
            }
        }
    }
}
