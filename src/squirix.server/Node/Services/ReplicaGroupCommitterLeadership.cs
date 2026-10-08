using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Errors;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>The promotion of <see cref="ReplicaGroupCommitter" /> into a term an election won.</summary>
internal static class ReplicaGroupCommitterLeadership
{
    extension(ReplicaGroupCommitter committer)
    {
        /// <summary>Leads the group in a won term and reports whether its leader-term entry is committed.</summary>
        /// <param name="term">The won term.</param>
        /// <param name="cancellationToken">Cancellation token; it ends the wait for the applier lease, local recovery, or the commit gate.</param>
        /// <returns>
        /// <see langword="true" /> once the leader-term entry this promotion appended is committed by a coordinator of this promotion and the
        /// log still holds it in <paramref name="term" />; <see langword="false" /> while it is not, or when the start failed and is retried.
        /// </returns>
        /// <exception cref="InvalidOperationException">The committer leads statically, or still leads another term.</exception>
        /// <remarks>
        /// The first call takes the lease of the applier, waiting for a running apply pass, and keeps it until
        /// <see cref="ReplicaGroupCommitter.RetireAsync" />, so the apply loop of the group never runs meanwhile. The entry is appended once per
        /// promotion, at a new index with an identity of its own, so no entry of an earlier leadership, committed or not, stands in for it. It
        /// is committed without the write majority check of client writes: every follower starts unverified, and verification needs an entry
        /// of the term in the tail.
        /// </remarks>
        internal async Task<bool> PromoteAsync(ulong term, CancellationToken cancellationToken)
        {
            ArgumentOutOfRangeException.ThrowIfZero(term);
            committer.ThrowIfDisposed();
            if (committer.Election == null)
                throw new InvalidOperationException($"Replica group '{committer.GroupId}' is led statically and is never promoted.");

            await committer.WaitForLocalRecoveryAsync(cancellationToken).ConfigureAwait(false);
            var tenure = await committer.TakeTenureAsync(term, cancellationToken).ConfigureAwait(false);
            using var guard = await committer.Gate.LockAsync(cancellationToken).ConfigureAwait(false);
            committer.ThrowIfDisposed();
            try
            {
                if (!committer.IsStarted)
                    await committer.StartAsync(cancellationToken).ConfigureAwait(false);

                // A start that succeeded ends the fault: the same fault coming back later is logged again.
                _ = tenure.ReportFault(null);
                _ = await committer.TryApplyPendingAsync().ConfigureAwait(false);
                return committer.Registry.TryGetLog(committer.GroupId, out var log) &&
                       await tenure.IsAuthorizedAsync(committer.Coordinator, log, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is IOException or InvalidDataException or InvalidOperationException or SquirixException)
            {
                // The start is retried on the next call: a storage fault, an inconsistent record, a log that moved past the term, or committed
                // entries of a replaced coordinator still to apply. The same fault repeats every tick, so it is logged when it changes.
                if (tenure.ReportFault(exception.GetType()))
                {
                    if (exception is IOException or InvalidDataException)
                        ServerLog.ReplicaPromotionStorageRetry(committer.Log, committer.GroupId, term, exception);
                    else
                        ServerLog.ReplicaPromotionRetry(committer.Log, committer.GroupId, term, exception);
                }

                return false;
            }
        }
    }
}
