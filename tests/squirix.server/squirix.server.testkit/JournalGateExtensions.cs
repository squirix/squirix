using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.TestKit;

/// <summary>
/// Journal appends for tests that drive the journal directly: each call takes the mutation gate the way the durable mutation executor
/// does, because the journal refuses a sequence allocation from a caller that does not hold it. Never call these from a flow that already
/// holds the gate: it is not reentrant.
/// </summary>
internal static class JournalGateExtensions
{
    /// <param name="journal">Journal to append to.</param>
    extension(IJournalCoordinator journal)
    {
        /// <summary>Appends a put frame under the mutation gate.</summary>
        /// <param name="key">Cache key.</param>
        /// <param name="entryBytes">Encoded cache entry.</param>
        /// <param name="cancellationToken">Cancels the gate wait and the append admission.</param>
        /// <returns>The append.</returns>
        internal ValueTask AppendPutUnderGateAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) =>
            journal.ExecuteUnderSnapshotBarrierAsync(
                (Journal: journal, Key: key, EntryBytes: entryBytes),
                static (s, ct) => s.Journal.AppendPutAsync(s.Key, s.EntryBytes, ct),
                cancellationToken);

        /// <summary>Appends a put frame under the mutation gate and waits for its durability before releasing the gate.</summary>
        /// <param name="key">Cache key.</param>
        /// <param name="entryBytes">Encoded cache entry.</param>
        /// <param name="cancellationToken">Cancels the gate wait and the append admission.</param>
        /// <returns>The durable append.</returns>
        internal ValueTask AppendPutDurablyUnderGateAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) =>
            journal.ExecuteUnderSnapshotBarrierAsync(
                (Journal: journal, Key: key, EntryBytes: entryBytes),
                static (s, ct) => s.Journal.AppendPutAndAwaitDurabilityAsync(s.Key, s.EntryBytes, ct),
                cancellationToken);

        /// <summary>Appends a remove frame under the mutation gate.</summary>
        /// <param name="key">Cache key.</param>
        /// <param name="cancellationToken">Cancels the gate wait and the append admission.</param>
        /// <returns>The append.</returns>
        internal ValueTask AppendRemoveUnderGateAsync(CacheKey key, CancellationToken cancellationToken) =>
            journal.ExecuteUnderSnapshotBarrierAsync((Journal: journal, Key: key), static (s, ct) => s.Journal.AppendRemoveAsync(s.Key, ct), cancellationToken);

        /// <summary>Appends a remove-expiration frame under the mutation gate.</summary>
        /// <param name="key">Cache key.</param>
        /// <param name="cancellationToken">Cancels the gate wait and the append admission.</param>
        /// <returns>The append.</returns>
        internal ValueTask AppendRemoveExpirationUnderGateAsync(CacheKey key, CancellationToken cancellationToken) =>
            journal.ExecuteUnderSnapshotBarrierAsync((Journal: journal, Key: key), static (s, ct) => s.Journal.AppendRemoveExpirationAsync(s.Key, ct), cancellationToken);

        /// <summary>
        /// Runs <paramref name="append" /> with only its admission under the mutation gate: the gate is released as soon as the call returns,
        /// so its sequence is allocated and its frame enqueued under the gate while the rest (write ack, durability wait) runs outside it, as
        /// for a caller whose durability wait is not gated. With a free gate and free ring space the admission completes before this method
        /// returns, so appends started one after another enter the ring in call order; a full ring would enqueue after the gate is released.
        /// </summary>
        /// <typeparam name="TState">Type of the append arguments.</typeparam>
        /// <param name="state">Arguments passed to <paramref name="append" />.</param>
        /// <param name="append">Append to run; it receives the journal, <paramref name="state" /> and the cancellation token.</param>
        /// <param name="cancellationToken">Cancels the gate wait and is passed to <paramref name="append" />.</param>
        /// <returns>The whole append, including the part after the gate was released.</returns>
        internal async Task AppendAdmittedUnderGateAsync<TState>(TState state, Func<IJournalCoordinator, TState, CancellationToken, ValueTask> append, CancellationToken cancellationToken)
        {
            var admitted = await journal.ExecuteUnderSnapshotBarrierAsync(
                                            (Journal: journal, State: state, Append: append),
                                            static (s, ct) => ValueTask.FromResult(s.Append(s.Journal, s.State, ct).AsTask()),
                                            cancellationToken)
                                        .ConfigureAwait(false);
            await admitted.ConfigureAwait(false);
        }

        /// <summary>Appends a touch-expiration frame under the mutation gate.</summary>
        /// <param name="key">Cache key.</param>
        /// <param name="expiresUtc">New absolute expiration.</param>
        /// <param name="cancellationToken">Cancels the gate wait and the append admission.</param>
        /// <returns>The append.</returns>
        internal ValueTask AppendTouchExpirationUnderGateAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken) =>
            journal.ExecuteUnderSnapshotBarrierAsync(
                (Journal: journal, Key: key, ExpiresUtc: expiresUtc),
                static (s, ct) => s.Journal.AppendTouchExpirationAsync(s.Key, s.ExpiresUtc, ct),
                cancellationToken);
    }
}
