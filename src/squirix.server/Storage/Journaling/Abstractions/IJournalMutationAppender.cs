using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Threading;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Append-only journal mutation surface for durable cache operations.</summary>
/// <remarks>
/// Every cache mutation append takes the <see cref="AsyncLockOwnership" /> of the snapshot barrier its caller is running
/// under and is refused with <see cref="InvalidOperationException" /> unless that barrier still holds the mutation gate.
/// </remarks>
internal interface IJournalMutationAppender
{
    /// <summary>Appends the idempotency outcome frame of an operation under the mutation gate.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="fingerprint">The mutation fingerprint.</param>
    /// <param name="responseBytes">The serialized response.</param>
    /// <param name="appended">
    /// Runs under the mutation gate once the frame is accepted (enqueued, and written when group commit is on), so a snapshot cut
    /// sees the frame and what this does together or neither; not run when the append throws. <see langword="null" /> runs nothing.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the frame is enqueued; durability is awaited separately.</returns>
    ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, Action? appended, CancellationToken cancellationToken);

    ValueTask AppendPutAsync(AsyncLockOwnership ownership, CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken);

    ValueTask AppendRemoveAsync(AsyncLockOwnership ownership, CacheKey key, CancellationToken cancellationToken);
}
