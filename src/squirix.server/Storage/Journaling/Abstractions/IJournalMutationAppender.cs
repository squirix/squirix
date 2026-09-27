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
    ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, CancellationToken cancellationToken);

    ValueTask AppendPutAndAwaitDurabilityAsync(AsyncLockOwnership ownership, CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken);

    ValueTask AppendPutAsync(AsyncLockOwnership ownership, CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken);

    ValueTask AppendRemoveAsync(AsyncLockOwnership ownership, CacheKey key, CancellationToken cancellationToken);

    ValueTask AppendRemoveExpirationAsync(AsyncLockOwnership ownership, CacheKey key, CancellationToken cancellationToken);

    ValueTask AppendTouchExpirationAsync(AsyncLockOwnership ownership, CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken);
}
