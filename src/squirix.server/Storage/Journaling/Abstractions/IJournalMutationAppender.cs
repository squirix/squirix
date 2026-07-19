using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Append-only journal mutation surface for durable cache operations.</summary>
internal interface IJournalMutationAppender
{
    ValueTask AppendPutAndAwaitDurabilityAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken);

    ValueTask AppendPutAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken);

    ValueTask AppendRemoveAsync(CacheKey key, CancellationToken cancellationToken);

    ValueTask AppendRemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken);

    ValueTask AppendTouchExpirationAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken);

    ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, CancellationToken cancellationToken);
}
