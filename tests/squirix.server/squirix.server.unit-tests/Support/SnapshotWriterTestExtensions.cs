using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Test helpers for writing single-entry snapshots without one-element collection allocations.</summary>
internal static class SnapshotWriterTestExtensions
{
    private static readonly IReadOnlyList<PersistedIdempotencyRecord> NoIdempotencyRecords = [];

    internal static Task<string> WriteSingleAsync(this ISnapshotWriter writer, int index, CacheKey key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
        writer.WriteAsync(index, new SingleItemReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)>((key, entry)), NoIdempotencyRecords, cancellationToken);
}
