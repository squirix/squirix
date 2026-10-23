using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.Storage.Snapshot;

/// <summary>Persists snapshot frames and idempotency records to durable storage.</summary>
internal interface ISnapshotWriter
{
    /// <summary>Returns the index for the next snapshot: above the published index and above every snapshot file on disk.</summary>
    /// <param name="publishedIndex">Index of the snapshot the manifest currently names, or 0 when there is none.</param>
    /// <returns>An index that no snapshot file on disk uses.</returns>
    int AllocateIndex(int publishedIndex);

    /// <summary>Writes a snapshot file for the given index and returns the final path.</summary>
    /// <param name="index">Monotonic snapshot index.</param>
    /// <param name="items">Live cache entries to persist.</param>
    /// <param name="idempotencyRecords">Retained idempotency records.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Path to the completed snapshot file.</returns>
    ValueTask<string> WriteAsync(
        int index,
        IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> items,
        IReadOnlyList<PersistedIdempotencyRecord> idempotencyRecords,
        CancellationToken cancellationToken);
}
