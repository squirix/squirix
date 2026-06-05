using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;

namespace Squirix.Server.Storage.Snapshot;

/// <summary>
/// Persists snapshot frames and idempotency records to durable storage.
/// </summary>
internal interface ISnapshotWriter
{
    /// <summary>
    /// Writes a snapshot file for the given index and returns the final path.
    /// </summary>
    /// <param name="index">Monotonic snapshot index.</param>
    /// <param name="items">Live cache entries to persist.</param>
    /// <param name="idempotencyRecords">Retained idempotency records.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Path to the completed snapshot file.</returns>
    Task<string> WriteAsync(
        int index,
        IEnumerable<(CacheKey Key, object Entry)> items,
        IEnumerable<PersistedIdempotencyRecord> idempotencyRecords,
        CancellationToken cancellationToken);
}
