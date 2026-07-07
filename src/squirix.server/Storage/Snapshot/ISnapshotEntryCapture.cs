using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.Storage.Snapshot;

/// <summary>Captures live cache entries for snapshot serialization without coupling storage to owner-cache types.</summary>
internal interface ISnapshotEntryCapture
{
    /// <summary>Appends non-expired live cache entries to <paramref name="target" />.</summary>
    /// <param name="target">The list to populate with snapshot-ready entries.</param>
    /// <param name="utcNow">The UTC instant used to filter expired entries.</param>
    /// <param name="cancellationToken">A token to observe while enumerating.</param>
    /// <returns>A task that completes when capture finishes.</returns>
    ValueTask CaptureEntriesAsync(List<(CacheKey Key, NodeCacheEntry<object?> Entry)> target, DateTime utcNow, CancellationToken cancellationToken);
}
