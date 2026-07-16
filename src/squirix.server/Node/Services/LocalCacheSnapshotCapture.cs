using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage.Entries.Binary;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Services;

/// <summary>Bridges owner-cache enumeration into snapshot-ready object entries.</summary>
/// <typeparam name="T">The stored cache value type.</typeparam>
internal sealed class LocalCacheSnapshotCapture<T> : ISnapshotEntryCapture
{
    private readonly ILocalCacheSnapshotReader<T> _reader;

    public LocalCacheSnapshotCapture(ILocalCacheSnapshotReader<T> reader)
    {
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
    }

    public async ValueTask CaptureEntriesAsync(List<(CacheKey Key, NodeCacheEntry<object?> Entry)> target, DateTime utcNow, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(target);

        var capacity = _reader is ILocalCacheStats stats ? stats.EntryCount : 0;
        if (target.Capacity < capacity)
            target.Capacity = capacity;

        await foreach (var (key, entry) in _reader.EnumerateLiveAsync(cancellationToken).ConfigureAwait(false))
        {
            if (entry.ExpiresUtc is { } exp && exp <= utcNow)
                continue;

            target.Add((key, ToSnapshotEntry(entry)));
        }
    }

    private static NodeCacheEntry<object?> ToSnapshotEntry(NodeCacheEntry<T> source)
    {
        var normalized = CacheEntryCodec.NormalizeValue(source.Value);
        if (source is NodeCacheEntry<object?> objectEntry && Equals(normalized, objectEntry.Value))
            return objectEntry;

        return new NodeCacheEntry<object?>(normalized, source.Version, source.ExpiresUtc, source.Expiration, source.Tags);
    }
}
