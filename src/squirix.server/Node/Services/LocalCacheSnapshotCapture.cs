using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Services;

/// <summary>Bridges owner-cache enumeration into snapshot-ready object entries.</summary>
/// <typeparam name="T">The stored cache value type.</typeparam>
[Immutable]
internal sealed class LocalCacheSnapshotCapture<T> : ISnapshotEntryCapture
{
    private readonly ILocalCacheSnapshotReader<T> _reader;

    internal LocalCacheSnapshotCapture(ILocalCacheSnapshotReader<T> reader)
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
        var value = source.Normalize();
        if (source is NodeCacheEntry<object?> entry && Equals(value, entry.Value))
            return entry;

        return new NodeCacheEntry<object?>(value, source.Version, source.ExpiresUtc, source.Expiration, source.Tags);
    }
}
