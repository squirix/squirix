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
    private readonly CacheExpiryAuthority _expiry;
    private readonly ILocalCacheSnapshotReader<T> _reader;

    /// <summary>Initializes a new instance of the <see cref="LocalCacheSnapshotCapture{T}" /> class.</summary>
    /// <param name="reader">The owner cache reader.</param>
    /// <param name="expiry">
    /// Who decides expiry. Under <see cref="CacheExpiryAuthority.CommittedRecords" /> an entry past its deadline is captured, since only a
    /// committed record removes it.
    /// </param>
    internal LocalCacheSnapshotCapture(ILocalCacheSnapshotReader<T> reader, CacheExpiryAuthority expiry = CacheExpiryAuthority.LocalClock)
    {
        ArgumentNullException.ThrowIfNull(reader);
        _reader = reader;
        _expiry = expiry;
    }

    public async ValueTask CaptureEntriesAsync(List<(CacheKey Key, NodeCacheEntry<object?> Entry)> target, DateTime utcNow, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(target);

        var capacity = _reader is ILocalCacheStats stats ? stats.EntryCount : 0;
        if (target.Capacity < capacity)
            target.Capacity = capacity;

        await foreach (var (key, entry) in _reader.EnumerateLiveAsync(cancellationToken).ConfigureAwait(false))
        {
            if (_expiry == CacheExpiryAuthority.LocalClock && entry.ExpiresUtc is { } exp && exp <= utcNow)
                continue;

            target.Add((key, ToSnapshotEntry(entry)));
        }
    }

    private static NodeCacheEntry<object?> ToSnapshotEntry(NodeCacheEntry<T> source)
    {
        var value = source.Normalize();
        return source is NodeCacheEntry<object?> entry && Equals(value, entry.Value) ? entry
            : new NodeCacheEntry<object?>(value, source.Version, source.ExpiresUtc, source.Expiration, source.Tags);
    }
}
