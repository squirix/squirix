using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Services;

/// <summary>Captures typed entries from the owner cache for the snapshot writer.</summary>
[Immutable]
internal sealed class LocalCacheSnapshotCapture : ISnapshotEntryCapture
{
    private readonly CacheExpiryAuthority _expiry;
    private readonly ILocalCacheSnapshotReader<StoredValue> _reader;

    /// <summary>Initializes a new instance of the <see cref="LocalCacheSnapshotCapture" /> class.</summary>
    /// <param name="reader">The owner cache reader.</param>
    /// <param name="expiry">
    /// Who decides expiry. Under <see cref="CacheExpiryAuthority.CommittedRecords" /> an entry past its deadline is captured, since only a
    /// committed record removes it.
    /// </param>
    internal LocalCacheSnapshotCapture(ILocalCacheSnapshotReader<StoredValue> reader, CacheExpiryAuthority expiry = CacheExpiryAuthority.LocalClock)
    {
        ArgumentNullException.ThrowIfNull(reader);
        _reader = reader;
        _expiry = expiry;
    }

    public async ValueTask CaptureEntriesAsync(List<(CacheKey Key, NodeCacheEntry<StoredValue> Entry)> target, DateTime utcNow, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(target);

        var capacity = _reader is ILocalCacheStats stats ? stats.EntryCount : 0;
        if (target.Capacity < capacity)
            target.Capacity = capacity;

        await foreach (var (key, entry) in _reader.EnumerateLiveAsync(cancellationToken).ConfigureAwait(false))
        {
            if (_expiry == CacheExpiryAuthority.LocalClock && entry.ExpiresUtc is { } exp && exp <= utcNow)
                continue;

            target.Add((key, entry));
        }
    }
}
