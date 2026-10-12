using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.LocalCache;

namespace Squirix.Server.Node.Services;

/// <summary>Walks the stored entries of the node by the replica group that owns each key.</summary>
/// <remarks>
/// The walk reads the entries the local cache reports as live, which includes an entry past its deadline that no committed record has
/// removed yet, and names the owner of each key through the node locator. It yields one entry at a time and allocates nothing per entry.
/// </remarks>
internal sealed class ReplicaGroupEntries
{
    private readonly INodeLocator _locator;
    private readonly ILocalCacheSnapshotReader<object?> _reader;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupEntries" /> class.</summary>
    /// <param name="reader">The stored entries.</param>
    /// <param name="locator">The key owner lookup, which names the group of a key.</param>
    internal ReplicaGroupEntries(ILocalCacheSnapshotReader<object?> reader, INodeLocator locator)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(locator);
        _reader = reader;
        _locator = locator;
    }

    /// <summary>Enumerates the stored entries, in the order of the local cache, optionally of one group and optionally only those past their deadline.</summary>
    /// <param name="groupId">The only group to yield, or <see langword="null" /> for every group.</param>
    /// <param name="expiredAtUtc">
    /// When set, only entries whose deadline is at or before this instant are yielded, and the owner is looked up for those alone;
    /// when <see langword="null" />, every entry is yielded.
    /// </param>
    /// <param name="cancellationToken">Cancellation token of the walk.</param>
    /// <returns>The matching entries.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    internal async IAsyncEnumerable<ReplicaGroupEntry> EnumerateAsync(
        string? groupId,
        DateTime? expiredAtUtc,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var (key, entry) in _reader.EnumerateLiveAsync(cancellationToken).ConfigureAwait(false))
        {
            if (expiredAtUtc is { } now && (entry.ExpiresUtc is not { } deadline || deadline.Ticks > now.Ticks))
                continue;

            var owner = _locator.GetOwner(key.Namespace, key.Key);
            if (groupId != null && !string.Equals(owner, groupId, StringComparison.Ordinal))
                continue;

            yield return new ReplicaGroupEntry(owner, key.Namespace, key.Key, entry);
        }
    }
}
