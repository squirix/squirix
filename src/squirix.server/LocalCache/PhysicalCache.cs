using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Utils;

namespace Squirix.Server.LocalCache;

/// <summary>In-memory cache store (KV and expiration).</summary>
/// <typeparam name="T">The stored value type.</typeparam>
/// <remarks>
/// Every mutation and lookup runs under a single <see cref="_lock" />. The value store and the
/// eviction-order bookkeeping used to be two independently synchronized structures (a lock-free
/// <c language="csharp">ConcurrentDictionary</c> plus a separately locked index). Any interleaving of concurrent
/// inserts, removals, expirations, and evictions across those two structures could leave them
/// out of sync - a key present in one but not the other (see issue #444, and the eviction-race
/// family it extends, #387). Merging both into one <see cref="Node" /> per key, mutated only
/// inside <see cref="_lock" />, makes that class of bug structurally impossible: there is no
/// second structure left to diverge from.
/// </remarks>
[Immutable]
internal sealed class PhysicalCache<T> : ILocalCache<T>, ILocalCacheSnapshotReader<T>
{
    private readonly EvictionOptions _eviction;
    private readonly Lock _lock = new();
    private readonly LinkedList<CacheKey> _order = new();
    private readonly Dictionary<CacheKey, Node> _store = [];
    private readonly TimeProvider _timeProvider;

    internal PhysicalCache(TimeProvider? timeProvider = null, EvictionOptions? eviction = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _eviction = eviction ?? new EvictionOptions { Policy = EvictionPolicyType.Lru };
    }

    int ILocalCacheStats.EntryCount
    {
        get
        {
            lock (_lock)
                return _store.Count;
        }
    }

    private DateTime UtcNow => _timeProvider.GetUtcNow().UtcDateTime;

    public async IAsyncEnumerable<(CacheKey Key, NodeCacheEntry<T> Entry)> EnumerateLiveAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        const int yieldEvery = 256;

        // Snapshot the key set under the lock, then look up one key at a time under its own short
        // lock acquisition. This never holds _lock across a `yield return` or an `await`, and the
        // lookup deliberately does not touch eviction order or frequency: a snapshot read must not
        // reorder LRU or inflate LFU counts for entries it merely enumerates.
        CacheKey[] keys;
        lock (_lock)
        {
            keys = new CacheKey[_store.Count];
            _store.Keys.CopyTo(keys, 0);
        }

        var produced = 0;
        foreach (var key in keys)
        {
            cancellationToken.ThrowIfCancellationRequested();

            NodeCacheEntry<T>? entry = null;
            lock (_lock)
            {
                if (_store.TryGetValue(key, out var node) && (node.ExpiresUtc == null || node.ExpiresUtc > UtcNow))
                    entry = new NodeCacheEntry<T>(node.Value, node.Version, node.ExpiresUtc, tags: node.Tags);
            }

            if (entry == null)
                continue;

            yield return (key, entry);
            produced++;
            if (produced % yieldEvery == 0)
                await Task.Yield();
        }
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
            return ValueTask.FromResult(TryGetLiveLocked(key, out var node) ? new NodeCacheEntry<T>(node.Value, node.Version, node.ExpiresUtc, tags: node.Tags) : null);
    }

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
            return ValueTask.FromResult(TryGetLiveLocked(key, out var node) ? new NodeCacheValueResult<T>(true, node.Value) : new NodeCacheValueResult<T>(false, default));
    }

    public ValueTask InsertRecoveryAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) => SetAsync(key, entry, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            var (removed, value) = RemoveLocked(key);
            return ValueTask.FromResult(new CacheRemoveResult<T>(removed, value));
        }
    }

    public ValueTask<bool> RemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            if (!TryGetLiveLocked(key, out var node) || node.ExpiresUtc == null)
                return ValueTask.FromResult(false);

            node.ExpiresUtc = null;
            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<bool> RemoveExpirationRecoveryAsync(CacheKey key, CancellationToken cancellationToken) => RemoveExpirationAsync(key, cancellationToken);

    public ValueTask<bool> RemoveRecoveryAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
            return ValueTask.FromResult(RemoveLocked(key).Removed);
    }

    public ValueTask SetAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
            _ = UpsertLocked(key, entry, false);
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> TouchAsync(CacheKey key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            if (!TryGetLiveLocked(key, out var node))
                return ValueTask.FromResult(false);

            node.ExpiresUtc = UtcNow.SaturatedAdd(expiration);
            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<bool> TouchExpirationRecoveryAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            if (!TryGetLiveLocked(key, out var node))
                return ValueTask.FromResult(false);

            node.ExpiresUtc = DateTime.SpecifyKind(expiresUtc, DateTimeKind.Utc);
            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<bool> TryAddAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
            return ValueTask.FromResult(UpsertLocked(key, entry, true));
    }

    public ValueTask<bool> UpdateAsync(CacheKey key, T? value, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            if (!TryGetLiveLocked(key, out var node))
                return ValueTask.FromResult(false);

            node.Value = value;
            return ValueTask.FromResult(true);
        }
    }

    private void EnforceCapacityLocked()
    {
        if (_eviction.Capacity is not { } cap)
            return;

        while (_store.Count > cap)
        {
            var candidate = _eviction.Policy switch
            {
                EvictionPolicyType.Fifo => _order.Last?.Value,
                EvictionPolicyType.Lru => _order.Last?.Value,
                EvictionPolicyType.Lfu => GetLeastFrequentlyUsedKeyLocked(),
                _ => throw new InvalidOperationException("Unsupported eviction policy."),
            };

            if (candidate == null)
                break;

            if (!_store.Remove(candidate, out var node))
                break; // Shouldn't happen: order and store are updated together now.

            UntrackLocked(node);
        }
    }

    private CacheKey? GetLeastFrequentlyUsedKeyLocked()
    {
        CacheKey? chosen = null;
        var minFrequency = long.MaxValue;

        for (var node = _order.Last; node != null; node = node.Previous)
        {
            if (!_store.TryGetValue(node.Value, out var entry) || entry.Frequency >= minFrequency)
                continue;

            minFrequency = entry.Frequency;
            chosen = node.Value;
        }

        return chosen;
    }

    private (bool Removed, T? Value) RemoveLocked(CacheKey key)
    {
        if (!_store.Remove(key, out var node))
            return (false, default);

        UntrackLocked(node);

        if (node.ExpiresUtc is { } expires && expires <= UtcNow)
            return (false, default);

        return (true, node.Value);
    }

    private void TouchOrderLocked(CacheKey key, Node node)
    {
        if (_eviction.Capacity == null || _eviction.Policy is EvictionPolicyType.Fifo)
            return;

        if (_eviction.Policy is EvictionPolicyType.Lfu)
        {
            node.Frequency++;
            return;
        }

        if (node.OrderNode != null)
            _order.Remove(node.OrderNode);
        node.OrderNode = _order.AddFirst(key);
        node.Frequency++;
    }

    private bool TryGetLiveLocked(CacheKey key, [NotNullWhen(true)] out Node? node)
    {
        if (!_store.TryGetValue(key, out node))
            return false;

        if (node.ExpiresUtc is { } expires && expires <= UtcNow)
        {
            _ = _store.Remove(key);
            UntrackLocked(node);
            node = null;
            return false;
        }

        TouchOrderLocked(key, node);
        return true;
    }

    private void UntrackLocked(Node node)
    {
        if (node.OrderNode == null)
            return;

        _order.Remove(node.OrderNode);
        node.OrderNode = null;
    }

    private bool UpsertLocked(CacheKey key, NodeCacheEntry<T> entry, bool insertOnly)
    {
        var normalized = NormalizeExpiration(entry);
        _ = _store.TryGetValue(key, out var node);
        if (node != null && insertOnly && node.ExpiresUtc is { } existingExpires && existingExpires <= UtcNow)
        {
            UntrackLocked(node);
            node = null;
        }

        if (node == null)
        {
            node = new Node(normalized.Value, normalized.ExpiresUtc, normalized.Version, normalized.Tags);
            _store[key] = node;

            if (_eviction.Capacity != null)
            {
                node.OrderNode = _order.AddFirst(key);
                node.Frequency = 1;
            }

            EnforceCapacityLocked();
            return true;
        }

        if (insertOnly)
            return false;

        node.Value = normalized.Value;
        node.ExpiresUtc = normalized.ExpiresUtc;
        node.Version = normalized.Version;
        node.Tags = normalized.Tags;
        TouchOrderLocked(key, node);
        return false;

        NodeCacheEntry<T> NormalizeExpiration(NodeCacheEntry<T> candidate)
        {
            var version = candidate.Version > 0 ? candidate.Version : 1;
            var expires = candidate.ExpiresUtc;
            if (candidate.Expiration is not { } expiration)
                return new NodeCacheEntry<T>(candidate.Value, version, expires, candidate.Expiration, candidate.Tags);
            var relativeDeadline = UtcNow.SaturatedAdd(expiration);
            if (expires == null || relativeDeadline < expires)
                expires = relativeDeadline;

            return new NodeCacheEntry<T>(candidate.Value, version, expires, candidate.Expiration, candidate.Tags);
        }
    }

    /// <summary>
    /// A single live entry plus its eviction-order bookkeeping. Merging value and order state
    /// into one object per key - instead of a value in one structure and metadata in another -
    /// is what removes the divergence race: there's nothing left to fall out of sync.
    /// </summary>
    private sealed class Node
    {
        /// <summary>Initializes a new instance of the <see cref="Node" /> class.</summary>
        /// <param name="value">The cached value.</param>
        /// <param name="expiresUtc">The expiration time.</param>
        /// <param name="version">The version.</param>
        /// <param name="tags">The tags.</param>
        internal Node(T? value, DateTime? expiresUtc, long version, FrozenDictionary<string, string>? tags)
        {
            Value = value;
            ExpiresUtc = expiresUtc;
            Version = version;
            Tags = tags;
        }

        internal DateTime? ExpiresUtc { get; set; }

        /// <summary>Gets or sets the access frequency, maintained only for the LFU policy.</summary>
        internal long Frequency { get; set; } = 1;

        /// <summary>Gets or sets the node in the shared eviction-order list, or <see langword="null" /> when eviction is unbounded.</summary>
        internal LinkedListNode<CacheKey>? OrderNode { get; set; }

        internal FrozenDictionary<string, string>? Tags { get; set; }

        internal T? Value { get; set; }

        internal long Version { get; set; }
    }
}
