using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>In-memory cache store (KV + expiration).</summary>
/// <typeparam name="T">The stored value type.</typeparam>
internal sealed class PhysicalCache<T> : ILocalCache<T>, ILocalCacheSnapshotReader<T>, IAsyncDisposable
{
    private readonly LocalEvictionIndex _evictionIndex;
    private readonly ConcurrentDictionary<CacheKey, StoredEntry> _store = new();
    private readonly TimeProvider _timeProvider;

    public PhysicalCache(TimeProvider? timeProvider = null, EvictionOptions? eviction = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _evictionIndex = new LocalEvictionIndex(eviction ?? new EvictionOptions { Policy = EvictionPolicyType.Lru });
    }

    int ILocalCacheStats.EntryCount => _store.Count;

    private DateTime UtcNow => _timeProvider.GetUtcNow().UtcDateTime;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public async IAsyncEnumerable<(CacheKey Key, NodeCacheEntry<T> Entry)> EnumerateLiveAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        const int yieldEvery = 256;
        var produced = 0;

        foreach (var pair in _store)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!TryGetLive(pair.Key, out var stored))
                continue;

            yield return (pair.Key, ToEntry(stored));
            produced++;
            if (produced % yieldEvery is 0)
                await Task.Yield();
        }
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(TryGetLive(key, out var stored) ? ToEntry(stored) : null);
    }

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(TryGetLive(key, out var stored) ? new NodeCacheValueResult<T>(true, stored.Value) : new NodeCacheValueResult<T>(false, default));
    }

    public ValueTask InsertForDurableRecoveryAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var normalized = NormalizeEntry(entry);
        _store[key] = new StoredEntry(normalized.Value, normalized.ExpiresUtc, normalized.Version);
        _evictionIndex.TrackNew(key);
        return ValueTask.CompletedTask;
    }

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_store.TryRemove(key, out var stored))
            return ValueTask.FromResult(new CacheRemoveResult<T>(false, default));

        _evictionIndex.Untrack(key);
        if (stored.ExpiresUtc is { } expires && expires <= UtcNow)
            return ValueTask.FromResult(new CacheRemoveResult<T>(false, default));

        return ValueTask.FromResult(new CacheRemoveResult<T>(true, stored.Value));
    }

    public ValueTask<bool> RemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryGetLive(key, out var stored) || stored.ExpiresUtc is null)
            return ValueTask.FromResult(false);

        _store[key] = stored with { ExpiresUtc = null };
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> RemoveExpirationForDurableRecoveryAsync(CacheKey key, CancellationToken cancellationToken) => RemoveExpirationAsync(key, cancellationToken);

    public async ValueTask<bool> RemoveForDurableRecoveryAsync(CacheKey key, CancellationToken cancellationToken) =>
        (await RemoveAsync(key, cancellationToken).ConfigureAwait(false)).Removed;

    public ValueTask SetAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var normalized = NormalizeEntry(entry);
        _store[key] = new StoredEntry(normalized.Value, normalized.ExpiresUtc, normalized.Version);
        _evictionIndex.TrackNew(key);
        EnforceCapacityIfNeeded();
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> TouchAsync(CacheKey key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryGetLive(key, out var stored))
            return ValueTask.FromResult(false);

        var expires = UtcNow.Add(expiration);
        _store[key] = stored with { ExpiresUtc = expires };
        _evictionIndex.TouchExisting(key);
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> TouchExpirationForDurableRecoveryAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryGetLive(key, out var stored))
            return ValueTask.FromResult(false);

        _store[key] = stored with { ExpiresUtc = DateTime.SpecifyKind(expiresUtc, DateTimeKind.Utc) };
        _evictionIndex.TouchExisting(key);
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> TryAddAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (TryGetLive(key, out _))
            return ValueTask.FromResult(false);

        var normalized = NormalizeEntry(entry);
        var added = _store.TryAdd(key, new StoredEntry(normalized.Value, normalized.ExpiresUtc, normalized.Version));
        if (!added)
            return ValueTask.FromResult(false);

        _evictionIndex.TrackNew(key);
        EnforceCapacityIfNeeded();
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> UpdateAsync(CacheKey key, T? value, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        while (true)
        {
            if (!_store.TryGetValue(key, out var stored))
                return ValueTask.FromResult(false);

            if (stored.ExpiresUtc is { } expires && expires <= UtcNow)
            {
                if (!_store.TryRemove(key, out _))
                    continue;

                _evictionIndex.Untrack(key);
                return ValueTask.FromResult(false);
            }

            if (EqualityComparer<T?>.Default.Equals(stored.Value, value))
                return ValueTask.FromResult(true);

            var updated = stored with { Value = value };
            if (!_store.TryUpdate(key, updated, stored))
                continue;
            _evictionIndex.TouchExisting(key);
            return ValueTask.FromResult(true);
        }
    }

    private static NodeCacheEntry<T> ToEntry(StoredEntry stored) => new()
    {
        Value = stored.Value,
        ExpiresUtc = stored.ExpiresUtc,
        Version = stored.Version,
    };

    private void EnforceCapacityIfNeeded()
    {
        if (_evictionIndex.BoundedCapacity is not { } cap)
            return;

        while (_store.Count > cap)
        {
            if (!_evictionIndex.TryPopEvictionVictim(out var victim))
                break;

            _ = _store.TryRemove(victim, out _);
        }
    }

    private NodeCacheEntry<T> NormalizeEntry(NodeCacheEntry<T> entry)
    {
        var version = entry.Version > 0 ? entry.Version : 1;
        var expires = entry.ExpiresUtc;
        if (expires is null && entry.Expiration is { } expiration)
            expires = UtcNow.Add(expiration);

        return new NodeCacheEntry<T>
        {
            Value = entry.Value,
            ExpiresUtc = expires,
            Expiration = entry.Expiration,
            Version = version,
        };
    }

    private bool TryGetLive(CacheKey key, out StoredEntry stored)
    {
        if (!_store.TryGetValue(key, out stored))
            return false;

        if (stored.ExpiresUtc is { } expires && expires <= UtcNow)
        {
            _ = _store.TryRemove(key, out _);
            _evictionIndex.Untrack(key);
            return false;
        }

        _evictionIndex.TouchExisting(key);
        return true;
    }

    private readonly record struct StoredEntry(T? Value, DateTime? ExpiresUtc, long Version);

    /// <summary>Tracks per-key ordering and frequency metadata used for capacity-based eviction (LRU, LFU, FIFO).</summary>
    private sealed class LocalEvictionIndex
    {
        private readonly Lock _lock = new();
        private readonly Dictionary<CacheKey, (LinkedListNode<CacheKey> Node, long Freq)> _meta = [];
        private readonly EvictionOptions _options;
        private readonly LinkedList<CacheKey> _order = [];

        public LocalEvictionIndex(EvictionOptions options)
        {
            _options = options;
        }

        /// <summary>Gets the bounded capacity limit when configured.</summary>
        internal int? BoundedCapacity => _options.Capacity;

        internal void TouchExisting(CacheKey key)
        {
            if (_options.Capacity is null)
                return;

            lock (_lock)
            {
                if (!_meta.TryGetValue(key, out var m))
                    return;

                switch (_options.Policy)
                {
                    case EvictionPolicyType.Fifo:
                        break;

                    case EvictionPolicyType.Lru:
                        _order.Remove(m.Node);
                        var newNode = _order.AddFirst(key);
                        _meta[key] = (newNode, m.Freq + 1);
                        break;

                    case EvictionPolicyType.Lfu:
                        _meta[key] = (m.Node, m.Freq + 1);
                        break;

                    default:
                        _order.Remove(m.Node);
                        var nn = _order.AddFirst(key);
                        _meta[key] = (nn, m.Freq + 1);
                        break;
                }
            }
        }

        internal void TrackNew(CacheKey key)
        {
            if (_options.Capacity is null)
                return;

            lock (_lock)
            {
                if (_meta.ContainsKey(key))
                    return;

                var node = _order.AddFirst(key);
                _meta[key] = (node, 1);
            }
        }

        internal bool TryPopEvictionVictim([NotNullWhen(true)] out CacheKey? victim)
        {
            victim = null;
            if (_options.Capacity is null)
                return false;

            lock (_lock)
            {
                if (_meta.Count is 0)
                    return false;

                var candidate = _options.Policy switch
                {
                    EvictionPolicyType.Fifo => _order.Last?.Value,
                    EvictionPolicyType.Lru => _order.Last?.Value,
                    EvictionPolicyType.Lfu => GetLeastFrequentlyUsedKey(),
                    _ => throw new InvalidOperationException($"Unsupported eviction policy: {_options.Policy}."),
                };

                if (candidate is null)
                    return false;

                victim = candidate;

                if (!_meta.TryGetValue(victim, out var metadata))
                    return true;
                _order.Remove(metadata.Node);
                _ = _meta.Remove(victim);
            }

            return true;
        }

        internal void Untrack(CacheKey key)
        {
            if (_options.Capacity is null)
                return;

            lock (_lock)
            {
                if (!_meta.TryGetValue(key, out var m))
                    return;

                _order.Remove(m.Node);
                _ = _meta.Remove(key);
            }
        }

        private CacheKey? GetLeastFrequentlyUsedKey()
        {
            CacheKey? chosen = null;
            var minFrequency = long.MaxValue;

            foreach (var pair in _meta)
            {
                if (pair.Value.Freq >= minFrequency)
                    continue;

                minFrequency = pair.Value.Freq;
                chosen = pair.Key;
            }

            return chosen;
        }
    }
}
