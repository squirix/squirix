using System;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Utils;

namespace Squirix.Server.LocalCache;

/// <summary>In-memory cache store (KV and expiration).</summary>
/// <typeparam name="T">The stored value type.</typeparam>
[Immutable]
internal sealed class PhysicalCache<T> : ILocalCache<T>, ILocalCacheSnapshotReader<T>
{
    private readonly LocalEvictionIndex _evictionIndex;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<CacheKey, StoredEntry> _store = new();
    private readonly TimeProvider _timeProvider;

    internal PhysicalCache(TimeProvider? timeProvider = null, EvictionOptions? eviction = null, ILogger? logger = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _evictionIndex = new LocalEvictionIndex(eviction ?? new EvictionOptions { Policy = EvictionPolicyType.Lru });
        _logger = logger ?? NullLogger.Instance;
    }

    int ILocalCacheStats.EntryCount => _store.Count;

    private DateTime UtcNow => _timeProvider.GetUtcNow().UtcDateTime;

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
            if (produced % yieldEvery == 0)
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

    public ValueTask InsertRecoveryAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var normalized = NormalizeEntry(entry);
        _store[key] = new StoredEntry(normalized.Value, normalized.ExpiresUtc, normalized.Version, normalized.Tags);
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
        if (!TryGetLive(key, out var stored) || stored.ExpiresUtc == null)
            return ValueTask.FromResult(false);

        var updated = stored with { ExpiresUtc = null };
        return ValueTask.FromResult(_store.TryUpdate(key, updated, stored));
    }

    public ValueTask<bool> RemoveExpirationRecoveryAsync(CacheKey key, CancellationToken cancellationToken) => RemoveExpirationAsync(key, cancellationToken);

    public async ValueTask<bool> RemoveRecoveryAsync(CacheKey key, CancellationToken cancellationToken) =>
        (await RemoveAsync(key, cancellationToken).ConfigureAwait(false)).Removed;

    public ValueTask SetAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var normalized = NormalizeEntry(entry);
        _store[key] = new StoredEntry(normalized.Value, normalized.ExpiresUtc, normalized.Version, normalized.Tags);
        _evictionIndex.TrackNew(key);
        EnforceCapacityIfNeeded();
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> TouchAsync(CacheKey key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryGetLive(key, out var stored))
            return ValueTask.FromResult(false);

        var expires = UtcNow.SaturatedAdd(expiration);
        var updated = stored with { ExpiresUtc = expires };
        if (!_store.TryUpdate(key, updated, stored))
            return ValueTask.FromResult(false);

        _evictionIndex.TouchExisting(key);
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> TouchExpirationRecoveryAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryGetLive(key, out var stored))
            return ValueTask.FromResult(false);

        var updated = stored with { ExpiresUtc = DateTime.SpecifyKind(expiresUtc, DateTimeKind.Utc) };
        if (!_store.TryUpdate(key, updated, stored))
            return ValueTask.FromResult(false);

        _evictionIndex.TouchExisting(key);
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> TryAddAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (TryGetLive(key, out _))
            return ValueTask.FromResult(false);

        var normalized = NormalizeEntry(entry);
        var added = _store.TryAdd(key, new StoredEntry(normalized.Value, normalized.ExpiresUtc, normalized.Version, normalized.Tags));
        if (!added)
            return ValueTask.FromResult(false);

        _evictionIndex.TrackNew(key);
        EnforceCapacityIfNeeded();
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> UpdateAsync(CacheKey key, T? value, CancellationToken cancellationToken)
    {
        const int maxAttempts = 64;
        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (TryApplyUpdate(key, value, out var completed))
                return ValueTask.FromResult(completed);
        }

        cancellationToken.ThrowIfCancellationRequested();
        LogManager.PhysicalCacheUpdateRetriesExhausted(_logger, maxAttempts, key.Namespace, key.Key);
        return ValueTask.FromResult(false);

        bool TryApplyUpdate(CacheKey updateKey, T? updateValue, out bool completed)
        {
            completed = false;
            if (!_store.TryGetValue(updateKey, out var stored))
                return true;

            if (TryRemoveExpired(updateKey, stored, out var removedAndRetry))
                return !removedAndRetry;

            // TryReplaceValue performs a CAS that confirms the entry is still present; a concurrent
            // expiry reclaim or capacity eviction makes it fail so we retry instead of reporting a
            // successful update on an absent key (issue #438). The eviction index is touched only
            // after the CAS succeeds.
            if (!TryReplaceValue(updateKey, stored, updateValue))
                return false;

            _evictionIndex.TouchExisting(updateKey);
            completed = true;
            return true;
        }
    }

    private static NodeCacheEntry<T> ToEntry(StoredEntry stored) => new(stored.Value, stored.Version, stored.ExpiresUtc, tags: stored.Tags);

    private void EnforceCapacityIfNeeded()
    {
        if (_evictionIndex.BoundedCapacity is not { } cap)
            return;

        while (_store.Count > cap)
        {
            if (!_evictionIndex.TryEvictOne(RemoveItem))
                break;
        }

        return;

        void RemoveItem(CacheKey key)
        {
            _ = _store.TryRemove(key, out _);
        }
    }

    private NodeCacheEntry<T> NormalizeEntry(NodeCacheEntry<T> entry)
    {
        var version = entry.Version > 0 ? entry.Version : 1;
        var expires = entry.ExpiresUtc;
        if (entry.Expiration is { } expiration)
        {
            var relativeDeadline = UtcNow.SaturatedAdd(expiration);
            if (expires == null || relativeDeadline < expires)
                expires = relativeDeadline;
        }

        return new NodeCacheEntry<T>(entry.Value, version, expires, entry.Expiration, entry.Tags);
    }

    private bool TryGetLive(CacheKey key, out StoredEntry stored)
    {
        while (true)
        {
            if (!_store.TryGetValue(key, out stored))
                return false;

            if (TryRemoveExpired(key, stored, out var removedAndRetry))
            {
                if (removedAndRetry)
                    continue;

                stored = default;
                return false;
            }

            _evictionIndex.TouchExisting(key);
            return true;
        }
    }

    private bool TryRemoveExpired(CacheKey key, StoredEntry stored, out bool removedAndRetry)
    {
        removedAndRetry = false;
        if (stored.ExpiresUtc is not { } expires || expires > UtcNow)
            return false;

        if (!_store.TryRemove(new KeyValuePair<CacheKey, StoredEntry>(key, stored)))
        {
            removedAndRetry = true;
            return true;
        }

        _evictionIndex.Untrack(key);
        if (_store.ContainsKey(key))
            _evictionIndex.TrackNew(key);
        return true;
    }

    private bool TryReplaceValue(CacheKey key, StoredEntry stored, T? value)
    {
        var updated = stored with { Value = value };
        return _store.TryUpdate(key, updated, stored);
    }

    /// <summary>Stores a single live entry's value, expiration, version, and extension-facing tags.</summary>
    /// <param name="Value">The cached value.</param>
    /// <param name="ExpiresUtc">The absolute UTC expiration, if any.</param>
    /// <param name="Version">The monotonic entry version.</param>
    /// <param name="Tags">The immutable tag dictionary shared with the originating entry; may be <see langword="null" />.</param>
    /// <remarks>
    /// Carries the tag dictionary so user metadata survives restarts and snapshot recovery.
    /// Costs one reference per stored entry even when tags are absent.
    /// </remarks>
    [Immutable]
    private readonly record struct StoredEntry(T? Value, DateTime? ExpiresUtc, long Version, FrozenDictionary<string, string>? Tags);

    /// <summary>Tracks per-key ordering and frequency metadata used for capacity-based eviction (LRU, LFU, FIFO).</summary>
    [Immutable]
    private sealed class LocalEvictionIndex
    {
        private readonly Lock _lock = new();
        private readonly Dictionary<CacheKey, (LinkedListNode<CacheKey> Node, long Freq)> _meta = [];
        private readonly EvictionOptions _options;
        private readonly LinkedList<CacheKey> _order = [];

        internal LocalEvictionIndex(EvictionOptions options)
        {
            _options = options;
        }

        /// <summary>Gets the bounded capacity limit when configured.</summary>
        internal int? BoundedCapacity => _options.Capacity;

        internal void TouchExisting(CacheKey key)
        {
            if (_options.Capacity == null)
                return;

            lock (_lock)
            {
                if (!_meta.TryGetValue(key, out var m))
                    return;

                ApplyTouchPolicy(key, m);
            }
        }

        internal void TrackNew(CacheKey key)
        {
            if (_options.Capacity == null)
                return;

            lock (_lock)
            {
                if (_meta.ContainsKey(key))
                    return;

                var node = _order.AddFirst(key);
                _meta[key] = (node, 1);
            }
        }

        internal bool TryEvictOne(Action<CacheKey> removeFromStore)
        {
            if (_options.Capacity == null)
                return false;

            lock (_lock)
            {
                if (_meta.Count == 0)
                    return false;

                var candidate = _options.Policy switch
                {
                    EvictionPolicyType.Fifo => _order.Last?.Value,
                    EvictionPolicyType.Lru => _order.Last?.Value,
                    EvictionPolicyType.Lfu => GetLeastFrequentlyUsedKey(),
                    _ => throw new InvalidOperationException("Unsupported eviction policy."),
                };

                if (candidate == null)
                    return false;

                if (_meta.TryGetValue(candidate, out var metadata))
                {
                    _order.Remove(metadata.Node);
                    _ = _meta.Remove(candidate);
                }

                removeFromStore(candidate);
                return true;
            }
        }

        internal void Untrack(CacheKey key)
        {
            if (_options.Capacity == null)
                return;

            lock (_lock)
            {
                if (!_meta.TryGetValue(key, out var m))
                    return;

                _order.Remove(m.Node);
                _ = _meta.Remove(key);
            }
        }

        private void ApplyTouchPolicy(CacheKey key, (LinkedListNode<CacheKey> Node, long Freq) m)
        {
            if (_options.Policy is EvictionPolicyType.Fifo)
                return;

            if (_options.Policy is EvictionPolicyType.Lfu)
            {
                _meta[key] = (m.Node, m.Freq + 1);
                return;
            }

            _order.Remove(m.Node);
            var newNode = _order.AddFirst(key);
            _meta[key] = (newNode, m.Freq + 1);
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
