using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Timing;

namespace Squirix.Server.LocalCache;

/// <summary>
/// In-memory cache store (KV + expiration).
/// </summary>
/// <typeparam name="T">The stored value type.</typeparam>
internal sealed class PhysicalCache<T> : ILocalCache<T>, ILocalCacheSnapshotReader<T>, IAsyncDisposable
{
    private readonly IClock _clock;
    private readonly LocalEvictionIndex _evictionIndex;
    private readonly ConcurrentDictionary<CacheKey, StoredEntry> _store = new();

    public PhysicalCache(IClock? clock = null, EvictionOptions? eviction = null)
    {
        _clock = clock ?? SystemClock.Instance;
        _evictionIndex = new LocalEvictionIndex(eviction ?? new EvictionOptions { Policy = EvictionPolicyType.Lru });
    }

    int ILocalCacheStats.EntryCount => _store.Count;

    public ValueTask AddAsync(CacheKey key, T? value, CancellationToken cancellationToken) => AddAsync(key, new CacheEntry<T> { Value = value }, cancellationToken);

    public async ValueTask AddAsync(CacheKey key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!await TryAddAsync(key, entry, cancellationToken).ConfigureAwait(false))
            throw new InvalidOperationException($"Key already exists: {key.Key}");
    }

    public ValueTask<bool> ContainsAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(TryGetLive(key, out _));
    }

    public ValueTask<TimeSpan?> GetExpirationAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryGetLive(key, out var stored) || stored.ExpiresUtc is not { } expires)
            return ValueTask.FromResult<TimeSpan?>(null);

        var remaining = expires - _clock.UtcNow;
        return ValueTask.FromResult<TimeSpan?>(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
    }

    public ValueTask<CacheEntry<T>?> GetValueAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(TryGetLive(key, out var stored) ? ToEntry(stored) : null);
    }

    public ValueTask<CacheValueResult<T>> TryGetValueAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(TryGetLive(key, out var stored) ? new CacheValueResult<T>(true, ToEntry(stored).Value) : new CacheValueResult<T>(false, default));
    }

    public ValueTask InsertAsync(CacheKey key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var normalized = NormalizeEntry(entry);
        _store[key] = new StoredEntry(normalized.Value, normalized.ExpiresUtc, normalized.Version);
        _evictionIndex.TrackNew(key);
        EnforceCapacityIfNeeded();
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> RemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryGetLive(key, out var stored) || stored.ExpiresUtc is null)
            return ValueTask.FromResult(false);

        _store[key] = stored with { ExpiresUtc = null };
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> RemoveAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryGetLive(key, out _))
            return ValueTask.FromResult(false);

        _ = _store.TryRemove(key, out _);
        _evictionIndex.Untrack(key);
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> TouchAsync(CacheKey key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryGetLive(key, out var stored))
            return ValueTask.FromResult(false);

        var expires = _clock.UtcNow.Add(expiration);
        _store[key] = stored with { ExpiresUtc = expires };
        _evictionIndex.TouchExisting(key);
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> UpdateAsync(CacheKey key, T? value, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryGetLive(key, out var stored))
            return ValueTask.FromResult(false);

        _store[key] = stored with { Value = value };
        _evictionIndex.TouchExisting(key);
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> TryAddAsync(CacheKey key, T? value, CancellationToken cancellationToken) => TryAddAsync(key, new CacheEntry<T> { Value = value }, cancellationToken);

    public ValueTask<bool> TryAddAsync(CacheKey key, CacheEntry<T> entry, CancellationToken cancellationToken)
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

    public ValueTask<CacheRemoveResult<T>> TryRemoveAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryGetLive(key, out var stored))
            return ValueTask.FromResult(new CacheRemoveResult<T>(false, default));

        _ = _store.TryRemove(key, out _);
        _evictionIndex.Untrack(key);
        return ValueTask.FromResult(new CacheRemoveResult<T>(true, stored.Value));
    }

    public ValueTask InsertForDurableRecoveryAsync(CacheKey key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var normalized = NormalizeEntry(entry);
        _store[key] = new StoredEntry(normalized.Value, normalized.ExpiresUtc, normalized.Version);
        _evictionIndex.TrackNew(key);
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> RemoveForDurableRecoveryAsync(CacheKey key, CancellationToken cancellationToken) => RemoveAsync(key, cancellationToken);

    public ValueTask<bool> RemoveExpirationForDurableRecoveryAsync(CacheKey key, CancellationToken cancellationToken) => RemoveExpirationAsync(key, cancellationToken);

    public ValueTask<bool> TouchExpirationForDurableRecoveryAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryGetLive(key, out var stored))
            return ValueTask.FromResult(false);

        _store[key] = stored with { ExpiresUtc = DateTime.SpecifyKind(expiresUtc, DateTimeKind.Utc) };
        _evictionIndex.TouchExisting(key);
        return ValueTask.FromResult(true);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public async IAsyncEnumerable<(CacheKey Key, CacheEntry<T> Entry)> EnumerateLiveAsync([EnumeratorCancellation] CancellationToken cancellationToken)
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

    private static CacheEntry<T> ToEntry(StoredEntry stored) => new()
    {
        Value = stored.Value,
        ExpiresUtc = stored.ExpiresUtc,
        Version = stored.Version,
    };

    private CacheEntry<T> NormalizeEntry(CacheEntry<T> entry)
    {
        var version = entry.Version > 0 ? entry.Version : 1;
        var expires = entry.ExpiresUtc;
        if (expires is null && entry.Expiration is { } expiration)
            expires = _clock.UtcNow.Add(expiration);

        return new CacheEntry<T>
        {
            Value = entry.Value,
            ExpiresUtc = expires,
            Expiration = entry.Expiration,
            Version = version,
        };
    }

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

    private bool TryGetLive(CacheKey key, out StoredEntry stored)
    {
        if (!_store.TryGetValue(key, out stored))
            return false;

        if (stored.ExpiresUtc is { } expires && expires <= _clock.UtcNow)
        {
            _ = _store.TryRemove(key, out _);
            _evictionIndex.Untrack(key);
            return false;
        }

        _evictionIndex.TouchExisting(key);
        return true;
    }

    private readonly record struct StoredEntry(T? Value, DateTime? ExpiresUtc, long Version);
}
