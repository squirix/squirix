using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>
/// Applies memory admission checks before delegating to the inner pipeline on local-owner write paths.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class MemoryAdmissionCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly IMemoryUsageAccounting _accounting;
    private readonly ICacheEntrySizeEstimator<T> _estimator;
    private readonly IMemoryPressureGate _gate;
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly INodeLocator _ring;
    private readonly string _self;

    public MemoryAdmissionCacheDecorator(
        ILogicalNamespacedCache<T> inner,
        IMemoryPressureGate gate,
        ICacheEntrySizeEstimator<T> estimator,
        IMemoryUsageAccounting accounting,
        string self,
        INodeLocator ring)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _gate = gate ?? throw new ArgumentNullException(nameof(gate));
        _estimator = estimator ?? throw new ArgumentNullException(nameof(estimator));
        _accounting = accounting ?? throw new ArgumentNullException(nameof(accounting));
        _self = self ?? throw new ArgumentNullException(nameof(self));
        _ring = ring ?? throw new ArgumentNullException(nameof(ring));
    }

    public ValueTask AddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => AddAsync(
        cacheName,
        key,
        new CacheEntry<T> { Value = value, Version = 1 },
        cancellationToken);

    public async ValueTask AddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
        {
            await _inner.AddAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false);
            return;
        }

        var keyValue = new CacheKey(cacheName, key);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        AdmitReplaceOrInsert(keyValue, existing, entry, MemoryPressureAdmissionOperations.Add);
        await _inner.AddAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false);
        AccountReplaceOrInsert(keyValue, existing, entry);
    }

    public ValueTask<bool> ContainsAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.ContainsAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<TimeSpan?> GetExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetExpirationAsync(cacheName, key, cancellationToken);

    public async ValueTask<CacheValueResult<T>> GetOrAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.GetOrAddAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false);

        var keyValue = new CacheKey(cacheName, key);
        var existing = await _inner.TryGetValueAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing.Found)
            return existing;

        AdmitReplaceOrInsert(keyValue, null, entry, MemoryPressureAdmissionOperations.TryAdd);
        var result = await _inner.GetOrAddAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false);
        if (result.Found)
            AccountInsert(keyValue, entry);

        return result;
    }

    public ValueTask<T?> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetValueAsync(cacheName, key, cancellationToken);

    public async ValueTask<bool> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.RemoveAsync(cacheName, key, cancellationToken).ConfigureAwait(false);

        var keyValue = new CacheKey(cacheName, key);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        var removed = await _inner.RemoveAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (removed && existing is not null)
            AccountRemove(keyValue, existing);

        return removed;
    }

    public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.RemoveExpirationAsync(cacheName, key, cancellationToken);

    public ValueTask SetAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => SetAsync(
        cacheName,
        key,
        new CacheEntry<T> { Value = value, Version = 1 },
        cancellationToken);

    public async ValueTask SetAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
        {
            await _inner.SetAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false);
            return;
        }

        var keyValue = new CacheKey(cacheName, key);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        AdmitReplaceOrInsert(keyValue, existing, entry, MemoryPressureAdmissionOperations.Set);
        await _inner.SetAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false);
        AccountReplaceOrInsert(keyValue, existing, entry);
    }

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        _inner.TouchAsync(cacheName, key, expiration, cancellationToken);

    public ValueTask<bool> TryAddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => TryAddAsync(
        cacheName,
        key,
        new CacheEntry<T> { Value = value, Version = 1 },
        cancellationToken);

    public async ValueTask<bool> TryAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.TryAddAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false);

        var keyValue = new CacheKey(cacheName, key);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing is not null)
            return false;

        AdmitReplaceOrInsert(keyValue, null, entry, MemoryPressureAdmissionOperations.TryAdd);
        if (!await _inner.TryAddAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false))
            return false;

        AccountInsert(keyValue, entry);
        return true;
    }

    public ValueTask<CacheValueResult<T>> TryGetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.TryGetValueAsync(cacheName, key, cancellationToken);

    public async ValueTask<CacheRemoveResult<T>> TryRemoveAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.TryRemoveAsync(cacheName, key, cancellationToken).ConfigureAwait(false);

        var keyValue = new CacheKey(cacheName, key);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        var result = await _inner.TryRemoveAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (result.Removed && existing is not null)
            AccountRemove(keyValue, existing);

        return result;
    }

    public async ValueTask<bool> UpdateAsync(string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.UpdateAsync(cacheName, key, value, cancellationToken).ConfigureAwait(false);

        var keyValue = new CacheKey(cacheName, key);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing is null)
            return false;

        var replacement = new CacheEntry<T>
        {
            Value = value,
            ExpiresUtc = existing.ExpiresUtc,
            Expiration = existing.Expiration,
            Version = existing.Version,
        };
        AdmitReplaceOrInsert(keyValue, existing, replacement, MemoryPressureAdmissionOperations.Set);
        var updated = await _inner.UpdateAsync(cacheName, key, value, cancellationToken).ConfigureAwait(false);
        if (updated)
            AccountReplaceOrInsert(keyValue, existing, replacement);

        return updated;
    }

    private void AccountInsert(CacheKey key, CacheEntry<T> entry) => _accounting.AddEntry(_estimator.EstimateBytes(key, entry, false));

    private void AccountRemove(CacheKey key, CacheEntry<T> entry) => _accounting.RemoveEntry(_estimator.EstimateBytes(key, entry, false));

    private void AccountReplaceOrInsert(CacheKey key, CacheEntry<T>? existing, CacheEntry<T> replacement)
    {
        if (existing is null)
        {
            AccountInsert(key, replacement);
            return;
        }

        _accounting.ReplaceEntry(_estimator.EstimateBytes(key, existing, false), _estimator.EstimateBytes(key, replacement, false));
    }

    private void AdmitReplaceOrInsert(CacheKey key, CacheEntry<T>? existing, CacheEntry<T> proposed, string operation)
    {
        var growth = MemoryAdmissionJournalExtensions.ComputeNetGrowthForReplace(key, existing, false, proposed, false, _estimator, out var magnitudeUnknown);
        _gate.ThrowIfMemoryGrowingWriteRejected(growth, magnitudeUnknown, operation);
    }

    private bool IsLocal(string cacheName, string key) => string.Equals(_ring.GetOwner(CacheName.NormalizeUnvalidated(cacheName), key), _self, StringComparison.Ordinal);
}
