using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Limits;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Applies memory admission checks before delegating to the inner pipeline on local-owner write paths.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class MemoryAdmissionCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly IMemoryUsageAccounting _accounting;
    private readonly ConcurrentDictionary<CacheKey, long> _accountedEntryBytes = new();
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

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetEntryAsync(cacheName, key, cancellationToken);

    public async ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);

        var keyValue = new CacheKey(cacheName, key);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing?.ExpiresUtc is null)
            return await _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);

        var replacement = CreateExpirationMetadataReplacement(existing, false);
        AdmitReplaceOrInsert(keyValue, existing, replacement, MemoryPressureAdmissionOperations.Set);
        var removed = await _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);
        if (removed)
            AccountReplaceOrInsert(keyValue, existing, replacement);

        return removed;
    }

    public async ValueTask SetEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
        {
            await _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
            return;
        }

        var keyValue = new CacheKey(cacheName, key);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        AdmitReplaceOrInsert(keyValue, existing, entry, MemoryPressureAdmissionOperations.Set);

        if (existing is null)
        {
            if (await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false))
            {
                AccountInsert(keyValue, entry);
                return;
            }

            await _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
            return;
        }

        await _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
        AccountReplaceOrInsert(keyValue, existing, entry);
    }

    public async ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken).ConfigureAwait(false);

        var keyValue = new CacheKey(cacheName, key);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing is null)
            return await _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken).ConfigureAwait(false);

        var replacement = CreateExpirationMetadataReplacement(existing, true);
        AdmitReplaceOrInsert(keyValue, existing, replacement, MemoryPressureAdmissionOperations.Set);
        var touched = await _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken).ConfigureAwait(false);
        if (touched)
            AccountReplaceOrInsert(keyValue, existing, replacement);

        return touched;
    }

    public async ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);

        var keyValue = new CacheKey(cacheName, key);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing is not null)
            return false;

        AdmitReplaceOrInsert(keyValue, null, entry, MemoryPressureAdmissionOperations.TryAdd);
        if (!await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false))
            return false;

        AccountInsert(keyValue, entry);
        return true;
    }

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetValueAsync(cacheName, key, cancellationToken);

    public async ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.RemoveAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);

        var keyValue = new CacheKey(cacheName, key);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        var result = await _inner.RemoveAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);
        if (result.Removed && existing is not null)
            AccountRemove(keyValue, existing);

        return result;
    }

    public async ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken).ConfigureAwait(false);

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
        EntryPayloadSizeGuard.EnsureEncodedLengthWithinLimit(replacement);
        AdmitReplaceOrInsert(keyValue, existing, replacement, MemoryPressureAdmissionOperations.Set);
        var updated = await _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken).ConfigureAwait(false);
        if (!updated || EqualityComparer<T?>.Default.Equals(existing.Value, value))
            return updated;

        AccountReplaceOrInsert(keyValue, existing, replacement);
        return updated;
    }

    private static CacheEntry<T> CreateExpirationMetadataReplacement(CacheEntry<T> existing, bool hasExpirationUtc) => new()
    {
        Value = existing.Value,
        ExpiresUtc = hasExpirationUtc ? existing.ExpiresUtc ?? DateTime.UnixEpoch : null,
        Expiration = existing.Expiration,
        Version = existing.Version,
        Tags = existing.Tags,
    };

    private void AccountInsert(CacheKey key, CacheEntry<T> entry)
    {
        var bytes = _estimator.EstimateBytes(key, entry, false);
        _accounting.AddEntry(bytes);
        _accountedEntryBytes[key] = bytes;
    }

    private void AccountRemove(CacheKey key, CacheEntry<T> entry)
    {
        _accounting.RemoveEntry(_estimator.EstimateBytes(key, entry, false));
        _ = _accountedEntryBytes.TryRemove(key, out _);
    }

    private void AccountReplaceOrInsert(CacheKey key, CacheEntry<T>? existing, CacheEntry<T> replacement)
    {
        if (existing is null)
        {
            AccountInsert(key, replacement);
            return;
        }

        var newBytes = _estimator.EstimateBytes(key, replacement, false);
        var baselineBytes = _estimator.EstimateBytes(key, existing, false);
        while (true)
        {
            var accountedBytes = _accountedEntryBytes.GetOrAdd(key, baselineBytes);
            if (accountedBytes == newBytes)
                return;

            if (!_accountedEntryBytes.TryUpdate(key, newBytes, accountedBytes))
                continue;
            _accounting.ReplaceEntry(accountedBytes, newBytes);
            return;
        }
    }

    private void AdmitReplaceOrInsert(CacheKey key, CacheEntry<T>? existing, CacheEntry<T> proposed, string operation)
    {
        var growth = MemoryAdmissionJournalExtensions.ComputeNetGrowthForReplace(key, existing, false, proposed, false, _estimator, out var magnitudeUnknown);
        _gate.ThrowIfMemoryGrowingWriteRejected(growth, magnitudeUnknown, operation);
    }

    private bool IsLocal(string cacheName, string key) => string.Equals(_ring.GetOwner(CacheName.NormalizeUnvalidated(cacheName), key), _self, StringComparison.Ordinal);
}
