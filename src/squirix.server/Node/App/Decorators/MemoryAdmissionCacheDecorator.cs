using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Applies memory admission checks before delegating to the inner pipeline on local-owner write paths.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Mutable]
internal sealed class MemoryAdmissionCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ConcurrentDictionary<CacheKey, long> _accountedEntryBytes = new();
    private readonly IMemoryUsageAccounting _accounting;
    private readonly ICacheEntrySizeEstimator<T> _estimator;
    private readonly IMemoryPressureGate _gate;
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly INodeLocator _ring;
    private readonly string _self;

    internal MemoryAdmissionCacheDecorator(
        ILogicalNamespacedCache<T> inner,
        IMemoryPressureGate gate,
        ICacheEntrySizeEstimator<T> estimator,
        IMemoryUsageAccounting accounting,
        INodeLocator ring,
        string self)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(estimator);
        ArgumentNullException.ThrowIfNull(accounting);
        ArgumentNullException.ThrowIfNull(self);
        ArgumentNullException.ThrowIfNull(ring);
        _inner = inner;
        _gate = gate;
        _estimator = estimator;
        _accounting = accounting;
        _self = self;
        _ring = ring;
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetValueAsync(cacheName, key, cancellationToken);

    public async ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.RemoveAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);

        var keyValue = new CacheKey(cacheName, key);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        var result = await _inner.RemoveAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);
        if (result.Removed && existing != null)
            AccountRemove(keyValue, existing);

        return result;
    }

    public async ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);

        var keyValue = new CacheKey(cacheName, key);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing?.ExpiresUtc == null)
            return await _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);

        var replacement = CreateExpirationMetadataReplacement(existing, false);
        AdmitReplaceOrInsert(keyValue, existing, replacement, AdmissionOperations.Set);
        var removed = await _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);
        if (removed)
            AccountReplaceOrInsert(keyValue, existing, replacement);

        return removed;
    }

    public async ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
        {
            await _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
            return;
        }

        var keyValue = new CacheKey(cacheName, key);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        AdmitReplaceOrInsert(keyValue, existing, entry, AdmissionOperations.Set);

        if (existing == null)
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
        if (existing == null)
            return await _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken).ConfigureAwait(false);

        var replacement = CreateExpirationMetadataReplacement(existing, true);
        AdmitReplaceOrInsert(keyValue, existing, replacement, AdmissionOperations.Set);
        var touched = await _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken).ConfigureAwait(false);
        if (touched)
            AccountReplaceOrInsert(keyValue, existing, replacement);

        return touched;
    }

    public async ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);

        var keyValue = new CacheKey(cacheName, key);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing != null)
            return false;

        AdmitReplaceOrInsert(keyValue, null, entry, AdmissionOperations.TryAdd);
        if (!await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false))
            return false;

        AccountInsert(keyValue, entry);
        return true;
    }

    public async ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken).ConfigureAwait(false);

        var keyValue = new CacheKey(cacheName, key);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing == null)
            return false;

        var replacement = new NodeCacheEntry<T>
        {
            Value = value,
            ExpiresUtc = existing.ExpiresUtc,
            Expiration = existing.Expiration,
            Version = existing.Version,
        };
        AdmitReplaceOrInsert(keyValue, existing, replacement, AdmissionOperations.Set);
        var updated = await _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken).ConfigureAwait(false);
        if (!updated || EqualityComparer<T?>.Default.Equals(existing.Value, value))
            return updated;

        AccountReplaceOrInsert(keyValue, existing, replacement);
        return updated;
    }

    private static NodeCacheEntry<T> CreateExpirationMetadataReplacement(NodeCacheEntry<T> existing, bool hasExpirationUtc) => new(
        existing.Value,
        existing.Version,
        hasExpirationUtc ? existing.ExpiresUtc ?? DateTime.UnixEpoch : null,
        existing.Expiration,
        existing.Tags);

    private void AccountInsert(CacheKey key, NodeCacheEntry<T> entry)
    {
        var bytes = _estimator.EstimateBytes(key, entry, false);
        _accounting.AddEntry(bytes);
        _accountedEntryBytes[key] = bytes;
    }

    private void AccountRemove(CacheKey key, NodeCacheEntry<T> entry)
    {
        _ = entry;
        if (_accountedEntryBytes.TryRemove(key, out var accountedBytes))
            _accounting.RemoveEntry(accountedBytes);
    }

    private void AccountReplaceOrInsert(CacheKey key, NodeCacheEntry<T>? existing, NodeCacheEntry<T> replacement)
    {
        if (existing == null)
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

    private void AdmitReplaceOrInsert(CacheKey key, NodeCacheEntry<T>? existing, NodeCacheEntry<T> proposed, string operation)
    {
        var growth = MemoryAdmissionJournalExtensions.ComputeNetGrowthForReplace(key, existing, false, proposed, false, _estimator, out var magnitudeUnknown);
        _gate.ThrowIfMemoryGrowingWriteRejected(growth, magnitudeUnknown, operation);
    }

    private bool IsLocal(string cacheName, string key) => string.Equals(_ring.GetOwner(ServerCacheName.NormalizeUnvalidated(cacheName), key), _self, StringComparison.Ordinal);
}
