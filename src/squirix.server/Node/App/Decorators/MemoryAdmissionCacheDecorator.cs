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
using Squirix.Server.Threading;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Applies memory admission checks before delegating to the inner pipeline on local-owner write paths.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Mutable]
internal sealed class MemoryAdmissionCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private const int KeyGateCount = 256;

    private readonly ConcurrentDictionary<CacheKey, long> _accountedEntryBytes = new();
    private readonly IMemoryUsageAccounting _accounting;
    private readonly ICacheEntrySizeEstimator<T> _estimator;
    private readonly IMemoryPressureGate _gate;
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly AsyncLock[] _keyGates = CreateKeyGates();
    private readonly bool _innerRecordsOutcomes;
    private readonly INodeLocator _ring;
    private readonly string _self;

    /// <summary>Initializes a new instance of the <see cref="MemoryAdmissionCacheDecorator{T}" /> class.</summary>
    /// <param name="inner">The cache pipeline below admission.</param>
    /// <param name="gate">The memory pressure gate.</param>
    /// <param name="estimator">The entry size estimator.</param>
    /// <param name="accounting">The memory usage accounting.</param>
    /// <param name="ring">The ownership ring.</param>
    /// <param name="self">This node identifier.</param>
    /// <param name="innerRecordsOutcomes">
    /// Whether <paramref name="inner" /> records the outcome of every conditional write by operation id, as the replicated committer
    /// does. A conditional write that admission could answer without writing is then still handed to it: a retry of a committed write
    /// replays its outcome there, and a refused write is recorded so its retry gets the same answer.
    /// </param>
    internal MemoryAdmissionCacheDecorator(
        ILogicalNamespacedCache<T> inner,
        IMemoryPressureGate gate,
        ICacheEntrySizeEstimator<T> estimator,
        IMemoryUsageAccounting accounting,
        INodeLocator ring,
        string self,
        bool innerRecordsOutcomes = false)
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
        _innerRecordsOutcomes = innerRecordsOutcomes;
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
        using var keyGuard = await LockKeyAsync(keyValue, cancellationToken).ConfigureAwait(false);
        var result = await _inner.RemoveAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);
        if (result.Removed)
            AccountRemove(keyValue);

        return result;
    }

    public async ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);

        var keyValue = new CacheKey(cacheName, key);
        using var keyGuard = await LockKeyAsync(keyValue, cancellationToken).ConfigureAwait(false);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing?.ExpiresUtc == null)
            return await _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);

        var replacement = CreateExpirationMetadataReplacement(existing, false);
        AdmitReplaceOrInsert(keyValue, existing, replacement, AdmissionOperations.Set);
        var removed = await _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);
        if (removed)
            AccountReplaceOrInsert(keyValue, replacement);

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
        using var keyGuard = await LockKeyAsync(keyValue, cancellationToken).ConfigureAwait(false);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        AdmitReplaceOrInsert(keyValue, existing, entry, AdmissionOperations.Set);

        // A pipeline that records outcomes decides the upsert itself; an add here would record a retry of this set under another kind.
        if (existing == null && !_innerRecordsOutcomes)
        {
            if (await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false))
            {
                AccountReplaceOrInsert(keyValue, entry);
                return;
            }

            await _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
            AccountReplaceOrInsert(keyValue, entry);
            return;
        }

        await _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
        AccountReplaceOrInsert(keyValue, entry);
    }

    public async ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken).ConfigureAwait(false);

        var keyValue = new CacheKey(cacheName, key);
        using var keyGuard = await LockKeyAsync(keyValue, cancellationToken).ConfigureAwait(false);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing == null)
            return await _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken).ConfigureAwait(false);

        var replacement = CreateExpirationMetadataReplacement(existing, true);
        AdmitReplaceOrInsert(keyValue, existing, replacement, AdmissionOperations.Set);
        var touched = await _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken).ConfigureAwait(false);
        if (touched)
            AccountReplaceOrInsert(keyValue, replacement);

        return touched;
    }

    public async ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);

        var keyValue = new CacheKey(cacheName, key);
        using var keyGuard = await LockKeyAsync(keyValue, cancellationToken).ConfigureAwait(false);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing != null)
        {
            if (!_innerRecordsOutcomes)
                return false;

            // An entry that expires before the inner pipeline decides lets the add insert: admit that replacement up front.
            if (existing.ExpiresUtc != null)
                AdmitReplaceOrInsert(keyValue, existing, entry, AdmissionOperations.TryAdd);

            return await AccountAnsweredAsync(keyValue, _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken)).ConfigureAwait(false);
        }

        AdmitReplaceOrInsert(keyValue, null, entry, AdmissionOperations.TryAdd);
        if (!await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false))
            return false;

        AccountReplaceOrInsert(keyValue, entry);
        return true;
    }

    public async ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        if (!IsLocal(cacheName, key))
            return await _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken).ConfigureAwait(false);

        var keyValue = new CacheKey(cacheName, key);
        using var keyGuard = await LockKeyAsync(keyValue, cancellationToken).ConfigureAwait(false);
        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing == null)
            return _innerRecordsOutcomes && await AccountAnsweredAsync(keyValue, _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken)).ConfigureAwait(false);

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

        AccountReplaceOrInsert(keyValue, replacement);
        return updated;
    }

    private static AsyncLock[] CreateKeyGates()
    {
        var gates = new AsyncLock[KeyGateCount];
        for (var i = 0; i < gates.Length; i++)
            gates[i] = new AsyncLock();

        return gates;
    }

    private static NodeCacheEntry<T> CreateExpirationMetadataReplacement(NodeCacheEntry<T> existing, bool hasExpirationUtc) => new(
        existing.Value,
        existing.Version,
        hasExpirationUtc ? existing.ExpiresUtc ?? DateTime.UnixEpoch : null,
        existing.Expiration,
        existing.Tags);

    private void AccountRemove(CacheKey key)
    {
        if (_accountedEntryBytes.TryRemove(key, out var accountedBytes))
            _accounting.RemoveEntry(accountedBytes);
    }

    /// <summary>Accounts an entry that was written to the inner cache, whether it inserted the key or replaced its value.</summary>
    /// <param name="key">The written key.</param>
    /// <param name="replacement">The entry now stored under <paramref name="key" />.</param>
    /// <remarks>
    /// The key is claimed in the accounting map before it is counted, so writers racing on one key (an insert that won the physical add and a
    /// set that lost it) count the entry once: the one that claims the key adds it, the others replace its size.
    /// </remarks>
    private void AccountReplaceOrInsert(CacheKey key, NodeCacheEntry<T> replacement)
    {
        var newBytes = _estimator.EstimateBytes(key, replacement, false);
        while (true)
        {
            if (_accountedEntryBytes.TryGetValue(key, out var accountedBytes))
            {
                if (accountedBytes == newBytes)
                    return;

                if (!_accountedEntryBytes.TryUpdate(key, newBytes, accountedBytes))
                    continue;

                _accounting.ReplaceEntry(accountedBytes, newBytes);
                return;
            }

            if (!_accountedEntryBytes.TryAdd(key, newBytes))
                continue;
            _accounting.AddEntry(newBytes);
            return;
        }
    }

    /// <summary>Awaits a conditional write that admission expected to change nothing, and accounts the entry the inner cache holds when it applied.</summary>
    /// <param name="key">The written key, whose write gate the caller holds.</param>
    /// <param name="write">The conditional write handed to the inner pipeline.</param>
    /// <returns>The outcome of the write.</returns>
    /// <remarks>
    /// A replayed outcome writes nothing; a fresh decision may write only when an entry with a deadline expired meanwhile, which the
    /// caller admitted. A <see langword="true" /> answer re-reads the key, uncancelled since the write already happened, so the accounted
    /// size stays the size of what the inner cache holds.
    /// </remarks>
    private async ValueTask<bool> AccountAnsweredAsync(CacheKey key, ValueTask<bool> write)
    {
        if (!await write.ConfigureAwait(false))
            return false;

        if (await _inner.GetEntryAsync(key.Namespace, key.Key, CancellationToken.None).ConfigureAwait(false) is { } current)
            AccountReplaceOrInsert(key, current);
        return true;
    }

    private void AdmitReplaceOrInsert(CacheKey key, NodeCacheEntry<T>? existing, NodeCacheEntry<T> proposed, string operation)
    {
        var growth = MemoryAdmissionJournalExtensions.ComputeNetGrowthForReplace(key, existing, false, proposed, false, _estimator, out var magnitudeUnknown);
        _gate.ThrowIfMemoryGrowingWriteRejected(growth, magnitudeUnknown, operation);
    }

    /// <summary>Takes the write gate of the key's stripe, which every local mutation holds from its inner write until its accounting update.</summary>
    /// <param name="key">The key about to be written.</param>
    /// <param name="cancellationToken">Cancels the wait for the gate.</param>
    /// <returns>The holder that releases the gate when disposed.</returns>
    /// <remarks>
    /// Writes to one key take effect in the inner cache and in the accounting map in the same order, so the accounted size is the size of the entry the
    /// inner cache holds. Keys share a fixed number of gates: unrelated keys on one gate wait for each other, which bounds the memory of the gates.
    /// </remarks>
    private ValueTask<AsyncLockHolder> LockKeyAsync(CacheKey key, CancellationToken cancellationToken) =>
        _keyGates[(key.GetHashCode() & int.MaxValue) % _keyGates.Length].LockAsync(cancellationToken);

    private bool IsLocal(string cacheName, string key) => string.Equals(_ring.GetOwner(ServerCacheName.NormalizeUnvalidated(cacheName), key), _self, StringComparison.Ordinal);
}
