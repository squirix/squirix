using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
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
    private readonly Func<string, string, string, bool>? _hasRecordedOutcome;
    private readonly Func<string, string, CancellationToken, ValueTask<NodeCacheEntry<T>?>> _readStored;

    /// <summary>Initializes a new instance of the <see cref="MemoryAdmissionCacheDecorator{T}" /> class.</summary>
    /// <param name="inner">The cache pipeline below admission.</param>
    /// <param name="gate">The memory pressure gate.</param>
    /// <param name="estimator">The entry size estimator.</param>
    /// <param name="accounting">The memory usage accounting.</param>
    /// <param name="hasRecordedOutcome">
    /// Tells, by cache name, key and operation id, whether <paramref name="inner" /> holds a recorded outcome for the operation in the
    /// replica group that owns the key, as the replicated committer does; <see langword="null" /> when it records none. A conditional write that admission would refuse from the
    /// current state is handed to <paramref name="inner" /> when its outcome is recorded, so a retry of a committed write replays it.
    /// Any other such write is refused here, without a log record: its first attempt changed nothing, so nothing needs replaying.
    /// Until the replicated committer has rebuilt its outcomes after a restart, every such write counts as recorded and is decided there.
    /// </param>
    /// <param name="readStored">
    /// Reads the entry <paramref name="inner" /> holds for admission and accounting, an entry past its deadline counting as absent, without
    /// any side effect; <see langword="null" /> reads through <paramref name="inner" />. A replicated pipeline passes a read that never
    /// starts an expiry commit: its committer folds the expiry of the key into the write's own decision.
    /// </param>
    internal MemoryAdmissionCacheDecorator(
        ILogicalNamespacedCache<T> inner,
        IMemoryPressureGate gate,
        ICacheEntrySizeEstimator<T> estimator,
        IMemoryUsageAccounting accounting,
        Func<string, string, string, bool>? hasRecordedOutcome = null,
        Func<string, string, CancellationToken, ValueTask<NodeCacheEntry<T>?>>? readStored = null)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(estimator);
        ArgumentNullException.ThrowIfNull(accounting);
        _inner = inner;
        _gate = gate;
        _estimator = estimator;
        _accounting = accounting;
        _hasRecordedOutcome = hasRecordedOutcome;
        _readStored = readStored ?? inner.GetEntryAsync;
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetValueAsync(cacheName, key, cancellationToken);

    public async ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        var keyValue = new CacheKey(cacheName, key);
        using var keyGuard = await LockKeyAsync(keyValue, cancellationToken).ConfigureAwait(false);
        var result = await _inner.RemoveAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);
        if (!result.Removed)
            return result;

        // A replayed remove may answer for an attempt made before another write stored the key again.
        if (_hasRecordedOutcome == null)
            AccountRemove(keyValue);
        else
            await AccountStoredAsync(keyValue).ConfigureAwait(false);

        return result;
    }

    public async ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        var keyValue = new CacheKey(cacheName, key);
        using var keyGuard = await LockKeyAsync(keyValue, cancellationToken).ConfigureAwait(false);
        var existing = await _readStored(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing?.ExpiresUtc == null)
            return await _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);

        var replacement = CreateExpirationMetadataReplacement(existing, false);
        if (AdmitReplaceOrInsert(keyValue, existing, replacement, AdmissionOperations.Set, operationId))
            return await AccountAnsweredAsync(keyValue, _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken)).ConfigureAwait(false);

        var removed = await _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);
        if (removed)
            await AccountWrittenAsync(keyValue, replacement).ConfigureAwait(false);

        return removed;
    }

    public async ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var keyValue = new CacheKey(cacheName, key);
        using var keyGuard = await LockKeyAsync(keyValue, cancellationToken).ConfigureAwait(false);
        var existing = await _readStored(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (AdmitReplaceOrInsert(keyValue, existing, entry, AdmissionOperations.Set, operationId))
        {
            await _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
            await AccountStoredAsync(keyValue).ConfigureAwait(false);
            return;
        }

        // A pipeline that records outcomes decides the upsert itself; an add here would record a retry of this set under another kind.
        if (existing == null && _hasRecordedOutcome == null)
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
        await AccountWrittenAsync(keyValue, entry).ConfigureAwait(false);
    }

    public async ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        var keyValue = new CacheKey(cacheName, key);
        using var keyGuard = await LockKeyAsync(keyValue, cancellationToken).ConfigureAwait(false);
        var existing = await _readStored(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing == null)
            return await _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken).ConfigureAwait(false);

        var replacement = CreateExpirationMetadataReplacement(existing, true);
        if (AdmitReplaceOrInsert(keyValue, existing, replacement, AdmissionOperations.Set, operationId))
            return await AccountAnsweredAsync(keyValue, _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken)).ConfigureAwait(false);

        var touched = await _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken).ConfigureAwait(false);
        if (touched)
            await AccountWrittenAsync(keyValue, replacement).ConfigureAwait(false);

        return touched;
    }

    public async ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var keyValue = new CacheKey(cacheName, key);
        using var keyGuard = await LockKeyAsync(keyValue, cancellationToken).ConfigureAwait(false);
        var existing = await _readStored(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing != null)
        {
            var recorded = IsRecorded(cacheName, key, operationId);
            return recorded && await AccountAnsweredAsync(keyValue, _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken)).ConfigureAwait(false);
        }

        if (AdmitReplaceOrInsert(keyValue, null, entry, AdmissionOperations.TryAdd, operationId))
            return await AccountAnsweredAsync(keyValue, _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken)).ConfigureAwait(false);

        if (!await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false))
            return false;

        await AccountWrittenAsync(keyValue, entry).ConfigureAwait(false);
        return true;
    }

    public async ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        var keyValue = new CacheKey(cacheName, key);
        using var keyGuard = await LockKeyAsync(keyValue, cancellationToken).ConfigureAwait(false);
        var existing = await _readStored(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing == null)
        {
            var recorded = IsRecorded(cacheName, key, operationId);
            return recorded && await AccountAnsweredAsync(keyValue, _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken)).ConfigureAwait(false);
        }

        var replacement = new NodeCacheEntry<T>
        {
            Value = value,
            ExpiresUtc = existing.ExpiresUtc,
            Expiration = existing.Expiration,
            Version = existing.Version,
        };
        if (AdmitReplaceOrInsert(keyValue, existing, replacement, AdmissionOperations.Set, operationId))
            return await AccountAnsweredAsync(keyValue, _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken)).ConfigureAwait(false);

        var updated = await _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken).ConfigureAwait(false);
        if (!updated)
            return false;

        if (_hasRecordedOutcome != null)
            await AccountStoredAsync(keyValue).ConfigureAwait(false);
        else if (!EqualityComparer<T?>.Default.Equals(existing.Value, value))
            AccountReplaceOrInsert(keyValue, replacement);

        return true;
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
    /// A replayed outcome writes nothing; a fresh decision, should the recorded outcome age out first, may write only when an entry with
    /// a deadline expired meanwhile, which the caller admitted. A <see langword="true" /> answer re-reads the key, uncancelled since the
    /// write already happened, so the accounted size stays the size of what the inner cache holds.
    /// </remarks>
    private async ValueTask<bool> AccountAnsweredAsync(CacheKey key, ValueTask<bool> write)
    {
        if (!await write.ConfigureAwait(false))
            return false;

        await AccountStoredAsync(key).ConfigureAwait(false);
        return true;
    }

    /// <summary>Accounts what the inner cache holds under a key after a write that a recording pipeline may have answered by replay.</summary>
    /// <param name="key">The written key, whose write gate the caller holds.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <remarks>
    /// A replayed outcome writes nothing, so the key may hold what a later write stored, or nothing. The read is uncancelled since the
    /// write already happened; a key the read finds absent, removed meanwhile or dropped as expired by the read, leaves the accounting.
    /// </remarks>
    private async ValueTask AccountStoredAsync(CacheKey key)
    {
        var stored = await _readStored(key.Namespace, key.Key, CancellationToken.None).ConfigureAwait(false);
        if (stored == null)
            AccountRemove(key);
        else
            AccountReplaceOrInsert(key, stored);
    }

    /// <summary>Accounts a write the inner pipeline applied: the written entry on a single-copy host, the stored one on a recording pipeline.</summary>
    /// <param name="key">The written key, whose write gate the caller holds.</param>
    /// <param name="written">The entry the write stores when it is not answered by replay.</param>
    /// <returns>An asynchronous operation.</returns>
    private ValueTask AccountWrittenAsync(CacheKey key, NodeCacheEntry<T> written)
    {
        if (_hasRecordedOutcome != null)
            return AccountStoredAsync(key);

        AccountReplaceOrInsert(key, written);
        return ValueTask.CompletedTask;
    }

    /// <summary>Admits a memory-growing write, unless it is a retry of an operation whose outcome the inner pipeline recorded.</summary>
    /// <param name="key">The written key.</param>
    /// <param name="existing">The entry the key holds now, or <see langword="null" />.</param>
    /// <param name="proposed">The entry the write would store.</param>
    /// <param name="operation">The admission operation label.</param>
    /// <param name="operationId">The client operation identifier.</param>
    /// <returns>
    /// <see langword="true" /> when pressure would refuse the write but its outcome is recorded: the caller hands it to the inner pipeline,
    /// which replays the outcome and grows nothing, and accounts what the inner cache then holds. The recorded outcome is looked up only
    /// when the write would be refused. Should the outcome age out between this lookup and the replay, the write runs once more past
    /// admission: a single write, still accounted from what the inner cache holds. Until the committer has rebuilt its outcomes after a
    /// restart, every write under pressure counts as recorded and passes; the first start closes that short window.
    /// </returns>
    /// <exception cref="Squirix.Server.Errors.ResourceExhaustedException">Pressure refuses the write and no outcome is recorded for it.</exception>
    private bool AdmitReplaceOrInsert(CacheKey key, NodeCacheEntry<T>? existing, NodeCacheEntry<T> proposed, string operation, string operationId)
    {
        var growth = MemoryAdmissionJournalExtensions.ComputeNetGrowthForReplace(key, existing, false, proposed, false, _estimator, out var magnitudeUnknown);
        if (!_gate.RejectsMemoryGrowingWrite(growth, magnitudeUnknown))
            return false;

        if (IsRecorded(key.Namespace, key.Key, operationId))
            return true;

        _gate.ThrowIfMemoryGrowingWriteRejected(growth, magnitudeUnknown, operation);
        return false;
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

    private bool IsRecorded(string cacheName, string key, string operationId) => _hasRecordedOutcome?.Invoke(cacheName, key, operationId) == true;
}
