using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Applies memory admission checks before delegating to the inner pipeline on local-owner write paths.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
/// <remarks>The decorator only decides admission. The bytes of every entry the node holds are recorded by the local cache itself.</remarks>
[Immutable]
internal sealed class MemoryAdmissionCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ICacheEntrySizeEstimator<T> _estimator;
    private readonly PressureGate _gate;
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly Func<string, string, bool>? _hasMemberPressure;
    private readonly Func<string, string, string, bool>? _hasRecordedOutcome;
    private readonly Func<string, string, CancellationToken, ValueTask<NodeCacheEntryPeek<T>>>? _readFenced;
    private readonly Func<string, string, CancellationToken, ValueTask<NodeCacheEntry<T>?>> _readStored;

    /// <summary>Initializes a new instance of the <see cref="MemoryAdmissionCacheDecorator{T}" /> class.</summary>
    /// <param name="inner">The cache pipeline below admission.</param>
    /// <param name="gate">The memory pressure gate.</param>
    /// <param name="estimator">The entry size estimator.</param>
    /// <param name="hasRecordedOutcome">
    /// Tells, by cache name, key and operation id, whether <paramref name="inner" /> holds a recorded outcome for the operation in the
    /// replica group that owns the key, as the replicated committer does; <see langword="null" /> when it records none. A conditional write that admission would refuse from the
    /// current state is handed to <paramref name="inner" /> when its outcome is recorded, so a retry of a committed write replays it.
    /// Any other such write is refused here, without a log record: its first attempt changed nothing, so nothing needs replaying.
    /// Until the replicated committer has rebuilt its outcomes after a restart, every such write counts as recorded and is decided there.
    /// </param>
    /// <param name="readStored">
    /// Reads the entry <paramref name="inner" /> holds for admission, an entry past its deadline counting as absent, without
    /// any side effect; <see langword="null" /> reads through <paramref name="inner" />. A replicated pipeline passes a read that never
    /// starts an expiry commit: its committer folds the expiry of the key into the write's own decision.
    /// </param>
    /// <param name="readFenced">
    /// Reads the entry as <paramref name="readStored" /> does, after a fence that proves the read current, or refuses with an exception,
    /// and tells in the same read whether an entry past its deadline still waits for its committed expiry; <see langword="null" /> when
    /// <paramref name="readStored" /> is authoritative and <paramref name="inner" /> never keeps an entry past its deadline (a single-copy
    /// host). A conditional write that admission would answer without a log record is answered from this read, so a replicated pipeline
    /// never answers from a copy it cannot prove current. The fence may itself apply and record the outcome of a retried write, so the
    /// recorded outcome is checked again after it. An update of a key whose expiry is pending is handed to <paramref name="inner" />, which
    /// commits the expiry before it answers. That write skips admission: the committer deletes the expired entry, and a concurrent write
    /// that makes the key live grows memory by at most this write.
    /// </param>
    /// <param name="hasMemberPressure">
    /// Tells, by cache name and key, whether a member of the replica group that owns the key recently reported critical memory pressure, so the
    /// group must not grow; <see langword="null" /> when the host replicates nothing. A memory-growing write is refused while it holds,
    /// unless its outcome is recorded.
    /// </param>
    internal MemoryAdmissionCacheDecorator(
        ILogicalNamespacedCache<T> inner,
        PressureGate gate,
        ICacheEntrySizeEstimator<T> estimator,
        Func<string, string, string, bool>? hasRecordedOutcome = null,
        Func<string, string, CancellationToken, ValueTask<NodeCacheEntry<T>?>>? readStored = null,
        Func<string, string, CancellationToken, ValueTask<NodeCacheEntryPeek<T>>>? readFenced = null,
        Func<string, string, bool>? hasMemberPressure = null)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(estimator);
        _inner = inner;
        _gate = gate;
        _estimator = estimator;
        _hasRecordedOutcome = hasRecordedOutcome;
        _readStored = readStored ?? inner.GetEntryAsync;
        _readFenced = readFenced;
        _hasMemberPressure = hasMemberPressure;
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetValueAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.RemoveAsync(operationId, cacheName, key, cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        return _estimator is ObjectCacheEntrySizeEstimator
            ? _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken)
            : RemoveExpirationWithEstimateAsync(operationId, cacheName, key, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var keyValue = new CacheKey(cacheName, key);
        if (!CanSetBeRefused(keyValue, entry))
        {
            await _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
            return;
        }

        var existing = await _readStored(cacheName, key, cancellationToken).ConfigureAwait(false);
        Admit(keyValue, existing, entry, AdmissionOperations.Set, operationId);
        await _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        var keyValue = new CacheKey(cacheName, key);
        var existing = await _readStored(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing == null)
            return await _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken).ConfigureAwait(false);
        if (_estimator is CacheEntrySizeEstimator<T> or ObjectCacheEntrySizeEstimator)
            AdmitGrowth(keyValue, CacheEntrySizeEstimator<T>.DeadlineIndexGrowthOf(existing), _estimator.HasUnknownPayloadMagnitude(existing, false), AdmissionOperations.Set, operationId);
        else
            Admit(keyValue, existing, CreateExpirationMetadataReplacement(existing, true), AdmissionOperations.Set, operationId);

        return await _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken).ConfigureAwait(false);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var keyValue = new CacheKey(cacheName, key);
        var existing = await _readStored(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing != null)
        {
            if (IsRecorded(cacheName, key, operationId))
                return await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);

            existing = (await ConfirmAsync(existing, cacheName, key, cancellationToken).ConfigureAwait(false)).Live;
            if (existing != null)
                return IsRecorded(cacheName, key, operationId) && await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
        }

        Admit(keyValue, null, entry, AdmissionOperations.TryAdd, operationId);
        return await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<NodeCacheAddOrGetResult<T>> AddOrGetAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        // Keep the recorded-outcome checks, fenced read and admission decision aligned with TryAddEntryAsync.
        // This decision path is duplicated; consider sharing it when the admission flow is refactored.
        var existing = await _readStored(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing != null)
        {
            if (IsRecorded(cacheName, key, operationId))
                return await AddOrReadInnerAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);

            existing = (await ConfirmAsync(existing, cacheName, key, cancellationToken).ConfigureAwait(false)).Live;
            if (existing != null)
            {
                if (IsRecorded(cacheName, key, operationId))
                    return await AddOrReadInnerAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);

                cancellationToken.ThrowIfCancellationRequested();
                return new NodeCacheAddOrGetResult<T>(false, new NodeCacheValueResult<T>(true, existing.Value));
            }
        }

        Admit(new CacheKey(cacheName, key), null, entry, AdmissionOperations.TryAdd, operationId);
        return await AddOrReadInnerAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        var keyValue = new CacheKey(cacheName, key);
        var existing = await _readStored(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing == null)
        {
            if (IsRecorded(cacheName, key, operationId))
                return await _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken).ConfigureAwait(false);

            var confirmed = await ConfirmAsync(null, cacheName, key, cancellationToken).ConfigureAwait(false);
            existing = confirmed.Live;
            if (existing == null)
            {
                // An expired entry exists until its expiry commits: the inner pipeline commits the expiry before answering, as a read does.
                return (confirmed.ExpiryPending || IsRecorded(cacheName, key, operationId))
                    && await _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken).ConfigureAwait(false);
            }
        }

        var replacement = new NodeCacheEntry<T>(value, existing.Version, existing.ExpiresUtc, existing.Expiration, existing.Tags);
        Admit(keyValue, existing, replacement, AdmissionOperations.Set, operationId);
        return await _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken).ConfigureAwait(false);
    }

    private static NodeCacheEntry<T> CreateExpirationMetadataReplacement(NodeCacheEntry<T> existing, bool hasExpirationUtc) => new(
        existing.Value,
        existing.Version,
        hasExpirationUtc ? existing.ExpiresUtc ?? DateTime.UnixEpoch : null,
        existing.Expiration,
        existing.Tags);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> RemoveExpirationWithEstimateAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        var keyValue = new CacheKey(cacheName, key);
        var existing = await _readStored(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing?.ExpiresUtc != null)
            Admit(keyValue, existing, CreateExpirationMetadataReplacement(existing, false), AdmissionOperations.Set, operationId);

        return await _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<NodeCacheAddOrGetResult<T>> AddOrReadInnerAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        return await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false)
            ? new NodeCacheAddOrGetResult<T>(true, default)
            : new NodeCacheAddOrGetResult<T>(false, await _inner.GetValueAsync(cacheName, key, cancellationToken).ConfigureAwait(false));
    }

    /// <summary>Refuses a memory-growing write, unless it is a retry of an operation whose outcome the inner pipeline recorded.</summary>
    /// <param name="key">The written key.</param>
    /// <param name="existing">The entry the key holds now, or <see langword="null" />.</param>
    /// <param name="proposed">The entry the write would store.</param>
    /// <param name="operation">The admission operation label.</param>
    /// <param name="operationId">The client operation identifier.</param>
    /// <remarks>
    /// A write that pressure would refuse but whose outcome is recorded passes: the inner pipeline replays the outcome and grows nothing.
    /// The recorded outcome is looked up only when the write would be refused. Should the outcome age out between this lookup and the
    /// replay, the write runs once more past admission: a single write. Until the committer has rebuilt its outcomes after a
    /// restart, every write under pressure counts as recorded and passes; the first start closes that short window. After the own gate,
    /// a write that grows memory is refused the same way while a member of the owning group reports critical pressure: every copy holds
    /// the same entries, so the group holds what its smallest member can.
    /// </remarks>
    /// <exception cref="Squirix.Server.Errors.ResourceExhaustedException">Pressure on this node or on a member of the owning group refuses the write and no outcome is recorded for it.</exception>
    private void Admit(CacheKey key, NodeCacheEntry<T>? existing, NodeCacheEntry<T> proposed, string operation, string operationId)
    {
        var growth = MemoryAdmissionJournalExtensions.ComputeNetGrowthForReplace(key, existing, false, proposed, false, _estimator, out var magnitudeUnknown);
        AdmitGrowth(key, growth, magnitudeUnknown, operation, operationId);
    }

    private void AdmitGrowth(CacheKey key, long growth, bool magnitudeUnknown, string operation, string operationId)
    {
        if (_gate.RejectsMemoryGrowingWrite(growth, magnitudeUnknown))
        {
            if (!IsRecorded(key.Namespace, key.Key, operationId))
                _gate.ThrowIfMemoryGrowingWriteRejected(growth, magnitudeUnknown, operation);

            return;
        }

        if ((magnitudeUnknown || growth > 0) && _hasMemberPressure?.Invoke(key.Namespace, key.Key) == true && !IsRecorded(key.Namespace, key.Key, operationId))
            _gate.ThrowReplicaPressureRejection(operation);
    }

    /// <summary>Tells whether admission could refuse a Set of <paramref name="proposed" />, so the stored entry must be read to size its net growth.</summary>
    /// <param name="key">The written key.</param>
    /// <param name="proposed">The entry the write would store.</param>
    /// <returns><see langword="false" /> when the write is admitted whatever entry the key holds.</returns>
    /// <remarks>
    /// The growth of a replacement never exceeds the full size of the proposed entry, which is the growth when the key holds nothing. The gate
    /// is monotonic in growth, so a write whose full size it admits is admitted at any smaller growth, and no member pressure is reported
    /// either. A proposed entry of unknown magnitude, or a gate that refuses the full size, takes the exact path with the stored entry. Pressure
    /// that rises between this check and the write is the race the exact path has too.
    /// </remarks>
    private bool CanSetBeRefused(CacheKey key, NodeCacheEntry<T> proposed)
    {
        var upperBound = MemoryAdmissionJournalExtensions.ComputeNetGrowthForReplace(key, null, false, proposed, false, _estimator, out var magnitudeUnknown);
        return magnitudeUnknown
               || upperBound <= 0
               || _gate.RejectsMemoryGrowingWrite(upperBound, false)
               || _hasMemberPressure?.Invoke(key.Namespace, key.Key) == true;
    }

    private ValueTask<NodeCacheEntryPeek<T>> ConfirmAsync(NodeCacheEntry<T>? stored, string cacheName, string key, CancellationToken cancellationToken) =>
        _readFenced?.Invoke(cacheName, key, cancellationToken) ?? ValueTask.FromResult(new NodeCacheEntryPeek<T>(stored, false));

    private bool IsRecorded(string cacheName, string key, string operationId) => _hasRecordedOutcome?.Invoke(cacheName, key, operationId) == true;
}
