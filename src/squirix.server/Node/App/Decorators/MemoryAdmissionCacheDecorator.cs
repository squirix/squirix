using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Applies memory admission checks before delegating to the inner pipeline on local-owner write paths.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
/// <remarks>The decorator only decides admission. The bytes of every entry the node holds are recorded by the local cache itself.</remarks>
[Immutable]
internal sealed class MemoryAdmissionCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ICacheEntrySizeEstimator<T> _estimator;
    private readonly IMemoryPressureGate _gate;
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly Func<string, string, string, bool>? _hasRecordedOutcome;
    private readonly Func<string, string, CancellationToken, ValueTask<NodeCacheEntry<T>?>>? _readFenced;
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
    /// Reads the entry as <paramref name="readStored" /> does, after a fence that proves the read current, or refuses with an exception;
    /// <see langword="null" /> when <paramref name="readStored" /> is authoritative. A conditional write that admission would answer
    /// without a log record is answered from this read, so a replicated pipeline never answers from a copy it cannot prove current.
    /// The fence may itself apply and record the outcome of a retried write, so the recorded outcome is checked again after it.
    /// </param>
    internal MemoryAdmissionCacheDecorator(
        ILogicalNamespacedCache<T> inner,
        IMemoryPressureGate gate,
        ICacheEntrySizeEstimator<T> estimator,
        Func<string, string, string, bool>? hasRecordedOutcome = null,
        Func<string, string, CancellationToken, ValueTask<NodeCacheEntry<T>?>>? readStored = null,
        Func<string, string, CancellationToken, ValueTask<NodeCacheEntry<T>?>>? readFenced = null)
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
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetValueAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.RemoveAsync(operationId, cacheName, key, cancellationToken);

    public async ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        var keyValue = new CacheKey(cacheName, key);
        var existing = await _readStored(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing?.ExpiresUtc != null)
            Admit(keyValue, existing, CreateExpirationMetadataReplacement(existing, false), AdmissionOperations.Set, operationId);

        return await _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var keyValue = new CacheKey(cacheName, key);
        var existing = await _readStored(cacheName, key, cancellationToken).ConfigureAwait(false);
        Admit(keyValue, existing, entry, AdmissionOperations.Set, operationId);
        await _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        var keyValue = new CacheKey(cacheName, key);
        var existing = await _readStored(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing != null)
            Admit(keyValue, existing, CreateExpirationMetadataReplacement(existing, true), AdmissionOperations.Set, operationId);

        return await _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var keyValue = new CacheKey(cacheName, key);
        var existing = await _readStored(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing != null)
        {
            if (IsRecorded(cacheName, key, operationId))
                return await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);

            existing = await ConfirmAsync(existing, cacheName, key, cancellationToken).ConfigureAwait(false);
            if (existing != null)
                return IsRecorded(cacheName, key, operationId) && await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
        }

        Admit(keyValue, null, entry, AdmissionOperations.TryAdd, operationId);
        return await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        var keyValue = new CacheKey(cacheName, key);
        var existing = await _readStored(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing == null)
        {
            if (IsRecorded(cacheName, key, operationId))
                return await _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken).ConfigureAwait(false);

            existing = await ConfirmAsync(null, cacheName, key, cancellationToken).ConfigureAwait(false);
            if (existing == null)
                return IsRecorded(cacheName, key, operationId) && await _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken).ConfigureAwait(false);
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
    /// restart, every write under pressure counts as recorded and passes; the first start closes that short window.
    /// </remarks>
    /// <exception cref="Squirix.Server.Errors.ResourceExhaustedException">Pressure refuses the write and no outcome is recorded for it.</exception>
    private void Admit(CacheKey key, NodeCacheEntry<T>? existing, NodeCacheEntry<T> proposed, string operation, string operationId)
    {
        var growth = MemoryAdmissionJournalExtensions.ComputeNetGrowthForReplace(key, existing, false, proposed, false, _estimator, out var magnitudeUnknown);
        if (_gate.RejectsMemoryGrowingWrite(growth, magnitudeUnknown) && !IsRecorded(key.Namespace, key.Key, operationId))
            _gate.ThrowIfMemoryGrowingWriteRejected(growth, magnitudeUnknown, operation);
    }

    private ValueTask<NodeCacheEntry<T>?> ConfirmAsync(NodeCacheEntry<T>? stored, string cacheName, string key, CancellationToken cancellationToken) =>
        _readFenced == null ? ValueTask.FromResult(stored) : _readFenced(cacheName, key, cancellationToken);

    private bool IsRecorded(string cacheName, string key, string operationId) => _hasRecordedOutcome?.Invoke(cacheName, key, operationId) == true;
}
