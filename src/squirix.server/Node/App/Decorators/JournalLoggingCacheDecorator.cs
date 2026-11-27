using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Appends journal records for local-owner core mutations.</summary>
/// <remarks>
/// A touch, an expiration removal and an update journal a put of the entry they decided, read once under the mutation gate. When a raw
/// reader is supplied the decision read leaves eviction order alone, so the memory apply is the only access that counts.
/// </remarks>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed partial class JournalLoggingCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly PhysicalCache<T>? _capacityCache;
    private readonly DurableMutationExecutor _executor;
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly IJournalCoordinator _journal;
    private readonly ILocalCacheRawReader<T> _rawReader;
    private readonly TimeProvider _timeProvider;

    internal JournalLoggingCacheDecorator(
        ILogicalNamespacedCache<T> inner,
        IJournalCoordinator journal,
        DurableMutationExecutor durableMutations,
        TimeProvider timeProvider,
        ILocalCacheRawReader<T> rawReader,
        PhysicalCache<T>? capacityCache = null)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(journal);
        ArgumentNullException.ThrowIfNull(durableMutations);
        ArgumentNullException.ThrowIfNull(rawReader);
        _inner = inner;
        _journal = journal;
        _executor = durableMutations;
        _timeProvider = timeProvider;
        _rawReader = rawReader;
        _capacityCache = capacityCache;
        DistributedAccess = new DistributedAccessOperations(this);
    }

    private DateTime UtcNow => _timeProvider.GetUtcNow().UtcDateTime;

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetValueAsync(cacheName, key, cancellationToken);

    public ValueTask<NodeCacheValueRead<T>> GetValueWithDeadlineAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetValueWithDeadlineAsync(cacheName, key, cancellationToken);

    public ValueTask<bool> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        var cacheKey = new CacheKey(cacheName, key);
        return _executor.ExecuteAsync(
            cacheKey,
            static (_, _) => ValueTask.FromResult(DurableMutationCondition<bool>.Apply()),
            new DurableMutationPipeline<(JournalLoggingCacheDecorator<T> Self, RemoveJournalArgs Journal, RemoveMemoryArgs Memory), bool>(
                (this, new RemoveJournalArgs(cacheKey), new RemoveMemoryArgs(operationId, cacheName, key)),
                static (s, ownership, ct) => s.Self._journal.AppendRemoveAsync(ownership, s.Journal.CacheKey, ct),
                static (s, ct) => s.Self._inner.RemoveAsync(s.Memory.OperationId, s.Memory.CacheName, s.Memory.Key, ct)),
            cancellationToken);
    }

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => ExecuteDecidedUpsertAsync(
        operationId,
        cacheName,
        key,
        default(NoDecisionArgs),
        static (current, _, _) => current.ExpiresUtc == null ? null : new NodeCacheEntry<T>(current.Value, current.Version, tags: current.Tags),
        null,
        cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var durable = ResolveExpiration(entry);
        var prepared = JournalEntryPayload.PrepareEncode(durable);
        return SetEntryWithPreparedPayloadAsync(operationId, cacheName, key, durable, prepared, cancellationToken);
    }

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => ExecuteDecidedUpsertAsync(
        operationId,
        cacheName,
        key,
        expiration,
        static (current, now, ttl) => new NodeCacheEntry<T>(
            current.Value,
            current.Version,
            JournalEntryExpirationMaterializer.PinToJournalPrecision(now.SaturatedAdd(ttl)),
            tags: current.Tags),
        null,
        cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var durable = ResolveExpiration(entry);
        var prepared = JournalEntryPayload.PrepareEncode(durable);
        return TryAddEntryWithPreparedPayloadAsync(operationId, cacheName, key, durable, prepared, cancellationToken);
    }

    /// <inheritdoc />
    /// <remarks>The new value is validated first, so an oversized or unserializable value is rejected even when the key is absent.</remarks>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        // Normalize and validate before the mutation gate. Immutable JSON is already serialized; other values are snapshotted here.
        using var prepared = JournalEntryPayload.PrepareValue(value);
        return await ExecuteDecidedUpsertAsync(
            operationId,
            cacheName,
            key,
            value,
            static (current, _, replacement) => new NodeCacheEntry<T>(
                replacement,
                current.Version,
                JournalEntryExpirationMaterializer.PinToJournalPrecision(current.ExpiresUtc),
                tags: current.Tags),
            prepared,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Fixes a relative expiration to an absolute deadline on this decorator's clock. The journal frame and the
    /// in-memory apply must both receive the resolved entry, so replay restores the exact deadline memory holds.
    /// </summary>
    /// <param name="entry">The entry to write.</param>
    /// <returns>The entry with only an absolute deadline.</returns>
    internal NodeCacheEntry<T> ResolveExpiration(NodeCacheEntry<T> entry) => JournalEntryExpirationMaterializer.ForDurableWrite(entry, UtcNow);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask SetEntryWithPreparedPayloadAsync(
        string operationId,
        string cacheName,
        string key,
        NodeCacheEntry<T> entry,
        PreparedJournalEntryBase prepared,
        CancellationToken cancellationToken)
    {
        using var payload = JournalEntryPayload.Encode(in prepared);
        var cacheKey = new CacheKey(cacheName, key);
        _ = await _executor.ExecuteAsync(
            cacheKey,
            static (_, _) => ValueTask.FromResult(DurableMutationCondition<bool>.Apply()),
            new DurableMutationPipeline<(JournalLoggingCacheDecorator<T> Self, PutJournalArgs Journal, SetMemoryArgs Memory), bool>(
                (this, new PutJournalArgs(cacheKey, payload.Memory), new SetMemoryArgs(operationId, cacheName, key, entry, payload.Memory.Length, _capacityCache == null ? null : new CapacityDecision())),
                static (s, ownership, ct) => s.Self.AppendPutAsync(in ownership, s.Journal.CacheKey, s.Journal.Payload, s.Memory.Capacity, ct),
                static (s, ct) => s.Self.ApplySetEntryAsync(s.Memory, ct)),
            cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask<DurableMutationCondition<bool>> EvaluateTryAddPreconditionAsync(
        JournalLoggingCacheDecorator<T> self,
        TryAddMutationArgs args,
        CancellationToken cancellationToken)
    {
        var existing = await self._inner.GetValueAsync(args.CacheName, args.Key, cancellationToken).ConfigureAwait(false);
        return existing.Found ? DurableMutationCondition<bool>.Skip(false) : DurableMutationCondition<bool>.Apply();
    }

    private static async ValueTask<bool> ApplyDecidedAsync<TArgs>(DecidedUpsertState<TArgs> state, CancellationToken cancellationToken)
    {
        var entry = state.Upsert.Entry ?? ThrowHelper.Throw<NodeCacheEntry<T>>(new InvalidOperationException("The decided entry was not prepared."));
        await state.Self._inner.SetEntryAsync(state.OperationId, state.CacheName, state.Key, entry, state.Upsert.Payload.Length, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private static async ValueTask<DurableMutationCondition<bool>> EvaluateDecidedUpsertAsync<TArgs>(
        DecidedUpsertState<TArgs> state,
        CancellationToken cancellationToken)
    {
        var current = await state.Self.ReadCurrentAsync(state.CacheName, state.Key, cancellationToken).ConfigureAwait(false);
        if (current == null)
            return DurableMutationCondition<bool>.Skip(false);

        var decided = state.Decide(current, state.Self.UtcNow, state.Args);
        if (decided == null)
            return DurableMutationCondition<bool>.Skip(false);

        state.Upsert.Set(decided, state.PreparedValue is { } value ? JournalEntryPayload.EncodeWithPreparedValue(decided, value) : EncodeWhole(decided));
        return DurableMutationCondition<bool>.Apply();
    }

    private static PooledJournalPayload EncodeWhole(NodeCacheEntry<T> decided)
    {
        var prepared = JournalEntryPayload.PrepareEncode(decided);
        return JournalEntryPayload.Encode(in prepared);
    }

    private ValueTask<bool> TryAddEntryWithPreparedPayloadAsync(
        string operationId,
        string cacheName,
        string key,
        NodeCacheEntry<T> entry,
        PreparedJournalEntryBase prepared,
        CancellationToken cancellationToken)
    {
        var cacheKey = new CacheKey(cacheName, key);
        var args = new TryAddMutationArgs(operationId, cacheName, key, entry, prepared, cacheKey, _capacityCache == null ? null : new CapacityDecision());
        return _executor.ExecuteAsync(
            cacheKey,
            static (s, ct) => EvaluateTryAddPreconditionAsync(s.Self, s.Args, ct),
            new DurableMutationPipeline<(JournalLoggingCacheDecorator<T> Self, TryAddMutationArgs Args), bool>(
                (this, args),
                static async (s, ownership, ct) =>
                {
                    using var payload = JournalEntryPayload.Encode(s.Args.Prepared);
                    await s.Self.AppendPutAsync(in ownership, s.Args.CacheKey, payload.Memory, s.Args.Capacity, ct).ConfigureAwait(false);
                },
                static (s, ct) => s.Args.Capacity is { } capacity
                    ? ValueTask.FromResult(s.Self._capacityCache!.Upsert(s.Args.CacheKey, s.Args.Entry, s.Args.Prepared.EncodedLength, PhysicalCache<T>.WriteKind.JournaledTryAdd, ct, capacity.Victim))
                    : s.Self._inner.TryAddEntryAsync(s.Args.OperationId, s.Args.CacheName, s.Args.Key, s.Args.Entry, s.Args.Prepared.EncodedLength, ct),
                static (s, actual) =>
                {
                    // The precondition predicted an add under the key lock: an apply that refused it leaves a put frame the memory never took.
                    if (!actual)
                        s.Self._journal.FailJournalPipeline(new InvalidOperationException("an add predicted to take effect was refused by memory after its journal frame entered the ring."));
                }),
            cancellationToken);
    }

    private async ValueTask<bool> ApplySetEntryAsync(SetMemoryArgs args, CancellationToken cancellationToken)
    {
        if (args.Capacity is { } capacity)
            _ = _capacityCache!.Upsert(new CacheKey(args.CacheName, args.Key), args.Entry, args.EncodedLength, PhysicalCache<T>.WriteKind.JournaledSet, cancellationToken, capacity.Victim);
        else
            await _inner.SetEntryAsync(args.OperationId, args.CacheName, args.Key, args.Entry, args.EncodedLength, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private ValueTask AppendPutAsync(in JournalAppendContext ownership, CacheKey key, ReadOnlyMemory<byte> payload, CapacityDecision? capacity, CancellationToken cancellationToken)
    {
        if (capacity != null)
        {
            capacity.Victim = _capacityCache!.SelectCapacityVictim(key);
            if (capacity.Victim is { } victim)
                return _journal.AppendPutWithEvictionAsync(ownership, key, payload, victim.Key, cancellationToken);
        }

        return _journal.AppendPutAsync(ownership, key, payload, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> ExecuteDecidedUpsertAsync<TArgs>(
        string operationId,
        string cacheName,
        string key,
        TArgs args,
        Func<NodeCacheEntry<T>, DateTime, TArgs, NodeCacheEntry<T>?> decide,
        PreparedJournalValue? preparedValue,
        CancellationToken cancellationToken)
    {
        using var upsert = new DecidedUpsert();
        var state = new DecidedUpsertState<TArgs>(this, operationId, cacheName, key, args, decide, preparedValue, upsert);
        return await _executor.ExecuteAsync(
            new CacheKey(cacheName, key),
            static (s, ct) => EvaluateDecidedUpsertAsync(s, ct),
            new DurableMutationPipeline<DecidedUpsertState<TArgs>, bool>(
                state,
                static (s, ownership, ct) => s.Self._journal.AppendPutAsync(ownership, new CacheKey(s.CacheName, s.Key), s.Upsert.Payload, ct),
                static (s, ct) => ApplyDecidedAsync(s, ct)),
            cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<NodeCacheEntry<T>?> ReadCurrentAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        // The raw read leaves eviction order alone; liveness is judged on this decorator's clock, the one the decision uses.
        var raw = _rawReader.GetEntryRaw(new CacheKey(cacheName, key), cancellationToken);
        if (raw is not { ExpiresUtc: { } expiresUtc } || expiresUtc > UtcNow)
            return raw;

        // The dead node is still in memory because the sweep may not have removed it yet: a lazy-expiring read drops it, and order no longer matters for it.
        _ = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        return null;
    }

    [Immutable]
    private readonly record struct PutJournalArgs(CacheKey CacheKey, ReadOnlyMemory<byte> Payload);

    [Immutable]
    private readonly record struct RemoveJournalArgs(CacheKey CacheKey);

    [Immutable]
    private readonly record struct RemoveMemoryArgs(string OperationId, string CacheName, string Key);

    [Immutable]
    private readonly record struct SetMemoryArgs(string OperationId, string CacheName, string Key, NodeCacheEntry<T> Entry, int EncodedLength, CapacityDecision? Capacity);

    [Immutable]
    private readonly record struct TryAddMutationArgs(
        string OperationId,
        string CacheName,
        string Key,
        NodeCacheEntry<T> Entry,
        PreparedJournalEntryBase Prepared,
        CacheKey CacheKey,
        CapacityDecision? Capacity);

    [Immutable]
    private readonly record struct DecidedUpsertState<TArgs>(
        JournalLoggingCacheDecorator<T> Self,
        string OperationId,
        string CacheName,
        string Key,
        TArgs Args,
        Func<NodeCacheEntry<T>, DateTime, TArgs, NodeCacheEntry<T>?> Decide,
        PreparedJournalValue? PreparedValue,
        DecidedUpsert Upsert);

    /// <summary>Decision arguments for a mutation that needs none.</summary>
    [Immutable]
    private readonly record struct NoDecisionArgs;

    [Mutable]
    private sealed class CapacityDecision
    {
        internal CacheKey? Victim { get; set; }
    }

    /// <summary>Carries the entry a precondition decided and its encoded journal payload from the precondition to the append and apply stages.</summary>
    [Mutable]
    private sealed class DecidedUpsert : IDisposable
    {
        private PooledJournalPayload? _payload;

        internal NodeCacheEntry<T>? Entry { get; private set; }

        internal ReadOnlyMemory<byte> Payload => _payload?.Memory ?? ReadOnlyMemory<byte>.Empty;

        public void Dispose() => _payload?.Dispose();

        internal void Set(NodeCacheEntry<T> entry, PooledJournalPayload payload)
        {
            _payload?.Dispose();
            Entry = entry;
            _payload = payload;
        }
    }
}
