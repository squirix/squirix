using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Appends journal records for local-owner core mutations.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class JournalLoggingCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly DurableMutationExecutor _executor;
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly IJournalCoordinator _journal;
    private readonly INodeLocator _ring;
    private readonly string _self;
    private readonly TimeProvider _timeProvider;

    internal JournalLoggingCacheDecorator(
        string self,
        INodeLocator ring,
        ILogicalNamespacedCache<T> inner,
        IJournalCoordinator journal,
        DurableMutationExecutor durableMutations,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(self);
        ArgumentNullException.ThrowIfNull(ring);
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(journal);
        ArgumentNullException.ThrowIfNull(durableMutations);
        _self = self;
        _ring = ring;
        _inner = inner;
        _journal = journal;
        _executor = durableMutations;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    private DateTime UtcNow => _timeProvider.GetUtcNow().UtcDateTime;

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetValueAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return _inner.RemoveAsync(operationId, cacheName, key, cancellationToken);

        var cacheKey = new CacheKey(cacheName, key);
        return _executor.ExecuteAsync(
            cacheKey,
            static (_, _) => ValueTask.FromResult(DurableMutationCondition<CacheRemoveResult<T>>.Apply()),
            new DurableMutationPipeline<(JournalLoggingCacheDecorator<T> Self, RemoveJournalArgs Journal, RemoveMemoryArgs Memory), CacheRemoveResult<T>>(
                (this, new RemoveJournalArgs(cacheKey), new RemoveMemoryArgs(operationId, cacheName, key)),
                static (s, ownership, ct) => s.Self._journal.AppendRemoveAsync(ownership, s.Journal.CacheKey, ct),
                static (s, ct) => s.Self._inner.RemoveAsync(s.Memory.OperationId, s.Memory.CacheName, s.Memory.Key, ct)),
            cancellationToken);
    }

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        !IsLocalOwner(cacheName, key)
            ? _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken)
            : ExecuteDecidedUpsertAsync(
                operationId,
                cacheName,
                key,
                0,
                static (current, _, _) => current.ExpiresUtc == null ? null : new NodeCacheEntry<T>(current.Value, current.Version, tags: current.Tags),
                cancellationToken);

    public async ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
        {
            await _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
            return;
        }

        var durable = ResolveExpiration(entry);
        var prepared = JournalEntryPayload.PrepareEncode(durable);
        EntryPayloadSizeGuard.EnsureLengthWithinLimit(prepared.EncodedLength);
        await SetEntryWithPreparedPayloadAsync(operationId, cacheName, key, durable, prepared, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        !IsLocalOwner(cacheName, key)
            ? _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken)
            : ExecuteDecidedUpsertAsync(
                operationId,
                cacheName,
                key,
                expiration,
                static (current, now, ttl) => new NodeCacheEntry<T>(
                    current.Value,
                    current.Version,
                    JournalEntryExpirationMaterializer.PinToJournalPrecision(now.SaturatedAdd(ttl)),
                    tags: current.Tags),
                cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken);

        var durable = ResolveExpiration(entry);
        var prepared = JournalEntryPayload.PrepareEncode(durable);
        EntryPayloadSizeGuard.EnsureLengthWithinLimit(prepared.EncodedLength);
        return TryAddEntryWithPreparedPayloadAsync(operationId, cacheName, key, durable, prepared, cancellationToken);
    }

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        !IsLocalOwner(cacheName, key)
            ? _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken)
            : ExecuteDecidedUpsertAsync(
                operationId,
                cacheName,
                key,
                value,
                static (current, _, replacement) => new NodeCacheEntry<T>(replacement, current.Version, current.ExpiresUtc, tags: current.Tags),
                cancellationToken);

    /// <summary>
    /// Fixes a relative expiration to an absolute deadline on this decorator's clock. The journal frame and the
    /// in-memory apply must both receive the resolved entry, so replay restores the exact deadline memory holds.
    /// </summary>
    /// <param name="entry">The entry to write.</param>
    /// <returns>The entry with only an absolute deadline.</returns>
    internal NodeCacheEntry<T> ResolveExpiration(NodeCacheEntry<T> entry) => JournalEntryExpirationMaterializer.ForDurableWrite(entry, UtcNow);

    internal async ValueTask SetEntryWithPreparedPayloadAsync(
        string operationId,
        string cacheName,
        string key,
        NodeCacheEntry<T> entry,
        PreparedJournalEntry prepared,
        CancellationToken cancellationToken)
    {
        using var payload = JournalEntryPayload.Encode(in prepared);
        var cacheKey = new CacheKey(cacheName, key);
        _ = await _executor.ExecuteAsync(
            cacheKey,
            static (_, _) => ValueTask.FromResult(DurableMutationCondition<bool>.Apply()),
            new DurableMutationPipeline<(JournalLoggingCacheDecorator<T> Self, PutJournalArgs Journal, SetMemoryArgs Memory), bool>(
                (this, new PutJournalArgs(cacheKey, payload.Memory), new SetMemoryArgs(operationId, cacheName, key, entry)),
                static (s, ownership, ct) => s.Self._journal.AppendPutAsync(ownership, s.Journal.CacheKey, s.Journal.Payload, ct),
                static (s, ct) => s.Self.ApplySetEntryAsync(s.Memory, ct)),
            cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask<bool> TryAddEntryWithPreparedPayloadAsync(
        string operationId,
        string cacheName,
        string key,
        NodeCacheEntry<T> entry,
        PreparedJournalEntry prepared,
        CancellationToken cancellationToken)
    {
        using var payload = JournalEntryPayload.Encode(in prepared);
        var cacheKey = new CacheKey(cacheName, key);
        var args = new TryAddMutationArgs(operationId, cacheName, key, entry, payload.Memory, cacheKey);
        return await _executor.ExecuteAsync(
            cacheKey,
            static (s, ct) => EvaluateTryAddPreconditionAsync(s.Self, s.Args, ct),
            new DurableMutationPipeline<(JournalLoggingCacheDecorator<T> Self, TryAddMutationArgs Args), bool>(
                (this, args),
                static (s, ownership, ct) => s.Self._journal.AppendPutAsync(ownership, s.Args.CacheKey, s.Args.Payload, ct),
                static (s, ct) => s.Self._inner.TryAddEntryAsync(s.Args.OperationId, s.Args.CacheName, s.Args.Key, s.Args.Entry, ct)),
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

    private static async ValueTask<DurableMutationCondition<bool>> EvaluateDecidedUpsertAsync<TArgs>(
        DecidedUpsertState<TArgs> state,
        CancellationToken cancellationToken)
    {
        var current = await state.Self._inner.GetEntryAsync(state.CacheName, state.Key, cancellationToken).ConfigureAwait(false);
        if (current == null)
            return DurableMutationCondition<bool>.Skip(false);

        var decided = state.Decide(current, state.Self.UtcNow, state.Args);
        if (decided == null)
            return DurableMutationCondition<bool>.Skip(false);

        var prepared = JournalEntryPayload.PrepareEncode(decided);
        EntryPayloadSizeGuard.EnsureLengthWithinLimit(prepared.EncodedLength);
        state.Upsert.Set(decided, JournalEntryPayload.Encode(in prepared));
        return DurableMutationCondition<bool>.Apply();
    }

    private async ValueTask<bool> ApplySetEntryAsync(SetMemoryArgs args, CancellationToken cancellationToken)
    {
        await _inner.SetEntryAsync(args.OperationId, args.CacheName, args.Key, args.Entry, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private bool IsLocalOwner(string cacheName, string key) => string.Equals(_ring.GetOwner(cacheName, key), _self, StringComparison.Ordinal);

    private async ValueTask<bool> ExecuteDecidedUpsertAsync<TArgs>(
        string operationId,
        string cacheName,
        string key,
        TArgs args,
        Func<NodeCacheEntry<T>, DateTime, TArgs, NodeCacheEntry<T>?> decide,
        CancellationToken cancellationToken)
    {
        using var upsert = new DecidedUpsert();
        var cacheKey = new CacheKey(cacheName, key);
        var state = new DecidedUpsertState<TArgs>(this, cacheName, key, args, decide, upsert);
        return await _executor.ExecuteAsync(
            cacheKey,
            static (s, ct) => EvaluateDecidedUpsertAsync(s.Decision, ct),
            new DurableMutationPipeline<(DecidedUpsertState<TArgs> Decision, string OperationId, CacheKey CacheKey), bool>(
                (state, operationId, cacheKey),
                static (s, ownership, ct) => s.Decision.Self._journal.AppendPutAsync(ownership, s.CacheKey, s.Decision.Upsert.Payload, ct),
                static (s, ct) => s.Decision.Self.ApplySetEntryAsync(new SetMemoryArgs(s.OperationId, s.Decision.CacheName, s.Decision.Key, s.Decision.Upsert.Entry ?? ThrowHelper.Throw<NodeCacheEntry<T>>(new InvalidOperationException("The decided entry was not prepared."))), ct)),
            cancellationToken).ConfigureAwait(false);
    }

    [Immutable]
    private readonly record struct PutJournalArgs(CacheKey CacheKey, ReadOnlyMemory<byte> Payload);

    [Immutable]
    private readonly record struct RemoveJournalArgs(CacheKey CacheKey);

    [Immutable]
    private readonly record struct RemoveMemoryArgs(string OperationId, string CacheName, string Key);

    [Immutable]
    private readonly record struct SetMemoryArgs(string OperationId, string CacheName, string Key, NodeCacheEntry<T> Entry);

    [Immutable]
    private readonly record struct TryAddMutationArgs(string OperationId, string CacheName, string Key, NodeCacheEntry<T> Entry, ReadOnlyMemory<byte> Payload, CacheKey CacheKey);

    [Immutable]
    private readonly record struct DecidedUpsertState<TArgs>(
        JournalLoggingCacheDecorator<T> Self,
        string CacheName,
        string Key,
        TArgs Args,
        Func<NodeCacheEntry<T>, DateTime, TArgs, NodeCacheEntry<T>?> Decide,
        DecidedUpsert Upsert);

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
