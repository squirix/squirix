using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Appends journal records for local-owner core mutations.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class JournalLoggingCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly DurableMutationExecutor _durableMutations;
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly IJournalCoordinator _journal;
    private readonly INodeLocator _ring;
    private readonly string _self;

    internal JournalLoggingCacheDecorator(string self, INodeLocator ring, ILogicalNamespacedCache<T> inner, IJournalCoordinator journal, DurableMutationExecutor durableMutations)
    {
        _self = self ?? throw new ArgumentNullException(nameof(self));
        _ring = ring ?? throw new ArgumentNullException(nameof(ring));
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
        _durableMutations = durableMutations ?? throw new ArgumentNullException(nameof(durableMutations));
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetValueAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return _inner.RemoveAsync(operationId, cacheName, key, cancellationToken);

        var cacheKey = new CacheKey(cacheName, key);
        return _durableMutations.ExecuteAsync(
            cacheKey,
            static _ => ValueTask.FromResult(DurableMutationCondition<CacheRemoveResult<T>>.Apply()),
            new DurableMutationPipeline<(JournalLoggingCacheDecorator<T> Self, RemoveJournalArgs Journal, RemoveMemoryArgs Memory), CacheRemoveResult<T>>(
                (this, new RemoveJournalArgs(cacheKey), new RemoveMemoryArgs(operationId, cacheName, key)),
                static (s, ct) => s.Self._journal.AppendRemoveAsync(s.Journal.CacheKey, ct),
                static (s, ct) => s.Self._inner.RemoveAsync(s.Memory.OperationId, s.Memory.CacheName, s.Memory.Key, ct)),
            cancellationToken);
    }

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);

        var cacheKey = new CacheKey(cacheName, key);
        return _durableMutations.ExecuteAsync(
            cacheKey,
            static _ => ValueTask.FromResult(DurableMutationCondition<bool>.Apply()),
            new DurableMutationPipeline<(JournalLoggingCacheDecorator<T> Self, RemoveExpirationJournalArgs Journal, RemoveExpirationMemoryArgs Memory), bool>(
                (this, new RemoveExpirationJournalArgs(cacheKey), new RemoveExpirationMemoryArgs(operationId, cacheName, key)),
                static (s, ct) => s.Self._journal.AppendRemoveExpirationAsync(s.Journal.CacheKey, ct),
                static (s, ct) => s.Self._inner.RemoveExpirationAsync(s.Memory.OperationId, s.Memory.CacheName, s.Memory.Key, ct)),
            cancellationToken);
    }

    public async ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
        {
            await _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
            return;
        }

        var prepared = JournalEntryPayload.PrepareEncode(entry);
        EntryPayloadSizeGuard.EnsureLengthWithinLimit(prepared.EncodedLength);
        await SetEntryWithPreparedPayloadAsync(operationId, cacheName, key, entry, prepared, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);

        var cacheKey = new CacheKey(cacheName, key);
        var expiresUtc = DateTime.UtcNow.Add(expiration);
        return _durableMutations.ExecuteAsync(
            cacheKey,
            static _ => ValueTask.FromResult(DurableMutationCondition<bool>.Apply()),
            new DurableMutationPipeline<(JournalLoggingCacheDecorator<T> Self, TouchJournalArgs Journal, TouchMemoryArgs Memory), bool>(
                (this, new TouchJournalArgs(cacheKey, expiresUtc), new TouchMemoryArgs(operationId, cacheName, key, expiration)),
                static (s, ct) => s.Self._journal.AppendTouchExpirationAsync(s.Journal.CacheKey, s.Journal.ExpiresUtc, ct),
                static (s, ct) => s.Self._inner.TouchAsync(s.Memory.OperationId, s.Memory.CacheName, s.Memory.Key, s.Memory.Expiration, ct)),
            cancellationToken);
    }

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken);

        var prepared = JournalEntryPayload.PrepareEncode(entry);
        EntryPayloadSizeGuard.EnsureLengthWithinLimit(prepared.EncodedLength);
        return TryAddEntryWithPreparedPayloadAsync(operationId, cacheName, key, entry, prepared, cancellationToken);
    }

    public async ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return await _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken).ConfigureAwait(false);

        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing == null)
            return false;

        var prepared = JournalEntryPayload.PrepareEncode(CreateUpdateReplacement(existing, value));
        EntryPayloadSizeGuard.EnsureLengthWithinLimit(prepared.EncodedLength);
        return await UpdateWithPreparedPayloadAsync(operationId, cacheName, key, value, prepared, cancellationToken).ConfigureAwait(false);
    }

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
        _ = await _durableMutations.ExecuteAsync(
            cacheKey,
            static _ => ValueTask.FromResult(DurableMutationCondition<bool>.Apply()),
            new DurableMutationPipeline<(JournalLoggingCacheDecorator<T> Self, PutJournalArgs Journal, SetMemoryArgs Memory), bool>(
                (this, new PutJournalArgs(cacheKey, payload.Memory), new SetMemoryArgs(operationId, cacheName, key, entry)),
                static (s, ct) => s.Self._journal.AppendPutAsync(s.Journal.CacheKey, s.Journal.Payload, ct),
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
        return await _durableMutations.ExecuteAsync(
            cacheKey,
            ct => EvaluateTryAddPreconditionAsync(this, args, ct),
            new DurableMutationPipeline<(JournalLoggingCacheDecorator<T> Self, TryAddMutationArgs Args), bool>(
                (this, args),
                static (s, ct) => s.Self._journal.AppendPutAsync(s.Args.CacheKey, s.Args.Payload, ct),
                static (s, ct) => s.Self._inner.TryAddEntryAsync(s.Args.OperationId, s.Args.CacheName, s.Args.Key, s.Args.Entry, ct)),
            cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask<bool> UpdateWithPreparedPayloadAsync(
        string operationId,
        string cacheName,
        string key,
        T? value,
        PreparedJournalEntry prepared,
        CancellationToken cancellationToken)
    {
        using var payload = JournalEntryPayload.Encode(in prepared);
        var cacheKey = new CacheKey(cacheName, key);
        return await _durableMutations.ExecuteAsync(
            cacheKey,
            static _ => ValueTask.FromResult(DurableMutationCondition<bool>.Apply()),
            new DurableMutationPipeline<(JournalLoggingCacheDecorator<T> Self, PutJournalArgs Journal, UpdateMemoryArgs Memory), bool>(
                (this, new PutJournalArgs(cacheKey, payload.Memory), new UpdateMemoryArgs(operationId, cacheName, key, value)),
                static (s, ct) => s.Self._journal.AppendPutAsync(s.Journal.CacheKey, s.Journal.Payload, ct),
                static (s, ct) => s.Self._inner.UpdateAsync(s.Memory.OperationId, s.Memory.CacheName, s.Memory.Key, s.Memory.Value, ct)),
            cancellationToken).ConfigureAwait(false);
    }

    private static NodeCacheEntry<T> CreateUpdateReplacement(NodeCacheEntry<T> existing, T? value) => new()
    {
        Value = value,
        ExpiresUtc = existing.ExpiresUtc,
        Expiration = existing.Expiration,
        Version = existing.Version,
    };

    private static async ValueTask<DurableMutationCondition<bool>> EvaluateTryAddPreconditionAsync(
        JournalLoggingCacheDecorator<T> self,
        TryAddMutationArgs args,
        CancellationToken cancellationToken)
    {
        var existing = await self._inner.GetValueAsync(args.CacheName, args.Key, cancellationToken).ConfigureAwait(false);
        return existing.Found ? DurableMutationCondition<bool>.Skip(false) : DurableMutationCondition<bool>.Apply();
    }

    private async ValueTask<bool> ApplySetEntryAsync(SetMemoryArgs args, CancellationToken cancellationToken)
    {
        await _inner.SetEntryAsync(args.OperationId, args.CacheName, args.Key, args.Entry, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private bool IsLocalOwner(string cacheName, string key) => string.Equals(_ring.GetOwner(cacheName, key), _self, StringComparison.Ordinal);

    [Immutable]
    private readonly record struct PutJournalArgs(CacheKey CacheKey, ReadOnlyMemory<byte> Payload);

    [Immutable]
    private readonly record struct RemoveExpirationJournalArgs(CacheKey CacheKey);

    [Immutable]
    private readonly record struct RemoveExpirationMemoryArgs(string OperationId, string CacheName, string Key);

    [Immutable]
    private readonly record struct RemoveJournalArgs(CacheKey CacheKey);

    [Immutable]
    private readonly record struct RemoveMemoryArgs(string OperationId, string CacheName, string Key);

    [Immutable]
    private readonly record struct SetMemoryArgs(string OperationId, string CacheName, string Key, NodeCacheEntry<T> Entry);

    [Immutable]
    private readonly record struct TouchJournalArgs(CacheKey CacheKey, DateTime ExpiresUtc);

    [Immutable]
    private readonly record struct TouchMemoryArgs(string OperationId, string CacheName, string Key, TimeSpan Expiration);

    [Immutable]
    private readonly record struct TryAddMutationArgs(string OperationId, string CacheName, string Key, NodeCacheEntry<T> Entry, ReadOnlyMemory<byte> Payload, CacheKey CacheKey);

    [Immutable]
    private readonly record struct UpdateMemoryArgs(string OperationId, string CacheName, string Key, T? Value);
}
