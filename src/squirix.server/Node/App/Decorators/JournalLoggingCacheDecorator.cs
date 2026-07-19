using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Appends journal records for local-owner core mutations.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
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
            new DurableMutationPipeline<JournalLoggingCacheDecorator<T>, RemoveJournalArgs, RemoveMemoryArgs, CacheRemoveResult<T>>(
                this,
                new RemoveJournalArgs(cacheKey),
                static (self, args, ct) => self._journal.AppendRemoveAsync(args.CacheKey, ct),
                new RemoveMemoryArgs(operationId, cacheName, key),
                static (self, args, ct) => self._inner.RemoveAsync(args.OperationId, args.CacheName, args.Key, ct)),
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
            new DurableMutationPipeline<JournalLoggingCacheDecorator<T>, RemoveExpirationJournalArgs, RemoveExpirationMemoryArgs, bool>(
                this,
                new RemoveExpirationJournalArgs(cacheKey),
                static (self, args, ct) => self._journal.AppendRemoveExpirationAsync(args.CacheKey, ct),
                new RemoveExpirationMemoryArgs(operationId, cacheName, key),
                static (self, args, ct) => self._inner.RemoveExpirationAsync(args.OperationId, args.CacheName, args.Key, ct)),
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
            new DurableMutationPipeline<JournalLoggingCacheDecorator<T>, TouchJournalArgs, TouchMemoryArgs, bool>(
                this,
                new TouchJournalArgs(cacheKey, expiresUtc),
                static (self, args, ct) => self._journal.AppendTouchExpirationAsync(args.CacheKey, args.ExpiresUtc, ct),
                new TouchMemoryArgs(operationId, cacheName, key, expiration),
                static (self, args, ct) => self._inner.TouchAsync(args.OperationId, args.CacheName, args.Key, args.Expiration, ct)),
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
        if (existing is null)
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
        var payloadLength = EncodePreparedPutPayload(in prepared, out var payloadBuffer);
        try
        {
            var cacheKey = new CacheKey(cacheName, key);
            _ = await _durableMutations.ExecuteAsync(
                cacheKey,
                static _ => ValueTask.FromResult(DurableMutationCondition<bool>.Apply()),
                new DurableMutationPipeline<JournalLoggingCacheDecorator<T>, PutJournalArgs, SetMemoryArgs, bool>(
                    this,
                    new PutJournalArgs(cacheKey, payloadBuffer.AsMemory(0, payloadLength)),
                    static (self, args, ct) => self._journal.AppendPutAsync(args.CacheKey, args.Payload, ct),
                    new SetMemoryArgs(operationId, cacheName, key, entry),
                    static (self, args, ct) => self.ApplySetEntryAsync(args, ct)),
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payloadBuffer);
        }
    }

    internal async ValueTask<bool> TryAddEntryWithPreparedPayloadAsync(
        string operationId,
        string cacheName,
        string key,
        NodeCacheEntry<T> entry,
        PreparedJournalEntry prepared,
        CancellationToken cancellationToken)
    {
        var payloadLength = EncodePreparedPutPayload(in prepared, out var payloadBuffer);
        try
        {
            var cacheKey = new CacheKey(cacheName, key);
            var args = new TryAddMutationArgs(operationId, cacheName, key, entry, payloadBuffer.AsMemory(0, payloadLength), cacheKey);
            return await _durableMutations.ExecuteAsync(
                cacheKey,
                this,
                args,
                static (self, state, ct) => EvaluateTryAddPreconditionAsync(self, state, ct),
                static (self, state, ct) => self._journal.AppendPutAsync(state.CacheKey, state.Payload, ct),
                static (self, state, ct) => self._inner.TryAddEntryAsync(state.OperationId, state.CacheName, state.Key, state.Entry, ct),
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payloadBuffer);
        }
    }

    internal async ValueTask<bool> UpdateWithPreparedPayloadAsync(
        string operationId,
        string cacheName,
        string key,
        T? value,
        PreparedJournalEntry prepared,
        CancellationToken cancellationToken)
    {
        var payloadLength = EncodePreparedPutPayload(in prepared, out var payloadBuffer);
        try
        {
            var cacheKey = new CacheKey(cacheName, key);
            return await _durableMutations.ExecuteAsync(
                cacheKey,
                static _ => ValueTask.FromResult(DurableMutationCondition<bool>.Apply()),
                new DurableMutationPipeline<JournalLoggingCacheDecorator<T>, PutJournalArgs, UpdateMemoryArgs, bool>(
                    this,
                    new PutJournalArgs(cacheKey, payloadBuffer.AsMemory(0, payloadLength)),
                    static (self, args, ct) => self._journal.AppendPutAsync(args.CacheKey, args.Payload, ct),
                    new UpdateMemoryArgs(operationId, cacheName, key, value),
                    static (self, args, ct) => self._inner.UpdateAsync(args.OperationId, args.CacheName, args.Key, args.Value, ct)),
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payloadBuffer);
        }
    }

    private static NodeCacheEntry<T> CreateUpdateReplacement(NodeCacheEntry<T> existing, T? value) => new()
    {
        Value = value,
        ExpiresUtc = existing.ExpiresUtc,
        Expiration = existing.Expiration,
        Version = existing.Version,
    };

    private static int EncodePreparedPutPayload(in PreparedJournalEntry prepared, out byte[] payloadBuffer) => JournalEntryPayload.Encode(in prepared, out payloadBuffer);

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

    private readonly record struct PutJournalArgs(CacheKey CacheKey, ReadOnlyMemory<byte> Payload);

    private readonly record struct RemoveExpirationJournalArgs(CacheKey CacheKey);

    private readonly record struct RemoveExpirationMemoryArgs(string OperationId, string CacheName, string Key);

    private readonly record struct RemoveJournalArgs(CacheKey CacheKey);

    private readonly record struct RemoveMemoryArgs(string OperationId, string CacheName, string Key);

    private readonly record struct SetMemoryArgs(string OperationId, string CacheName, string Key, NodeCacheEntry<T> Entry);

    private readonly record struct TouchJournalArgs(CacheKey CacheKey, DateTime ExpiresUtc);

    private readonly record struct TouchMemoryArgs(string OperationId, string CacheName, string Key, TimeSpan Expiration);

    private readonly record struct TryAddMutationArgs(string OperationId, string CacheName, string Key, NodeCacheEntry<T> Entry, ReadOnlyMemory<byte> Payload, CacheKey CacheKey);

    private readonly record struct UpdateMemoryArgs(string OperationId, string CacheName, string Key, T? Value);
}
