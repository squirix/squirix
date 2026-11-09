using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Serialization.Grpc;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Adapters.Grpc;

[Immutable]
internal sealed class SquirixServiceAdapter<T> : SquirixCacheService.SquirixCacheServiceBase
{
    private readonly IInboundEndpointCacheOperations<T> _cacheOperations;
    private readonly IDistributedCacheAccess? _distributedAccess;
    private readonly OwnerRpcForwarder _forwarder;
    private readonly MutationHandlers _handlers;
    private readonly IRpcMutationIdempotencyCoordinator _idempotency;
    private readonly OwnerRouter _router;
    private readonly TimeProvider _timeProvider;

    public SquirixServiceAdapter(
        IInboundEndpointCacheOperations<T> cacheOperations,
        OwnerRouter router,
        OwnerRpcForwarder forwarder,
        IRpcMutationIdempotencyCoordinator idempotency,
        TimeProvider timeProvider,
        IDistributedCacheAccess? distributedAccess = null)
    {
        ArgumentNullException.ThrowIfNull(cacheOperations);
        ArgumentNullException.ThrowIfNull(router);
        ArgumentNullException.ThrowIfNull(forwarder);
        ArgumentNullException.ThrowIfNull(idempotency);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _cacheOperations = cacheOperations;
        _router = router;
        _forwarder = forwarder;
        _idempotency = idempotency;
        _timeProvider = timeProvider;
        _distributedAccess = distributedAccess;
        _handlers = new MutationHandlers(cacheOperations);
    }

    public override Task<GetEntryAsyncResponse> GetEntry(GetEntryAsyncRequest request, ServerCallContext context) => _router.ExecuteAsync(
        request.CacheName,
        request.Key,
        (Adapter: this, Request: request),
        static (s, owner, ct) => s.Adapter._forwarder.GetEntryAsync(owner, s.Request, ct),
        static (s, ct) => s.Adapter.GetEntryLocalAsync(s.Request, ct),
        context.CancellationToken);

    public override Task<GetExpirationAsyncResponse> GetExpiration(GetExpirationAsyncRequest request, ServerCallContext context) => _router.ExecuteAsync(
        request.CacheName,
        request.Key,
        (Adapter: this, Request: request),
        static (s, owner, ct) => s.Adapter._forwarder.GetExpirationAsync(owner, s.Request, ct),
        static (s, ct) => s.Adapter.GetExpirationLocalAsync(s.Request, ct),
        context.CancellationToken);

    public override Task<GetOrAddAsyncResponse> GetOrAdd(GetOrAddAsyncRequest request, ServerCallContext context) => _router.ExecuteAsync(
        request.CacheName,
        request.Key,
        (Adapter: this, Request: request),
        static (s, owner, ct) => s.Adapter._forwarder.GetOrAddAsync(owner, s.Request, ct),
        static (s, ct) => s.Adapter._idempotency.ExecuteAsync(
            s.Request.OperationId,
            static h => RpcMutationFingerprints.GetOrAdd(h.Request.CacheName, h.Request.Key, h.Request.Entry),
            (Handlers: s.Adapter._handlers, s.Request),
            static (h, token) => h.Handlers.GetOrAddAsyncCoreAsync(h.Request, token),
            ct),
        context.CancellationToken);

    public override Task<GetValueAsyncResponse> GetValue(GetValueAsyncRequest request, ServerCallContext context) => _router.ExecuteAsync(
        request.CacheName,
        request.Key,
        (Adapter: this, Request: request),
        static (s, owner, ct) => s.Adapter._forwarder.GetValueAsync(owner, s.Request, ct),
        static (s, ct) => s.Adapter.GetValueLocalAsync(s.Request, ct),
        context.CancellationToken);

    public override Task<RemoveAsyncResponse> Remove(RemoveAsyncRequest request, ServerCallContext context) => _router.ExecuteAsync(
        request.CacheName,
        request.Key,
        (Adapter: this, Request: request),
        static (s, owner, ct) => s.Adapter._forwarder.RemoveAsync(owner, s.Request, ct),
        static (s, ct) => s.Adapter._idempotency.ExecuteAsync(
            s.Request.OperationId,
            static h => RpcMutationFingerprints.Remove(h.Request.CacheName, h.Request.Key),
            (Handlers: s.Adapter._handlers, s.Request),
            static (h, token) => h.Handlers.RemoveAsyncCoreAsync(h.Request, token),
            ct),
        context.CancellationToken);

    public override Task<RemoveExpirationAsyncResponse> RemoveExpiration(RemoveExpirationAsyncRequest request, ServerCallContext context) => _router.ExecuteAsync(
        request.CacheName,
        request.Key,
        (Adapter: this, Request: request),
        static (s, owner, ct) => s.Adapter._forwarder.RemoveExpirationAsync(owner, s.Request, ct),
        static (s, ct) => s.Adapter._idempotency.ExecuteAsync(
            s.Request.OperationId,
            static h => RpcMutationFingerprints.RemoveExpiration(h.Request.CacheName, h.Request.Key),
            (Handlers: s.Adapter._handlers, s.Request),
            static (h, token) => h.Handlers.RemoveExpirationAsyncCoreAsync(h.Request, token),
            ct),
        context.CancellationToken);

    public override Task<GetDistributedAsyncResponse> GetDistributed(GetDistributedAsyncRequest request, ServerCallContext context) => _router.ExecuteAsync(
        request.CacheName,
        request.Key,
        (Adapter: this, Request: request),
        static (s, owner, ct) => s.Adapter._forwarder.GetDistributedAsync(owner, s.Request, ct),
        static (s, ct) => s.Adapter.AccessDistributedLocalAsync(s.Request.OperationId, s.Request.CacheName, s.Request.Key, false, ct),
        context.CancellationToken);

    public override Task<RefreshDistributedAsyncResponse> RefreshDistributed(RefreshDistributedAsyncRequest request, ServerCallContext context) => _router.ExecuteAsync(
        request.CacheName,
        request.Key,
        (Adapter: this, Request: request),
        static (s, owner, ct) => s.Adapter._forwarder.RefreshDistributedAsync(owner, s.Request, ct),
        static async (s, ct) =>
        {
            _ = await s.Adapter.AccessDistributedLocalAsync(s.Request.OperationId, s.Request.CacheName, s.Request.Key, true, ct).ConfigureAwait(false);
            return new RefreshDistributedAsyncResponse();
        },
        context.CancellationToken);

    public override Task<SetAsyncResponse> SetDistributed(SetDistributedAsyncRequest request, ServerCallContext context) => _router.ExecuteAsync(
        request.CacheName,
        request.Key,
        (Adapter: this, Request: request),
        static (s, owner, ct) => s.Adapter._forwarder.SetDistributedAsync(owner, s.Request, ct),
        static (s, ct) => s.Adapter._idempotency.ExecuteAsync(
            s.Request.OperationId,
            static h => RpcMutationFingerprints.SetDistributed(h.Request),
            (Handlers: s.Adapter._handlers, s.Request),
            static (h, token) => h.Handlers.SetDistributedAsyncCoreAsync(h.Request, token),
            ct),
        context.CancellationToken);

    public override Task<SetAsyncResponse> SetEntry(SetEntryAsyncRequest request, ServerCallContext context) => _router.ExecuteAsync(
        request.CacheName,
        request.Key,
        (Adapter: this, Request: request),
        static (s, owner, ct) => s.Adapter._forwarder.SetEntryAsync(owner, s.Request, ct),
        static (s, ct) => s.Adapter._idempotency.ExecuteAsync(
            s.Request.OperationId,
            static h => RpcMutationFingerprints.SetEntry(h.Request.CacheName, h.Request.Key, h.Request.Entry),
            (Handlers: s.Adapter._handlers, s.Request),
            static (h, token) => h.Handlers.SetEntryAsyncCoreAsync(h.Request, token),
            ct),
        context.CancellationToken);

    public override Task<TouchAsyncResponse> Touch(TouchAsyncRequest request, ServerCallContext context) => _router.ExecuteAsync(
        request.CacheName,
        request.Key,
        (Adapter: this, Request: request),
        static (s, owner, ct) => s.Adapter._forwarder.TouchAsync(owner, s.Request, ct),
        static (s, ct) => s.Adapter._idempotency.ExecuteAsync(
            s.Request.OperationId,
            static h => RpcMutationFingerprints.Touch(h.Request.CacheName, h.Request.Key, h.Request.Expiration),
            (Handlers: s.Adapter._handlers, s.Request),
            static (h, token) => h.Handlers.TouchAsyncCoreAsync(h.Request, token),
            ct),
        context.CancellationToken);

    public override Task<TryAddAsyncResponse> TryAddEntry(TryAddEntryAsyncRequest request, ServerCallContext context) => _router.ExecuteAsync(
        request.CacheName,
        request.Key,
        (Adapter: this, Request: request),
        static (s, owner, ct) => s.Adapter._forwarder.AddEntryIfAbsentAsync(owner, s.Request, ct),
        static (s, ct) => s.Adapter._idempotency.ExecuteAsync(
            s.Request.OperationId,
            static h => RpcMutationFingerprints.AddEntryIfAbsent(h.Request.CacheName, h.Request.Key, h.Request.Entry),
            (Handlers: s.Adapter._handlers, s.Request),
            static (h, token) => h.Handlers.AddEntryAsyncCoreAsync(h.Request, token),
            ct),
        context.CancellationToken);

    public override Task<UpdateAsyncResponse> Update(UpdateAsyncRequest request, ServerCallContext context) => _router.ExecuteAsync(
        request.CacheName,
        request.Key,
        (Adapter: this, Request: request),
        static (s, owner, ct) => s.Adapter._forwarder.UpdateAsync(owner, s.Request, ct),
        static (s, ct) => s.Adapter._idempotency.ExecuteAsync(
            s.Request.OperationId,
            static h => RpcMutationFingerprints.Update(h.Request.CacheName, h.Request.Key, h.Request.Entry),
            (Handlers: s.Adapter._handlers, s.Request),
            static (h, token) => h.Handlers.UpdateAsyncCoreAsync(h.Request, token),
            ct),
        context.CancellationToken);

    private Task<GetDistributedAsyncResponse> AccessDistributedLocalAsync(string operationId, string cacheName, string key, bool refresh, CancellationToken cancellationToken)
    {
        var cache = SquirixServiceAdapterValidation.RequireCacheName(cacheName);
        SquirixServiceAdapterValidation.RequireValidCacheKey(key);
        var id = RpcMutationContracts.RequireOperationId(operationId);
        var fingerprint = RpcMutationFingerprints.DistributedAccess(cache, key, refresh);
        var access = _distributedAccess ?? ThrowHelper.Throw<IDistributedCacheAccess>(new RpcException(new Status(StatusCode.Unimplemented, "Distributed cache access is unavailable.")));
        return access.AccessAsync(id, cache, key, fingerprint, refresh, cancellationToken);
    }

    private async Task<GetEntryAsyncResponse> GetEntryLocalAsync(GetEntryAsyncRequest request, CancellationToken cancellationToken)
    {
        SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
        var entry = await ApiForRequest(request.CacheName).GetEntryAsync(request.Key, cancellationToken).ConfigureAwait(false);
        if (entry == null)
            return new GetEntryAsyncResponse { Found = false };

        var response = new GetEntryAsyncResponse { Found = true, Entry = entry.MapToProto() };
        if (entry.ExpiresUtc is { } expiresUtc)
            response.Remaining = Duration.FromTimeSpan(RemainingUntil(expiresUtc));

        return response;
    }

    private async Task<GetExpirationAsyncResponse> GetExpirationLocalAsync(GetExpirationAsyncRequest request, CancellationToken cancellationToken)
    {
        SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
        var read = await ApiForRequest(request.CacheName).GetValueWithDeadlineAsync(request.Key, cancellationToken).ConfigureAwait(false);
        if (!read.Found)
            return new GetExpirationAsyncResponse { Found = false };

        var response = new GetExpirationAsyncResponse { Found = true };
        if (read.ExpiresUtc is not { } expiresUtc)
            return response;

        response.HasExpiration = true;
        response.Remaining = Duration.FromTimeSpan(RemainingUntil(expiresUtc));
        return response;
    }

    private async Task<GetValueAsyncResponse> GetValueLocalAsync(GetValueAsyncRequest request, CancellationToken cancellationToken)
    {
        SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
        var result = await ApiForRequest(request.CacheName).GetValueAsync(request.Key, cancellationToken).ConfigureAwait(false);
        var response = new GetValueAsyncResponse { Found = result.Found };
        if (result.Found)
            response.Value = ServerProtoEx.CacheValueToGrpcValue(result.Value);

        return response;
    }

    private ICacheApi<T> ApiForRequest(string cacheName) => _cacheOperations.ForCache(SquirixServiceAdapterValidation.RequireCacheName(cacheName));

    /// <summary>Returns the time left before <paramref name="expiresUtc" /> on the server clock, never negative.</summary>
    /// <param name="expiresUtc">The entry deadline.</param>
    /// <returns>The remaining time.</returns>
    private TimeSpan RemainingUntil(DateTime expiresUtc)
    {
        var remaining = expiresUtc - _timeProvider.GetUtcNow().UtcDateTime;
        return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
    }

    [Immutable]
    private sealed class MutationHandlers
    {
        private readonly IInboundEndpointCacheOperations<T> _cacheOperations;

        internal MutationHandlers(IInboundEndpointCacheOperations<T> cacheOperations)
        {
            _cacheOperations = cacheOperations;
        }

        internal async Task<TryAddAsyncResponse> AddEntryAsyncCoreAsync(TryAddEntryAsyncRequest request, CancellationToken cancellationToken)
        {
            var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
            RpcMutationIdempotencyExecutionAmbient.RegisterOutcomeProjection<bool>(static added => new TryAddAsyncResponse { Added = added });
            var added = await _cacheOperations.ForCache(cacheName).TryAddEntryAsync(
                RpcMutationContracts.RequireOperationId(request.OperationId),
                request.Key,
                request.Entry.MapFromProto<T>(),
                cancellationToken).ConfigureAwait(false);
            return new TryAddAsyncResponse { Added = added };
        }

        internal async Task<GetOrAddAsyncResponse> GetOrAddAsyncCoreAsync(GetOrAddAsyncRequest request, CancellationToken cancellationToken)
        {
            var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
            var api = _cacheOperations.ForCache(cacheName);

            // The add runs first, with the operation id: a retry whose RPC record is gone replays the recorded outcome of the add instead
            // of reading the value its first attempt added and answering that nothing was added. An add over a present key without a
            // recorded outcome is refused before it reaches the journal or the replica group.
            var entry = request.Entry.MapFromProto<T>();

            // The response of an add that took effect carries no value and is known before the add is applied.
            RpcMutationIdempotencyExecutionAmbient.RegisterOutcomeProjection<bool>(static added =>
                added ? AddedResponse() : throw new InvalidOperationException("Only an add that took effect has a predicted outcome."));
            var result = await api.AddOrGetAsync(RpcMutationContracts.RequireOperationId(request.OperationId), request.Key, entry, cancellationToken).ConfigureAwait(false);
            if (result.Added)
                return AddedResponse();

            var existing = result.Existing;
            return existing.Found
                ? new GetOrAddAsyncResponse { Added = false, Found = true, Value = ServerProtoEx.CacheValueToGrpcValue(existing.Value) }
                : new GetOrAddAsyncResponse { Added = false, Found = false };
        }

        internal async Task<RemoveAsyncResponse> RemoveAsyncCoreAsync(RemoveAsyncRequest request, CancellationToken cancellationToken)
        {
            var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
            RpcMutationIdempotencyExecutionAmbient.RegisterOutcomeProjection<CacheRemoveResult<T>>(static result => RemoveResponse(result));
            var result = await _cacheOperations.ForCache(cacheName).RemoveAsync(RpcMutationContracts.RequireOperationId(request.OperationId), request.Key, cancellationToken)
                                               .ConfigureAwait(false);
            return RemoveResponse(result);
        }

        internal async Task<RemoveExpirationAsyncResponse> RemoveExpirationAsyncCoreAsync(RemoveExpirationAsyncRequest request, CancellationToken cancellationToken)
        {
            var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
            RpcMutationIdempotencyExecutionAmbient.RegisterOutcomeProjection<bool>(static found => new RemoveExpirationAsyncResponse { Found = found });
            var found = await _cacheOperations.ForCache(cacheName)
                                              .RemoveExpirationAsync(RpcMutationContracts.RequireOperationId(request.OperationId), request.Key, cancellationToken)
                                              .ConfigureAwait(false);
            return new RemoveExpirationAsyncResponse { Found = found };
        }

        internal async Task<SetAsyncResponse> SetDistributedAsyncCoreAsync(SetDistributedAsyncRequest request, CancellationToken cancellationToken)
        {
            var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
            var policy = new DistributedCachePolicy(request.HasExpiresAtUnixMs ? request.ExpiresAtUnixMs : null, request.HasSlidingTicks ? request.SlidingTicks : null);
            if (!policy.IsValid())
                throw new RpcException(new Status(StatusCode.InvalidArgument, "The distributed cache expiration policy is invalid."));

            if (request.Value.Length > EntryLimits.MaxEntrySizeBytes - DistributedCachePolicy.HeaderLength)
                throw ServerOpContract.PayloadTooLarge().ToRpcException();

            var write = new DistributedCacheWrite(in policy, request.Value.Span);
            if (write is not T value)
                throw new RpcException(new Status(StatusCode.Internal, "The node cannot store distributed cache values."));

            RpcMutationIdempotencyExecutionAmbient.RegisterOutcomeProjection<bool>(static _ => new SetAsyncResponse());
            await _cacheOperations.ForCache(cacheName).SetEntryAsync(
                RpcMutationContracts.RequireOperationId(request.OperationId),
                request.Key,
                new NodeCacheEntry<T>(value, expiresUtc: write.DeadlineForSizing),
                cancellationToken).ConfigureAwait(false);
            return new SetAsyncResponse();
        }

        internal async Task<SetAsyncResponse> SetEntryAsyncCoreAsync(SetEntryAsyncRequest request, CancellationToken cancellationToken)
        {
            var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
            RpcMutationIdempotencyExecutionAmbient.RegisterOutcomeProjection<bool>(static _ => new SetAsyncResponse());
            await _cacheOperations.ForCache(cacheName).SetEntryAsync(
                RpcMutationContracts.RequireOperationId(request.OperationId),
                request.Key,
                request.Entry.MapFromProto<T>(),
                cancellationToken).ConfigureAwait(false);
            return new SetAsyncResponse();
        }

        internal async Task<TouchAsyncResponse> TouchAsyncCoreAsync(TouchAsyncRequest request, CancellationToken cancellationToken)
        {
            var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
            RpcMutationIdempotencyExecutionAmbient.RegisterOutcomeProjection<bool>(static found => new TouchAsyncResponse { Found = found });
            var found = await _cacheOperations.ForCache(cacheName).TouchAsync(
                RpcMutationContracts.RequireOperationId(request.OperationId),
                request.Key,
                ServerProtoEx.RequireDuration(request.Expiration),
                cancellationToken).ConfigureAwait(false);
            return new TouchAsyncResponse { Found = found };
        }

        internal async Task<UpdateAsyncResponse> UpdateAsyncCoreAsync(UpdateAsyncRequest request, CancellationToken cancellationToken)
        {
            var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
            RpcMutationIdempotencyExecutionAmbient.RegisterOutcomeProjection<bool>(static updated => new UpdateAsyncResponse { Updated = updated });
            var updated = await _cacheOperations.ForCache(cacheName).UpdateAsync(
                RpcMutationContracts.RequireOperationId(request.OperationId),
                request.Key,
                request.Entry.MapFromProto<T>().Value,
                cancellationToken).ConfigureAwait(false);
            return new UpdateAsyncResponse { Updated = updated };
        }

        private static GetOrAddAsyncResponse AddedResponse() => new() { Added = true, Found = true };

        private static RemoveAsyncResponse RemoveResponse(CacheRemoveResult<T> result) => new() { Removed = result.Removed };
    }
}
