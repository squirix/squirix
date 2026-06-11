using System;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Contracts;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Squirix.Transport.Grpc.Mappers;

namespace Squirix.Server.Adapters.Grpc;

internal sealed class SquirixServiceAdapter<T> : SquirixCacheService.SquirixCacheServiceBase
{
    private readonly IGrpcCacheOperations<T> _cacheOperations;
    private readonly IRemoteInvocationState _invocationState;
    private readonly INodeOwnershipResolver _ownershipResolver;

    public SquirixServiceAdapter(IGrpcCacheOperations<T> cacheOperations, INodeOwnershipResolver ownershipResolver, IRemoteInvocationState invocationState)
    {
        _cacheOperations = cacheOperations ?? throw new ArgumentNullException(nameof(cacheOperations));
        _ownershipResolver = ownershipResolver ?? throw new ArgumentNullException(nameof(ownershipResolver));
        _invocationState = invocationState ?? throw new ArgumentNullException(nameof(invocationState));
    }

    public override async Task<GetResponse> Get(GetRequest request, ServerCallContext context)
    {
        RequireValidCacheKey(request.Key);
        var entry = await ApiForRequest(request.CacheName).GetEntryAsync(request.Key, context.CancellationToken);
        return entry is null ? throw CacheOperationContract.NotFound().ToRpcException() : new GetResponse { Entry = entry.MapToProto() };
    }

    public override async Task<GetValueResponse> GetValue(GetValueRequest request, ServerCallContext context)
    {
        RequireValidCacheKey(request.Key);
        var result = await ApiForRequest(request.CacheName).TryGetValueAsync(request.Key, context.CancellationToken);
        var response = new GetValueResponse { Found = result.Found };
        if (result.Found)
            response.Value = ProtoEx.CacheValueToGrpcValue(result.Value);

        return response;
    }

    public override async Task<SetResponse> Set(SetRequest request, ServerCallContext context)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        await _cacheOperations.ForCache(cacheName).InsertAsync(request.Key, request.Entry.MapFromProto<T>(), context.CancellationToken).ConfigureAwait(false);
        return new SetResponse();
    }

    public override async Task<SetResponse> SetValue(SetValueRequest request, ServerCallContext context)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        await _cacheOperations.ForCache(cacheName).InsertAsync(
            request.Key,
            ProtoEx.CacheValueFromGrpcValue<T>(request.Value, request.ExpiresUtc, request.Expiration),
            context.CancellationToken).ConfigureAwait(false);
        return new SetResponse();
    }

    public override async Task<RemoveExpirationResponse> RemoveExpiration(RemoveExpirationRequest request, ServerCallContext context)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var found = await _cacheOperations.ForCache(cacheName).RemoveExpirationAsync(request.Key, context.CancellationToken).ConfigureAwait(false);
        return new RemoveExpirationResponse { Found = found };
    }

    public override async Task<RemoveResponse> Remove(RemoveRequest request, ServerCallContext context)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var result = await _cacheOperations.ForCache(cacheName).TryRemoveAsync(request.Key, context.CancellationToken);
        var response = new RemoveResponse { Removed = result.Removed };
        if (result.Removed)
            response.PreviousValue = ProtoEx.CacheValueToGrpcStruct(result.Value);

        return response;
    }

    public override async Task<TouchResponse> Touch(TouchRequest request, ServerCallContext context)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var found = await _cacheOperations.ForCache(cacheName).TouchAsync(request.Key, request.Expiration.ToTimeSpan(), context.CancellationToken).ConfigureAwait(false);
        return new TouchResponse { Found = found };
    }

    public override async Task<TrySetResponse> TrySet(TrySetRequest request, ServerCallContext context)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var added = await _cacheOperations.ForCache(cacheName).TryInsertAsync(request.Key, request.Entry.MapFromProto<T>(), context.CancellationToken).ConfigureAwait(false);
        return new TrySetResponse { Added = added };
    }

    public override async Task<TrySetResponse> TrySetValue(TrySetValueRequest request, ServerCallContext context)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var added = await _cacheOperations.ForCache(cacheName).TryInsertAsync(
            request.Key,
            ProtoEx.CacheValueFromGrpcValue<T>(request.Value, request.ExpiresUtc, request.Expiration),
            context.CancellationToken).ConfigureAwait(false);
        return new TrySetResponse { Added = added };
    }

    private static string RequireCacheName(string cacheName) => string.IsNullOrWhiteSpace(cacheName)
        ? throw new RpcException(new Status(StatusCode.InvalidArgument, "cache_name is required for internal cluster RPCs."))
        : cacheName;

    private static void RequireValidCacheKey(string key)
    {
        if (!CacheKeyValidator.TryValidate(key, out _))
            throw CacheOperationContract.InvalidCacheKey(key).ToRpcException();
    }

    private ICacheApi<T> ApiForRequest(string cacheName) => _cacheOperations.ForCache(RequireCacheName(cacheName));

    private void EnsureLocalOwnerForInternalOwnerRpc(string cacheName, string key)
    {
        if (!_invocationState.IsInternalOwnerInvocation)
            return;

        var expectedOwner = _ownershipResolver.GetOwner(cacheName, key);
        if (string.Equals(expectedOwner, _ownershipResolver.SelfNodeId, StringComparison.Ordinal))
            return;

        var detail = $"Key '{CacheKeySanitizer.Sanitize(key)}' for cache '{cacheName}' is owned by '{expectedOwner}', not current node '{_ownershipResolver.SelfNodeId}'.";
        throw new RpcException(new Status(StatusCode.FailedPrecondition, detail), GrpcStaleOwnerMarkers.CreateStaleOwnerTrailers());
    }
}
