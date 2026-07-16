using System;
using System.Threading;
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

internal sealed class MutationHandlers<T>
{
    private readonly IGrpcCacheOperations<T> _cacheOperations;
    private readonly IRemoteInvocationState _invocationState;
    private readonly INodeOwnershipResolver _ownershipResolver;

    internal MutationHandlers(
        IGrpcCacheOperations<T> cacheOperations,
        INodeOwnershipResolver ownershipResolver,
        IRemoteInvocationState invocationState)
    {
        _cacheOperations = cacheOperations;
        _ownershipResolver = ownershipResolver;
        _invocationState = invocationState;
    }

    internal async Task<GetOrAddAsyncResponse> GetOrAddAsyncCoreAsync(GetOrAddAsyncRequest request, CancellationToken cancellationToken)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var api = _cacheOperations.ForCache(cacheName);
        var existing = await api.GetValueAsync(request.Key, cancellationToken).ConfigureAwait(false);
        if (existing.Found)
        {
            return new GetOrAddAsyncResponse
            {
                Added = false,
                Value = ServerProtoEx.CacheValueToGrpcValue(existing.Value),
            };
        }

        var entry = await request.Entry.MapFromProtoAsync<T>().ConfigureAwait(false);
        if (await api.TryAddEntryAsync(RpcMutationContracts.RequireOperationId(request.OperationId), request.Key, entry, cancellationToken).ConfigureAwait(false))
        {
            return new GetOrAddAsyncResponse
            {
                Added = true,
                Value = ServerProtoEx.CacheValueToGrpcValue(entry.Value),
            };
        }

        var afterRace = await api.GetValueAsync(request.Key, cancellationToken).ConfigureAwait(false);
        return new GetOrAddAsyncResponse
        {
            Added = false,
            Value = ServerProtoEx.CacheValueToGrpcValue(afterRace.Value),
        };
    }

    internal async Task<RemoveAsyncResponse> RemoveAsyncCoreAsync(RemoveAsyncRequest request, CancellationToken cancellationToken)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var result = await _cacheOperations.ForCache(cacheName).RemoveAsync(RpcMutationContracts.RequireOperationId(request.OperationId), request.Key, cancellationToken)
                                           .ConfigureAwait(false);
        var response = new RemoveAsyncResponse { Removed = result.Removed };
        if (result.Removed)
            response.PreviousValue = ServerProtoEx.CacheValueToGrpcValue(result.Value);

        return response;
    }

    internal async Task<RemoveExpirationAsyncResponse> RemoveExpirationAsyncCoreAsync(RemoveExpirationAsyncRequest request, CancellationToken cancellationToken)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var found = await _cacheOperations.ForCache(cacheName).RemoveExpirationAsync(RpcMutationContracts.RequireOperationId(request.OperationId), request.Key, cancellationToken)
                                          .ConfigureAwait(false);
        return new RemoveExpirationAsyncResponse { Found = found };
    }

    internal async Task<SetAsyncResponse> SetEntryAsyncCoreAsync(SetEntryAsyncRequest request, CancellationToken cancellationToken)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        await _cacheOperations.ForCache(cacheName).SetEntryAsync(
            RpcMutationContracts.RequireOperationId(request.OperationId),
            request.Key,
            await request.Entry.MapFromProtoAsync<T>().ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);
        return new SetAsyncResponse();
    }

    internal async Task<TouchAsyncResponse> TouchAsyncCoreAsync(TouchAsyncRequest request, CancellationToken cancellationToken)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var found = await _cacheOperations.ForCache(cacheName).TouchAsync(
            RpcMutationContracts.RequireOperationId(request.OperationId),
            request.Key,
            request.Expiration.ToTimeSpan(),
            cancellationToken).ConfigureAwait(false);
        return new TouchAsyncResponse { Found = found };
    }

    internal async Task<TryAddAsyncResponse> TryAddEntryAsyncCoreAsync(TryAddEntryAsyncRequest request, CancellationToken cancellationToken)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var added = await _cacheOperations.ForCache(cacheName).TryAddEntryAsync(
            RpcMutationContracts.RequireOperationId(request.OperationId),
            request.Key,
            await request.Entry.MapFromProtoAsync<T>().ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);
        return new TryAddAsyncResponse { Added = added };
    }

    internal async Task<UpdateAsyncResponse> UpdateAsyncCoreAsync(UpdateAsyncRequest request, CancellationToken cancellationToken)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var updated = await _cacheOperations.ForCache(cacheName).UpdateAsync(
            RpcMutationContracts.RequireOperationId(request.OperationId),
            request.Key,
            (await request.Entry.MapFromProtoAsync<T>().ConfigureAwait(false)).Value,
            cancellationToken).ConfigureAwait(false);
        return new UpdateAsyncResponse { Updated = updated };
    }

    private static string RequireCacheName(string cacheName) => string.IsNullOrWhiteSpace(cacheName)
        ? throw new RpcException(new Status(StatusCode.InvalidArgument, "cache_name is required for internal cluster RPCs.")) : cacheName;

    private static void RequireValidCacheKey(string key)
    {
        if (!CacheKeyValidator.TryValidate(key, out _))
            throw ServerOpContract.InvalidCacheKey(key).ToRpcException();
    }

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
