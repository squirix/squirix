using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Squirix.Server.Contracts;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Squirix.Transport.Grpc.Mappers;

namespace Squirix.Server.Adapters.Grpc;

internal sealed class SquirixServiceAdapter<T> : SquirixCacheService.SquirixCacheServiceBase
{
    private readonly IGrpcCacheOperations<T> _cacheOperations;
    private readonly RpcMutationIdempotencyCoordinator _idempotency;
    private readonly IRemoteInvocationState _invocationState;
    private readonly INodeOwnershipResolver _ownershipResolver;

    public SquirixServiceAdapter(
        IGrpcCacheOperations<T> cacheOperations,
        INodeOwnershipResolver ownershipResolver,
        IRemoteInvocationState invocationState,
        RpcMutationIdempotencyCoordinator idempotency)
    {
        _cacheOperations = cacheOperations ?? throw new ArgumentNullException(nameof(cacheOperations));
        _ownershipResolver = ownershipResolver ?? throw new ArgumentNullException(nameof(ownershipResolver));
        _invocationState = invocationState ?? throw new ArgumentNullException(nameof(invocationState));
        _idempotency = idempotency ?? throw new ArgumentNullException(nameof(idempotency));
    }

    public override async Task<GetResponse> Get(GetRequest request, ServerCallContext context)
    {
        RequireValidCacheKey(request.Key);
        var entry = await ApiForRequest(request.CacheName).GetEntryAsync(request.Key, context.CancellationToken).ConfigureAwait(false);
        return entry is null ? throw CacheOperationContract.NotFound().ToRpcException() : new GetResponse { Entry = entry.MapToProto() };
    }

    public override async Task<GetExpirationResponse> GetExpiration(GetExpirationRequest request, ServerCallContext context)
    {
        RequireValidCacheKey(request.Key);
        var entry = await ApiForRequest(request.CacheName).GetEntryAsync(request.Key, context.CancellationToken).ConfigureAwait(false);
        if (entry is null)
            return new GetExpirationResponse { Found = false };

        var response = new GetExpirationResponse { Found = true };
        if (entry.ExpiresUtc is not { } expiresUtc)
            return response;

        response.HasExpiration = true;
        var remaining = expiresUtc - DateTime.UtcNow;
        response.Remaining = Duration.FromTimeSpan(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
        return response;
    }

    public override Task<GetOrAddValueResponse> GetOrAddValue(GetOrAddValueRequest request, ServerCallContext context) => _idempotency.ExecuteAsync(
        request.OperationId,
        RpcMutationFingerprints.GetOrAddValue(request.CacheName, request.Key, request.Value, request.ExpiresUtc, request.Expiration),
        ct => GetOrAddValueCoreAsync(request, ct),
        context.CancellationToken);

    public override async Task<GetValueResponse> GetValue(GetValueRequest request, ServerCallContext context)
    {
        RequireValidCacheKey(request.Key);
        var result = await ApiForRequest(request.CacheName).TryGetValueAsync(request.Key, context.CancellationToken).ConfigureAwait(false);
        var response = new GetValueResponse { Found = result.Found };
        if (result.Found)
            response.Value = ProtoEx.CacheValueToGrpcValue(result.Value);

        return response;
    }

    public override Task<RemoveResponse> Remove(RemoveRequest request, ServerCallContext context) => _idempotency.ExecuteAsync(
        request.OperationId,
        RpcMutationFingerprints.Remove(request.CacheName, request.Key),
        ct => RemoveCoreAsync(request, ct),
        context.CancellationToken);

    public override Task<RemoveExpirationResponse> RemoveExpiration(RemoveExpirationRequest request, ServerCallContext context) => _idempotency.ExecuteAsync(
        request.OperationId,
        RpcMutationFingerprints.RemoveExpiration(request.CacheName, request.Key),
        ct => RemoveExpirationCoreAsync(request, ct),
        context.CancellationToken);

    public override Task<SetResponse> Set(SetRequest request, ServerCallContext context) => _idempotency.ExecuteAsync(
        request.OperationId,
        RpcMutationFingerprints.Set(request.CacheName, request.Key, request.Entry),
        ct => SetCoreAsync(request, ct),
        context.CancellationToken);

    public override Task<SetResponse> SetValue(SetValueRequest request, ServerCallContext context) => _idempotency.ExecuteAsync(
        request.OperationId,
        RpcMutationFingerprints.SetValue(request.CacheName, request.Key, request.Value, request.ExpiresUtc, request.Expiration),
        ct => SetValueCoreAsync(request, ct),
        context.CancellationToken);

    public override Task<TouchResponse> Touch(TouchRequest request, ServerCallContext context) => _idempotency.ExecuteAsync(
        request.OperationId,
        RpcMutationFingerprints.Touch(request.CacheName, request.Key, request.Expiration),
        ct => TouchCoreAsync(request, ct),
        context.CancellationToken);

    public override Task<TrySetResponse> TrySet(TrySetRequest request, ServerCallContext context) => _idempotency.ExecuteAsync(
        request.OperationId,
        RpcMutationFingerprints.TrySet(request.CacheName, request.Key, request.Entry),
        ct => TrySetCoreAsync(request, ct),
        context.CancellationToken);

    public override Task<TrySetResponse> TrySetValue(TrySetValueRequest request, ServerCallContext context) => _idempotency.ExecuteAsync(
        request.OperationId,
        RpcMutationFingerprints.TrySetValue(request.CacheName, request.Key, request.Value, request.ExpiresUtc, request.Expiration),
        ct => TrySetValueCoreAsync(request, ct),
        context.CancellationToken);

    public override Task<UpdateValueResponse> UpdateValue(UpdateValueRequest request, ServerCallContext context) => _idempotency.ExecuteAsync(
        request.OperationId,
        RpcMutationFingerprints.UpdateValue(request.CacheName, request.Key, request.Value),
        ct => UpdateValueCoreAsync(request, ct),
        context.CancellationToken);

    private static string RequireCacheName(string cacheName) => string.IsNullOrWhiteSpace(cacheName)
        ? throw new RpcException(new Status(StatusCode.InvalidArgument, "cache_name is required for internal cluster RPCs.")) : cacheName;

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

    private async Task<GetOrAddValueResponse> GetOrAddValueCoreAsync(GetOrAddValueRequest request, CancellationToken cancellationToken)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var api = _cacheOperations.ForCache(cacheName);
        var existing = await api.TryGetValueAsync(request.Key, cancellationToken).ConfigureAwait(false);
        if (existing.Found)
        {
            return new GetOrAddValueResponse
            {
                Added = false,
                Value = ProtoEx.CacheValueToGrpcValue(existing.Value),
            };
        }

        var entry = await ProtoEx.CacheValueFromGrpcValueAsync<T>(request.Value, request.ExpiresUtc, request.Expiration).ConfigureAwait(false);
        if (await api.TryInsertAsync(request.Key, entry, cancellationToken).ConfigureAwait(false))
        {
            return new GetOrAddValueResponse
            {
                Added = true,
                Value = ProtoEx.CacheValueToGrpcValue(entry.Value),
            };
        }

        var afterRace = await api.TryGetValueAsync(request.Key, cancellationToken).ConfigureAwait(false);
        return new GetOrAddValueResponse
        {
            Added = false,
            Value = ProtoEx.CacheValueToGrpcValue(afterRace.Value),
        };
    }

    private async Task<RemoveResponse> RemoveCoreAsync(RemoveRequest request, CancellationToken cancellationToken)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var result = await _cacheOperations.ForCache(cacheName).TryRemoveAsync(request.Key, cancellationToken).ConfigureAwait(false);
        var response = new RemoveResponse { Removed = result.Removed };
        if (result.Removed)
            response.PreviousValue = ProtoEx.CacheValueToGrpcStruct(result.Value);

        return response;
    }

    private async Task<RemoveExpirationResponse> RemoveExpirationCoreAsync(RemoveExpirationRequest request, CancellationToken cancellationToken)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var found = await _cacheOperations.ForCache(cacheName).RemoveExpirationAsync(request.Key, cancellationToken).ConfigureAwait(false);
        return new RemoveExpirationResponse { Found = found };
    }

    private async Task<SetResponse> SetCoreAsync(SetRequest request, CancellationToken cancellationToken)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        await _cacheOperations.ForCache(cacheName).InsertAsync(request.Key, await request.Entry.MapFromProtoAsync<T>().ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
        return new SetResponse();
    }

    private async Task<SetResponse> SetValueCoreAsync(SetValueRequest request, CancellationToken cancellationToken)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        await _cacheOperations.ForCache(cacheName).InsertAsync(
            request.Key,
            await ProtoEx.CacheValueFromGrpcValueAsync<T>(request.Value, request.ExpiresUtc, request.Expiration).ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);
        return new SetResponse();
    }

    private async Task<TouchResponse> TouchCoreAsync(TouchRequest request, CancellationToken cancellationToken)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var found = await _cacheOperations.ForCache(cacheName).TouchAsync(request.Key, request.Expiration.ToTimeSpan(), cancellationToken).ConfigureAwait(false);
        return new TouchResponse { Found = found };
    }

    private async Task<TrySetResponse> TrySetCoreAsync(TrySetRequest request, CancellationToken cancellationToken)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var added = await _cacheOperations.ForCache(cacheName).TryInsertAsync(request.Key, await request.Entry.MapFromProtoAsync<T>().ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
        return new TrySetResponse { Added = added };
    }

    private async Task<TrySetResponse> TrySetValueCoreAsync(TrySetValueRequest request, CancellationToken cancellationToken)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var added = await _cacheOperations.ForCache(cacheName).TryInsertAsync(
            request.Key,
            await ProtoEx.CacheValueFromGrpcValueAsync<T>(request.Value, request.ExpiresUtc, request.Expiration).ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);
        return new TrySetResponse { Added = added };
    }

    private async Task<UpdateValueResponse> UpdateValueCoreAsync(UpdateValueRequest request, CancellationToken cancellationToken)
    {
        var cacheName = RequireCacheName(request.CacheName);
        RequireValidCacheKey(request.Key);
        EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
        var updated = await _cacheOperations.ForCache(cacheName).UpdateAsync(
            request.Key,
            (await ProtoEx.CacheValueFromGrpcValueAsync<T>(request.Value, null, null).ConfigureAwait(false)).Value,
            cancellationToken).ConfigureAwait(false);
        return new UpdateValueResponse { Updated = updated };
    }
}
