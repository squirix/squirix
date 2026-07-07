using System;
using System.Buffers;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Squirix.Server.Contracts;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Adapters.Grpc;

internal sealed class SquirixServiceAdapter<T> : SquirixCacheService.SquirixCacheServiceBase
{
    private readonly IGrpcCacheOperations<T> _cacheOperations;
    private readonly IRpcMutationIdempotencyCoordinator _idempotency;
    private readonly MutationHandlers<T> _handlers;

    public SquirixServiceAdapter(
        IGrpcCacheOperations<T> cacheOperations,
        INodeOwnershipResolver ownershipResolver,
        IRemoteInvocationState invocationState,
        IRpcMutationIdempotencyCoordinator idempotency)
    {
        _cacheOperations = cacheOperations ?? throw new ArgumentNullException(nameof(cacheOperations));
        _idempotency = idempotency ?? throw new ArgumentNullException(nameof(idempotency));
        _handlers = new MutationHandlers<T>(
            cacheOperations,
            ownershipResolver ?? throw new ArgumentNullException(nameof(ownershipResolver)),
            invocationState ?? throw new ArgumentNullException(nameof(invocationState)));
    }

    public override async Task<GetEntryAsyncResponse> GetEntry(GetEntryAsyncRequest request, ServerCallContext context)
    {
        RequireValidCacheKey(request.Key);
        var entry = await ApiForRequest(request.CacheName).GetEntryAsync(request.Key, context.CancellationToken).ConfigureAwait(false);
        if (entry is null)
            return new GetEntryAsyncResponse { Found = false };

        return new GetEntryAsyncResponse { Found = true, Entry = entry.MapToProto() };
    }

    public override async Task<GetExpirationAsyncResponse> GetExpiration(GetExpirationAsyncRequest request, ServerCallContext context)
    {
        RequireValidCacheKey(request.Key);
        var entry = await ApiForRequest(request.CacheName).GetEntryAsync(request.Key, context.CancellationToken).ConfigureAwait(false);
        if (entry is null)
            return new GetExpirationAsyncResponse { Found = false };

        var response = new GetExpirationAsyncResponse { Found = true };
        if (entry.ExpiresUtc is not { } expiresUtc)
            return response;

        response.HasExpiration = true;
        var remaining = expiresUtc - DateTime.UtcNow;
        response.Remaining = Duration.FromTimeSpan(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
        return response;
    }

    public override Task<GetOrAddAsyncResponse> GetOrAdd(GetOrAddAsyncRequest request, ServerCallContext context) => _idempotency.ExecuteAsync(
        request.OperationId,
        RpcMutationFingerprints.GetOrAdd(request.CacheName, request.Key, request.Entry),
        (Handlers: _handlers, Request: request),
        static (s, ct) => s.Handlers.GetOrAddAsyncCoreAsync(s.Request, ct),
        context.CancellationToken);

    public override async Task<GetValueAsyncResponse> GetValue(GetValueAsyncRequest request, ServerCallContext context)
    {
        RequireValidCacheKey(request.Key);
        var result = await ApiForRequest(request.CacheName).GetValueAsync(request.Key, context.CancellationToken).ConfigureAwait(false);
        var response = new GetValueAsyncResponse { Found = result.Found };
        if (result.Found)
            response.Value = ServerProtoEx.CacheValueToGrpcValue(result.Value);

        return response;
    }

    public override Task<RemoveAsyncResponse> Remove(RemoveAsyncRequest request, ServerCallContext context) => _idempotency.ExecuteAsync(
        request.OperationId,
        RpcMutationFingerprints.Remove(request.CacheName, request.Key),
        (Handlers: _handlers, Request: request),
        static (s, ct) => s.Handlers.RemoveAsyncCoreAsync(s.Request, ct),
        context.CancellationToken);

    public override Task<RemoveExpirationAsyncResponse> RemoveExpiration(RemoveExpirationAsyncRequest request, ServerCallContext context) => _idempotency.ExecuteAsync(
        request.OperationId,
        RpcMutationFingerprints.RemoveExpiration(request.CacheName, request.Key),
        (Handlers: _handlers, Request: request),
        static (s, ct) => s.Handlers.RemoveExpirationAsyncCoreAsync(s.Request, ct),
        context.CancellationToken);

    public override Task<SetAsyncResponse> SetEntry(SetEntryAsyncRequest request, ServerCallContext context) => _idempotency.ExecuteAsync(
        request.OperationId,
        RpcMutationFingerprints.SetEntry(request.CacheName, request.Key, request.Entry),
        (Handlers: _handlers, Request: request),
        static (s, ct) => s.Handlers.SetEntryAsyncCoreAsync(s.Request, ct),
        context.CancellationToken);

    public override Task<TouchAsyncResponse> Touch(TouchAsyncRequest request, ServerCallContext context) => _idempotency.ExecuteAsync(
        request.OperationId,
        RpcMutationFingerprints.Touch(request.CacheName, request.Key, request.Expiration),
        (Handlers: _handlers, Request: request),
        static (s, ct) => s.Handlers.TouchAsyncCoreAsync(s.Request, ct),
        context.CancellationToken);

    public override Task<TryAddAsyncResponse> TryAddEntry(TryAddEntryAsyncRequest request, ServerCallContext context) => _idempotency.ExecuteAsync(
        request.OperationId,
        RpcMutationFingerprints.AddEntryIfAbsent(request.CacheName, request.Key, request.Entry),
        (Handlers: _handlers, Request: request),
        static (s, ct) => s.Handlers.TryAddEntryAsyncCoreAsync(s.Request, ct),
        context.CancellationToken);

    public override Task<UpdateAsyncResponse> Update(UpdateAsyncRequest request, ServerCallContext context) => _idempotency.ExecuteAsync(
        request.OperationId,
        RpcMutationFingerprints.Update(request.CacheName, request.Key, request.Entry),
        (Handlers: _handlers, Request: request),
        static (s, ct) => s.Handlers.UpdateAsyncCoreAsync(s.Request, ct),
        context.CancellationToken);

    private static string RequireCacheName(string cacheName) => string.IsNullOrWhiteSpace(cacheName)
        ? throw new RpcException(new Status(StatusCode.InvalidArgument, "cache_name is required for internal cluster RPCs.")) : cacheName;

    private static void RequireValidCacheKey(string key)
    {
        if (!CacheKeyValidator.TryValidate(key, out _))
            throw ServerOpContract.InvalidCacheKey(key).ToRpcException();
    }

    private ICacheApi<T> ApiForRequest(string cacheName) => _cacheOperations.ForCache(RequireCacheName(cacheName));

    /// <summary>Builds deterministic fingerprints for mutating cache RPC requests.</summary>
    private static class RpcMutationFingerprints
    {
        internal static string Remove(string cacheName, string key) => JoinFingerprint("remove-async", cacheName, key);

        internal static string RemoveExpiration(string cacheName, string key) => JoinFingerprint("remove-expiration-async", cacheName, key);

        internal static string SetEntry(string cacheName, string key, CacheEntryWire entry) => JoinFingerprint("set-entry-async", cacheName, key, HashMessage(entry));

        internal static string Touch(string cacheName, string key, Duration expiration) => JoinFingerprint("touch-async", cacheName, key, HashMessage(expiration));

        internal static string AddEntryIfAbsent(string cacheName, string key, CacheEntryWire entry) => JoinFingerprint("try-add-entry-async", cacheName, key, HashMessage(entry));

        internal static string Update(string cacheName, string key, CacheEntryWire entry) => JoinFingerprint("update-async", cacheName, key, HashMessage(entry));

        internal static string GetOrAdd(string cacheName, string key, CacheEntryWire entry) => JoinFingerprint("get-or-add-async", cacheName, key, HashMessage(entry));

        private static string HashMessage(IMessage message)
        {
            var size = message.CalculateSize();
            var buffer = ArrayPool<byte>.Shared.Rent(size);
            try
            {
                message.WriteTo(buffer.AsSpan(0, size));
                Span<byte> digest = stackalloc byte[32];
                _ = SHA256.HashData(buffer.AsSpan(0, size), digest);
                return HexFormat.FormatSha256HexUpper(digest);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        private static string JoinFingerprint(string separator, params ReadOnlySpan<string?> parts) => string.Join(separator, parts);
    }
}
