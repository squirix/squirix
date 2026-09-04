using System;
using System.Buffers;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Errors;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Squirix.Transport.Grpc.Mappers;

namespace Squirix.Server.Adapters.Grpc;

[Immutable]
internal sealed class SquirixServiceAdapter<T> : SquirixCacheService.SquirixCacheServiceBase
{
    private readonly IGrpcCacheOperations<T> _cacheOperations;
    private readonly MutationHandlers _handlers;
    private readonly IRpcMutationIdempotencyCoordinator _idempotency;
    private readonly TimeProvider _timeProvider;

    public SquirixServiceAdapter(
        IGrpcCacheOperations<T> cacheOperations,
        INodeOwnershipResolver ownershipResolver,
        IRemoteInvocationState invocationState,
        IRpcMutationIdempotencyCoordinator idempotency,
        TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(cacheOperations);
        ArgumentNullException.ThrowIfNull(idempotency);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _cacheOperations = cacheOperations;
        _idempotency = idempotency;
        _timeProvider = timeProvider;
        ArgumentNullException.ThrowIfNull(ownershipResolver);
        ArgumentNullException.ThrowIfNull(invocationState);
        _handlers = new MutationHandlers(cacheOperations, ownershipResolver, invocationState);
    }

    public override async Task<GetEntryAsyncResponse> GetEntry(GetEntryAsyncRequest request, ServerCallContext context)
    {
        SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
        var entry = await ApiForRequest(request.CacheName).GetEntryAsync(request.Key, context.CancellationToken).ConfigureAwait(false);
        if (entry == null)
            return new GetEntryAsyncResponse { Found = false };

        return new GetEntryAsyncResponse { Found = true, Entry = entry.MapToProto() };
    }

    public override async Task<GetExpirationAsyncResponse> GetExpiration(GetExpirationAsyncRequest request, ServerCallContext context)
    {
        SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
        var entry = await ApiForRequest(request.CacheName).GetEntryAsync(request.Key, context.CancellationToken).ConfigureAwait(false);
        if (entry == null)
            return new GetExpirationAsyncResponse { Found = false };

        var response = new GetExpirationAsyncResponse { Found = true };
        if (entry.ExpiresUtc is not { } expiresUtc)
            return response;

        response.HasExpiration = true;
        var remaining = expiresUtc - _timeProvider.GetUtcNow().UtcDateTime;
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
        SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
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

    private ICacheApi<T> ApiForRequest(string cacheName) => _cacheOperations.ForCache(SquirixServiceAdapterValidation.RequireCacheName(cacheName));

    /// <summary>Builds deterministic fingerprints for mutating cache RPC requests.</summary>
    private static class RpcMutationFingerprints
    {
        internal static string AddEntryIfAbsent(string cacheName, string key, CacheEntryWire entry) => JoinFingerprint(cacheName, "try-add-entry-async", key, entry);

        internal static string GetOrAdd(string cacheName, string key, CacheEntryWire entry) => JoinFingerprint(cacheName, "get-or-add-async", key, entry);

        internal static string Remove(string cacheName, string key) => JoinFingerprint(cacheName, "remove-async", key);

        internal static string RemoveExpiration(string cacheName, string key) => JoinFingerprint(cacheName, "remove-expiration-async", key);

        internal static string SetEntry(string cacheName, string key, CacheEntryWire entry) => JoinFingerprint(cacheName, "set-entry-async", key, entry);

        internal static string Touch(string cacheName, string key, Duration expiration) => JoinFingerprint(cacheName, "touch-async", key, expiration);

        internal static string Update(string cacheName, string key, CacheEntryWire entry) => JoinFingerprint(cacheName, "update-async", key, entry);

        private static void HashMessage(IMessage message, Span<byte> digest)
        {
            var size = message.CalculateSize();
            if (size <= 512)
            {
                Span<byte> buffer = stackalloc byte[size];
                message.WriteTo(buffer);
                _ = SHA256.HashData(buffer, digest);
                return;
            }

            var rented = ArrayPool<byte>.Shared.Rent(size);
            try
            {
                message.WriteTo(rented.AsSpan(0, size));
                _ = SHA256.HashData(rented.AsSpan(0, size), digest);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        private static string JoinFingerprint(string cacheName, string operation, string key) => string.Create(
            checked(cacheName.Length + operation.Length + key.Length),
            (cacheName, operation, key),
            static (destination, state) =>
            {
                state.cacheName.AsSpan().CopyTo(destination);
                var written = state.cacheName.Length;
                state.operation.AsSpan().CopyTo(destination[written..]);
                written += state.operation.Length;
                state.key.AsSpan().CopyTo(destination[written..]);
            });

        private static string JoinFingerprint(string cacheName, string operation, string key, IMessage message)
        {
            Span<byte> digest = stackalloc byte[32];
            HashMessage(message, digest);

            var length = checked(cacheName.Length + (operation.Length * 2) + key.Length + 64);
            if (length <= 1024)
            {
                Span<char> buffer = stackalloc char[length];
                WriteFingerprint(buffer, cacheName, operation, key, digest);
                return new string(buffer);
            }

            var rented = ArrayPool<char>.Shared.Rent(length);
            try
            {
                var buffer = rented.AsSpan(0, length);
                WriteFingerprint(buffer, cacheName, operation, key, digest);
                return new string(buffer);
            }
            finally
            {
                ArrayPool<char>.Shared.Return(rented);
            }
        }

        private static void WriteFingerprint(Span<char> destination, string cacheName, string operation, string key, ReadOnlySpan<byte> digest)
        {
            cacheName.AsSpan().CopyTo(destination);
            var written = cacheName.Length;
            operation.AsSpan().CopyTo(destination[written..]);
            written += operation.Length;
            key.AsSpan().CopyTo(destination[written..]);
            written += key.Length;
            operation.AsSpan().CopyTo(destination[written..]);
            written += operation.Length;
            HexFormat.WriteSha256HexUpper(destination[written..], digest);
        }
    }

    [Immutable]
    private sealed class MutationHandlers
    {
        private readonly IGrpcCacheOperations<T> _cacheOperations;
        private readonly IRemoteInvocationState _invocationState;
        private readonly INodeOwnershipResolver _ownershipResolver;

        internal MutationHandlers(IGrpcCacheOperations<T> cacheOperations, INodeOwnershipResolver ownershipResolver, IRemoteInvocationState invocationState)
        {
            _cacheOperations = cacheOperations;
            _ownershipResolver = ownershipResolver;
            _invocationState = invocationState;
        }

        internal async Task<GetOrAddAsyncResponse> GetOrAddAsyncCoreAsync(GetOrAddAsyncRequest request, CancellationToken cancellationToken)
        {
            var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
            EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
            var api = _cacheOperations.ForCache(cacheName);
            var existing = await api.GetValueAsync(request.Key, cancellationToken).ConfigureAwait(false);
            if (existing.Found)
            {
                return new GetOrAddAsyncResponse
                {
                    Added = false,
                    Found = true,
                    Value = ServerProtoEx.CacheValueToGrpcValue(existing.Value),
                };
            }

            var entry = await request.Entry.MapFromProtoAsync<T>().ConfigureAwait(false);
            if (await api.TryAddEntryAsync(RpcMutationContracts.RequireOperationId(request.OperationId), request.Key, entry, cancellationToken).ConfigureAwait(false))
            {
                return new GetOrAddAsyncResponse
                {
                    Added = true,
                    Found = true,
                    Value = ServerProtoEx.CacheValueToGrpcValue(entry.Value),
                };
            }

            var afterRace = await api.GetValueAsync(request.Key, cancellationToken).ConfigureAwait(false);
            if (afterRace.Found)
            {
                return new GetOrAddAsyncResponse
                {
                    Added = false,
                    Found = true,
                    Value = ServerProtoEx.CacheValueToGrpcValue(afterRace.Value),
                };
            }

            return new GetOrAddAsyncResponse
            {
                Added = false,
                Found = false,
            };
        }

        internal async Task<RemoveAsyncResponse> RemoveAsyncCoreAsync(RemoveAsyncRequest request, CancellationToken cancellationToken)
        {
            var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
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
            var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
            EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
            var found = await _cacheOperations.ForCache(cacheName)
                                              .RemoveExpirationAsync(RpcMutationContracts.RequireOperationId(request.OperationId), request.Key, cancellationToken)
                                              .ConfigureAwait(false);
            return new RemoveExpirationAsyncResponse { Found = found };
        }

        internal async Task<SetAsyncResponse> SetEntryAsyncCoreAsync(SetEntryAsyncRequest request, CancellationToken cancellationToken)
        {
            var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
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
            var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
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
            var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
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
            var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key);
            EnsureLocalOwnerForInternalOwnerRpc(cacheName, request.Key);
            var updated = await _cacheOperations.ForCache(cacheName).UpdateAsync(
                RpcMutationContracts.RequireOperationId(request.OperationId),
                request.Key,
                (await request.Entry.MapFromProtoAsync<T>().ConfigureAwait(false)).Value,
                cancellationToken).ConfigureAwait(false);
            return new UpdateAsyncResponse { Updated = updated };
        }

        private void EnsureLocalOwnerForInternalOwnerRpc(string cacheName, string key)
        {
            if (!_invocationState.IsInternalOwnerInvocation)
                return;

            var expectedOwner = _ownershipResolver.GetOwner(cacheName, key);
            if (string.Equals(expectedOwner, _ownershipResolver.SelfNodeId, StringComparison.Ordinal))
                return;

            var detail = $"Key is owned by '{expectedOwner}', not current node '{_ownershipResolver.SelfNodeId}'.";
            throw new RpcException(new Status(StatusCode.FailedPrecondition, detail), GrpcStaleOwnerMarkers.CreateStaleOwnerTrailers());
        }
    }
}
