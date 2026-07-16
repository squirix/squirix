using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Errors;
using Squirix.Internal.Cluster.Transport;
using Squirix.Serialization;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Internal;

internal sealed class RemoteCacheRpc<T>
{
    private readonly string _cacheName;
    private readonly IClientPool _clients;
    private readonly EndpointFailover _failover;
    private readonly ISquirixSerializer _serializer;

    internal RemoteCacheRpc(string cacheName, EndpointFailover failover, IClientPool clients, ISquirixSerializer serializer)
    {
        _cacheName = cacheName;
        _failover = failover ?? throw new ArgumentNullException(nameof(failover));
        _clients = clients ?? throw new ArgumentNullException(nameof(clients));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    }

    internal static CacheEntry<T> ToEntry(T? value, CacheEntryOptions? options)
    {
        if (options?.Expiration is not null && options.ExpiresAt is not null)
            throw new ArgumentException("Cache entry options cannot specify both Expiration and ExpiresAt; set at most one expiration mechanism.", nameof(options));

        return new CacheEntry<T>
        {
            Value = value,
            Expiration = options?.Expiration,
            ExpiresUtc = options?.ExpiresAt?.UtcDateTime,
        };
    }

    internal ValueTask<TResult> ExecuteAsync<TState, TResult>(
        Func<SquirixCacheService.SquirixCacheServiceClient, TState, CancellationToken, ValueTask<TResult>> action,
        TState state,
        CancellationToken cancellationToken) => AwaitRpcGuardedAsync(ExecuteCoreAsync(action, state, cancellationToken));

    internal GetOrAddAsyncRequest ToGetOrAddAsyncRequest(string key, CacheEntry<T> entry) => new()
    {
        CacheName = _cacheName,
        Key = key,
        Entry = ProtoEx.MapEntryToProto(entry, _serializer),
    };

    internal SetEntryAsyncRequest ToSetEntryAsyncRequest(string key, CacheEntry<T> entry) => new()
    {
        CacheName = _cacheName,
        Key = key,
        Entry = ProtoEx.MapEntryToProto(entry, _serializer),
    };

    internal TryAddEntryAsyncRequest ToTryAddEntryAsyncRequest(string key, CacheEntry<T> entry) => new()
    {
        CacheName = _cacheName,
        Key = key,
        Entry = ProtoEx.MapEntryToProto(entry, _serializer),
    };

    private static async ValueTask<TResult> AwaitRpcGuardedAsync<TResult>(ValueTask<TResult> task)
    {
        try
        {
            return await task.ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            RemoteRpcErrorMapper.Map(ex);
            throw;
        }
    }

    private ValueTask<TResult> ExecuteCoreAsync<TState, TResult>(
        Func<SquirixCacheService.SquirixCacheServiceClient, TState, CancellationToken, ValueTask<TResult>> action,
        TState state,
        CancellationToken cancellationToken) => _failover.ExecuteAsync(
        static (nodeId, execution, ct) =>
        {
            var client = execution.Cache._clients.ForNode(nodeId);
            return execution.Cache._clients.PolicyFor(nodeId).ExecuteAsync(
                static (policyState, token) => policyState.Action(policyState.Client, policyState.State, token),
                (execution.Action, Client: client, execution.State),
                ct);
        },
        (Cache: this, Action: action, State: state),
        cancellationToken);

    private static class RemoteRpcErrorMapper
    {
        /// <summary>Applies remote RPC error mapping and always throws (never returns normally).</summary>
        /// <param name="ex">The gRPC transport exception from the remote cache pipeline.</param>
        /// <exception cref="OperationIdRequiredException">When the server rejected a missing operation id.</exception>
        /// <exception cref="OperationIdReuseMismatchException">When the server rejected an operation-id reuse mismatch.</exception>
        /// <exception cref="RpcException">When no mapping applies; rethrows <paramref name="ex" /> with preserved stack.</exception>
        [DoesNotReturn]
        internal static void Map(RpcException ex)
        {
            ArgumentNullException.ThrowIfNull(ex);

            if (ex.StatusCode is StatusCode.InvalidArgument && CacheOperationContract.IsOperationIdRequiredMessage(ex.Status.Detail))
                throw new OperationIdRequiredException(ex.Status.Detail, ex);

            if (ex.StatusCode is StatusCode.FailedPrecondition && CacheOperationContractClassifier.IsOperationIdReuseMismatchDetail(ex.Status.Detail))
                throw new OperationIdReuseMismatchException(ex.Status.Detail, ex);

            ExceptionDispatchInfo.Capture(ex).Throw();
        }
    }
}
