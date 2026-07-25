using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Cluster;

/// <summary>Routes cache operations to the static owner using gRPC on remote peers.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class ClusteredCache<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _local;
    private readonly INodeLocator _locator;
    private readonly OwnerPeerCacheClient _remote;
    private readonly string _selfId;

    internal ClusteredCache(string selfId, ILogicalNamespacedCache<T> local, INodeLocator locator, IServerClientPool clients)
    {
        _selfId = selfId ?? throw new ArgumentNullException(nameof(selfId));
        _local = local ?? throw new ArgumentNullException(nameof(local));
        _locator = locator ?? throw new ArgumentNullException(nameof(locator));
        _remote = new OwnerPeerCacheClient(clients ?? throw new ArgumentNullException(nameof(clients)));
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return string.Equals(owner, _selfId, StringComparison.OrdinalIgnoreCase) ? _local.GetEntryAsync(cacheName, key, cancellationToken)
            : _remote.GetEntryAsync(owner, cacheName, key, cancellationToken);
    }

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return string.Equals(owner, _selfId, StringComparison.OrdinalIgnoreCase) ? _local.GetValueAsync(cacheName, key, cancellationToken)
            : _remote.GetValueAsync(owner, cacheName, key, cancellationToken);
    }

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return string.Equals(owner, _selfId, StringComparison.OrdinalIgnoreCase) ? _local.RemoveAsync(operationId, cacheName, key, cancellationToken)
            : _remote.RemoveAsync(operationId, owner, cacheName, key, cancellationToken);
    }

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return string.Equals(owner, _selfId, StringComparison.OrdinalIgnoreCase) ? _local.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken)
            : _remote.RemoveExpirationAsync(operationId, owner, cacheName, key, cancellationToken);
    }

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return string.Equals(owner, _selfId, StringComparison.OrdinalIgnoreCase) ? _local.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken)
            : _remote.SetEntryAsync(operationId, owner, cacheName, key, entry, cancellationToken);
    }

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return string.Equals(owner, _selfId, StringComparison.OrdinalIgnoreCase) ? _local.TouchAsync(operationId, cacheName, key, expiration, cancellationToken)
            : _remote.TouchAsync(operationId, owner, cacheName, key, expiration, cancellationToken);
    }

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return string.Equals(owner, _selfId, StringComparison.OrdinalIgnoreCase) ? _local.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken)
            : _remote.TryAddEntryAsync(operationId, owner, cacheName, key, entry, cancellationToken);
    }

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return string.Equals(owner, _selfId, StringComparison.OrdinalIgnoreCase) ? _local.UpdateAsync(operationId, cacheName, key, value, cancellationToken)
            : _remote.UpdateAsync(operationId, owner, cacheName, key, value, cancellationToken);
    }

    private string OwnerFor(string cacheName, string key) => _locator.GetOwner(cacheName, key);

    /// <summary>Forwards cache operations to the key owner over inter-node gRPC.</summary>
    private sealed class OwnerPeerCacheClient
    {
        private readonly IServerClientPool _clients;

        internal OwnerPeerCacheClient(IServerClientPool clients)
        {
            _clients = clients ?? throw new ArgumentNullException(nameof(clients));
        }

        internal async ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string owner, string cacheName, string key, CancellationToken cancellationToken)
        {
            var response = await ExecuteOwnerAsync(
                owner,
                (CacheName: cacheName, Key: key),
                static (client, s, ct) => new ValueTask<GetEntryAsyncResponse>(
                    client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = s.CacheName, Key = s.Key }, cancellationToken: ct).ResponseAsync),
                cancellationToken).ConfigureAwait(false);

            return response.Found ? await response.Entry.MapFromProtoAsync<T>().ConfigureAwait(false) : null;
        }

        internal async ValueTask<NodeCacheValueResult<T>> GetValueAsync(string owner, string cacheName, string key, CancellationToken cancellationToken)
        {
            var response = await ExecuteOwnerAsync(
                owner,
                (CacheName: cacheName, Key: key),
                static (client, s, ct) => new ValueTask<GetValueAsyncResponse>(
                    client.GetValueAsync(new GetValueAsyncRequest { CacheName = s.CacheName, Key = s.Key }, cancellationToken: ct).ResponseAsync),
                cancellationToken).ConfigureAwait(false);

            if (response.Found)
                return new NodeCacheValueResult<T>(true, await MapOptionalCacheValueAsync(response.Value).ConfigureAwait(false));
            return new NodeCacheValueResult<T>(false, default);
        }

        internal async ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string owner, string cacheName, string key, CancellationToken cancellationToken)
        {
            var response = await ExecuteOwnerAsync(
                owner,
                (OperationId: operationId, CacheName: cacheName, Key: key),
                static (client, s, ct) => new ValueTask<RemoveAsyncResponse>(
                    client.RemoveAsync(new RemoveAsyncRequest { OperationId = s.OperationId, CacheName = s.CacheName, Key = s.Key }, cancellationToken: ct).ResponseAsync),
                cancellationToken).ConfigureAwait(false);

            if (response.Removed)
                return new CacheRemoveResult<T>(true, await MapOptionalCacheValueAsync(response.PreviousValue).ConfigureAwait(false));
            return new CacheRemoveResult<T>(false, default);
        }

        internal async ValueTask<bool> RemoveExpirationAsync(string operationId, string owner, string cacheName, string key, CancellationToken cancellationToken)
        {
            var response = await ExecuteOwnerAsync(
                owner,
                (OperationId: operationId, CacheName: cacheName, Key: key),
                static (client, s, ct) => new ValueTask<RemoveExpirationAsyncResponse>(
                    client.RemoveExpirationAsync(new RemoveExpirationAsyncRequest { OperationId = s.OperationId, CacheName = s.CacheName, Key = s.Key }, cancellationToken: ct)
                          .ResponseAsync),
                cancellationToken).ConfigureAwait(false);

            return response.Found;
        }

        internal async ValueTask SetEntryAsync(string operationId, string owner, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
        {
            _ = await ExecuteOwnerAsync(
                owner,
                (OperationId: operationId, CacheName: cacheName, Key: key, Entry: entry),
                static (client, s, ct) => new ValueTask<SetAsyncResponse>(
                    client.SetEntryAsync(
                        new SetEntryAsyncRequest { OperationId = s.OperationId, CacheName = s.CacheName, Key = s.Key, Entry = s.Entry.MapToProto() },
                        cancellationToken: ct).ResponseAsync),
                cancellationToken).ConfigureAwait(false);
        }

        internal async ValueTask<bool> TouchAsync(string operationId, string owner, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
        {
            var response = await ExecuteOwnerAsync(
                owner,
                (OperationId: operationId, CacheName: cacheName, Key: key, Expiration: expiration),
                static (client, s, ct) => new ValueTask<TouchAsyncResponse>(
                    client.TouchAsync(
                        new TouchAsyncRequest { OperationId = s.OperationId, CacheName = s.CacheName, Key = s.Key, Expiration = Duration.FromTimeSpan(s.Expiration) },
                        cancellationToken: ct).ResponseAsync),
                cancellationToken).ConfigureAwait(false);

            return response.Found;
        }

        internal async ValueTask<bool> TryAddEntryAsync(
            string operationId,
            string owner,
            string cacheName,
            string key,
            NodeCacheEntry<T> entry,
            CancellationToken cancellationToken)
        {
            var response = await ExecuteOwnerAsync(
                owner,
                (OperationId: operationId, CacheName: cacheName, Key: key, Entry: entry),
                static (client, s, ct) => new ValueTask<TryAddAsyncResponse>(
                    client.TryAddEntryAsync(
                        new TryAddEntryAsyncRequest { OperationId = s.OperationId, CacheName = s.CacheName, Key = s.Key, Entry = s.Entry.MapToProto() },
                        cancellationToken: ct).ResponseAsync),
                cancellationToken).ConfigureAwait(false);

            return response.Added;
        }

        internal async ValueTask<bool> UpdateAsync(string operationId, string owner, string cacheName, string key, T? value, CancellationToken cancellationToken)
        {
            var response = await ExecuteOwnerAsync(
                owner,
                (OperationId: operationId, CacheName: cacheName, Key: key, Value: value),
                static (client, s, ct) =>
                {
                    var nodeCacheEntry = new NodeCacheEntry<T> { Value = s.Value };
                    var request = new UpdateAsyncRequest { OperationId = s.OperationId, CacheName = s.CacheName, Key = s.Key, Entry = nodeCacheEntry.MapToProto() };
                    var responseAsync = client.UpdateAsync(request, cancellationToken: ct).ResponseAsync;
                    return new ValueTask<UpdateAsyncResponse>(responseAsync);
                },
                cancellationToken).ConfigureAwait(false);

            return response.Updated;
        }

        /// <summary>Maps an optional compact <see cref="CacheValue" /> wire field to a typed cache value.</summary>
        /// <param name="value">Optional protobuf value; unset or <see cref="CacheValue.KindOneofCase.None" /> yields <see langword="default" />.</param>
        /// <returns>The decoded cache value, or <see langword="default" /> when <paramref name="value" /> is unset.</returns>
        private static async ValueTask<T?> MapOptionalCacheValueAsync(CacheValue? value)
        {
            if (value is null or { KindCase: CacheValue.KindOneofCase.None })
                return default;

            return await ServerProtoEx.MapCacheValueAsync<T>(value).ConfigureAwait(false);
        }

        private ValueTask<TResponse> ExecuteOwnerAsync<TState, TResponse>(
            string owner,
            TState state,
            Func<SquirixCacheService.SquirixCacheServiceClient, TState, CancellationToken, ValueTask<TResponse>> action,
            CancellationToken cancellationToken)
        {
            var client = _clients.ForNode(owner);
            return _clients.PolicyFor(owner).ExecuteAsync((Client: client, State: state, Action: action), static (s, ct) => s.Action(s.Client, s.State, ct), cancellationToken);
        }
    }
}
