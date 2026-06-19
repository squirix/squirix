using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Squirix.Server.Cluster.Reliability;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using RpcEntry = Squirix.Transport.Grpc.Cache.CacheEntryWire;

namespace Squirix.Server.Cluster.Routing;

/// <summary>
/// Forwards cache operations to a remote owner via <see cref="SquirixCacheService" />.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class ClusterRemote<T>
{
    private readonly IClientPool _clients;

    public ClusterRemote(IClientPool clients)
    {
        _clients = clients ?? throw new ArgumentNullException(nameof(clients));
    }

    public async ValueTask<CacheEntry<T>?> GetEntryAsync(string owner, string cacheName, string key, CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        try
        {
            var response = await Policy(owner).ExecuteAsync<GetEntryAsyncResponse>(
                async ct => await client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = cacheName, Key = key }, cancellationToken: ct).ResponseAsync.ConfigureAwait(false),
                cancellationToken).ConfigureAwait(false);

            return await response.Entry.MapFromProtoAsync<T>().ConfigureAwait(false);
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.NotFound)
        {
            return null;
        }
    }

    public async ValueTask<bool> RemoveExpirationAsync(string owner, string cacheName, string key, CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        var operationId = RpcOperationIdentity.New();
        var response = await Policy(owner).ExecuteAsync<RemoveExpirationAsyncResponse>(
            async ct => await client.RemoveExpirationAsync(new RemoveExpirationAsyncRequest { OperationId = operationId, CacheName = cacheName, Key = key }, cancellationToken: ct)
                                    .ResponseAsync.ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);

        return response.Found;
    }

    public async ValueTask SetEntryAsync(string owner, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        var operationId = RpcOperationIdentity.New();
        _ = await Policy(owner).ExecuteAsync<SetAsyncResponse>(
            async ct =>
            {
                var setRequest = new SetEntryAsyncRequest { OperationId = operationId, CacheName = cacheName, Key = key, Entry = entry.MapToProto() };
                return await client.SetEntryAsync(setRequest, cancellationToken: ct).ResponseAsync.ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> TouchAsync(string owner, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        var operationId = RpcOperationIdentity.New();
        var response = await Policy(owner).ExecuteAsync<TouchAsyncResponse>(
            async ct => await client.TouchAsync(
                new TouchAsyncRequest { OperationId = operationId, CacheName = cacheName, Key = key, Expiration = Duration.FromTimeSpan(expiration) },
                cancellationToken: ct).ResponseAsync.ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);

        return response.Found;
    }

    public async ValueTask<bool> TryAddEntryAsync(string owner, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        var operationId = RpcOperationIdentity.New();
        var response = await Policy(owner).ExecuteAsync<TryAddAsyncResponse>(
            async ct => await client.TryAddEntryAsync(
                new TryAddEntryAsyncRequest { OperationId = operationId, CacheName = cacheName, Key = key, Entry = entry.MapToProto() },
                cancellationToken: ct).ResponseAsync.ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);

        return response.Added;
    }

    public async ValueTask<CacheValueResult<T>> GetValueAsync(string owner, string cacheName, string key, CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        var response = await Policy(owner).ExecuteAsync<GetValueAsyncResponse>(
            async ct => await client.GetValueAsync(new GetValueAsyncRequest { CacheName = cacheName, Key = key }, cancellationToken: ct).ResponseAsync.ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);

        return response.Found
            ? new CacheValueResult<T>(true, (await ProtoEx.CacheValueFromGrpcValueAsync<T>(response.Value, null, null).ConfigureAwait(false)).Value)
            : new CacheValueResult<T>(false, default);
    }

    public async ValueTask<CacheRemoveResult<T>> RemoveAsync(string owner, string cacheName, string key, CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        var operationId = RpcOperationIdentity.New();
        var response = await Policy(owner).ExecuteAsync<RemoveAsyncResponse>(
            async ct => await client.RemoveAsync(new RemoveAsyncRequest { OperationId = operationId, CacheName = cacheName, Key = key }, cancellationToken: ct).ResponseAsync
                                    .ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);

        if (!response.Removed)
            return new CacheRemoveResult<T>(false, default);

        var previous = response.PreviousValue is null or { KindCase: CacheValue.KindOneofCase.None }
            ? default
            : (await ProtoEx.CacheValueFromGrpcValueAsync<T>(response.PreviousValue, null, null).ConfigureAwait(false)).Value;
        return new CacheRemoveResult<T>(true, previous);
    }

    public async ValueTask<bool> UpdateAsync(string owner, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        var operationId = RpcOperationIdentity.New();
        var response = await Policy(owner).ExecuteAsync<UpdateAsyncResponse>(
            async ct => await client.UpdateAsync(
                new UpdateAsyncRequest { OperationId = operationId, CacheName = cacheName, Key = key, Entry = new CacheEntry<T> { Value = value }.MapToProto() },
                cancellationToken: ct).ResponseAsync.ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);

        return response.Updated;
    }

    private ICallPolicy Policy(string owner) => _clients.PolicyFor(owner);
}
