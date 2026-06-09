using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Squirix.Server.Node.Cluster.Reliability;
using Squirix.Server.Node.Cluster.Transport;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using RpcEntry = Squirix.Transport.Grpc.Cache.Entry;

namespace Squirix.Server.Node.Cluster.Routing;

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

    public async ValueTask<bool> ContainsAsync(string owner, string cacheName, string key, CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        var response = await Policy(owner).ExecuteAsync<ContainsResponse>(
            async ct => await client.ContainsAsync(new ContainsRequest { CacheName = cacheName, Key = key }, cancellationToken: ct).ResponseAsync.ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);

        return response.Exists;
    }

    public async ValueTask<CacheEntry<T>?> GetEntryAsync(string owner, string cacheName, string key, CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        try
        {
            var response = await Policy(owner).ExecuteAsync<GetResponse>(
                async ct => await client.GetAsync(new GetRequest { CacheName = cacheName, Key = key }, cancellationToken: ct).ResponseAsync.ConfigureAwait(false),
                cancellationToken).ConfigureAwait(false);

            return response.Entry.MapFromProto<T>();
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
        {
            return null;
        }
    }

    public async ValueTask<CacheValueResult<T>> TryGetValueAsync(string owner, string cacheName, string key, CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        var response = await Policy(owner).ExecuteAsync<GetValueResponse>(
            async ct => await client.GetValueAsync(new GetValueRequest { CacheName = cacheName, Key = key }, cancellationToken: ct).ResponseAsync.ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);

        return response.Found ? new CacheValueResult<T>(true, ProtoEx.CacheValueFromGrpcValue<T>(response.Value, null, null).Value) : new CacheValueResult<T>(false, default);
    }

    public async ValueTask SetAsync(string owner, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        _ = await Policy(owner).ExecuteAsync<InsertResponse>(
            async ct =>
            {
                var insertRequest = new InsertRequest { CacheName = cacheName, Key = key, Entry = entry.MapToProto() };
                return await client.InsertAsync(insertRequest, cancellationToken: ct).ResponseAsync.ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> RemoveExpirationAsync(string owner, string cacheName, string key, CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        var response = await Policy(owner).ExecuteAsync<RemoveExpirationResponse>(
            async ct => await client.RemoveExpirationAsync(new RemoveExpirationRequest { CacheName = cacheName, Key = key }, cancellationToken: ct).ResponseAsync.ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);

        return response.Found;
    }

    public async ValueTask<bool> RemoveAsync(string owner, string cacheName, string key, CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        var response = await Policy(owner).ExecuteAsync<RemoveResponse>(
            async ct => await client.RemoveAsync(new RemoveRequest { CacheName = cacheName, Key = key }, cancellationToken: ct).ResponseAsync.ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);

        return response.Removed;
    }

    public async ValueTask<bool> TouchAsync(string owner, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        var response = await Policy(owner).ExecuteAsync<TouchResponse>(
            async ct => await client.TouchAsync(new TouchRequest { CacheName = cacheName, Key = key, Expiration = Duration.FromTimeSpan(expiration) }, cancellationToken: ct)
                                    .ResponseAsync.ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);

        return response.Found;
    }

    public async ValueTask<bool> TryAddAsync(string owner, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        var response = await Policy(owner).ExecuteAsync<TryInsertResponse>(
            async ct => await client.TryInsertAsync(new TryInsertRequest { CacheName = cacheName, Key = key, Entry = entry.MapToProto() }, cancellationToken: ct).ResponseAsync
                                    .ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);

        return response.Inserted;
    }

    public async ValueTask<CacheRemoveResult<T>> TryRemoveAsync(string owner, string cacheName, string key, CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        var response = await Policy(owner).ExecuteAsync<RemoveResponse>(
            async ct => await client.RemoveAsync(new RemoveRequest { CacheName = cacheName, Key = key }, cancellationToken: ct).ResponseAsync.ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);

        if (!response.Removed)
            return new CacheRemoveResult<T>(false, default);

        var previous = response.PreviousValue is null ? default : new RpcEntry { Value = response.PreviousValue }.MapFromProto<T>().Value;
        return new CacheRemoveResult<T>(true, previous);
    }

    private ICallPolicy Policy(string owner) => _clients.PolicyFor(owner);
}
