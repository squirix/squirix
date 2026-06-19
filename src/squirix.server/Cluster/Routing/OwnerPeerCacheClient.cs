using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Cluster.Routing;

/// <summary>Forwards cache operations to the key owner over inter-node gRPC.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class OwnerPeerCacheClient<T>
{
    private readonly IClientPool _clients;

    public OwnerPeerCacheClient(IClientPool clients)
    {
        _clients = clients ?? throw new ArgumentNullException(nameof(clients));
    }

    public async ValueTask<CacheEntry<T>?> GetEntryAsync(string owner, string cacheName, string key, CancellationToken cancellationToken)
    {
        var response = await ExecuteOwnerAsync(
            owner,
            async (client, ct) =>
            {
                var request = new GetEntryAsyncRequest { CacheName = cacheName, Key = key };
                return await client.GetEntryAsync(request, cancellationToken: ct).ResponseAsync.ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);

        return response.Found ? await response.Entry.MapFromProtoAsync<T>().ConfigureAwait(false) : null;
    }

    public async ValueTask<CacheValueResult<T>> GetValueAsync(string owner, string cacheName, string key, CancellationToken cancellationToken)
    {
        var response = await ExecuteOwnerAsync(
            owner,
            async (client, ct) =>
            {
                var request = new GetValueAsyncRequest { CacheName = cacheName, Key = key };
                return await client.GetValueAsync(request, cancellationToken: ct).ResponseAsync.ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);

        if (response.Found)
            return new CacheValueResult<T>(true, await MapOptionalCacheValueAsync(response.Value).ConfigureAwait(false));
        return new CacheValueResult<T>(false, default);
    }

    public async ValueTask<CacheRemoveResult<T>> RemoveAsync(string owner, string cacheName, string key, CancellationToken cancellationToken)
    {
        var response = await ExecuteOwnerAsync(
            owner,
            async (client, ct) =>
            {
                var request = new RemoveAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = cacheName,
                    Key = key,
                };
                return await client.RemoveAsync(request, cancellationToken: ct).ResponseAsync.ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);

        if (response.Removed)
            return new CacheRemoveResult<T>(true, await MapOptionalCacheValueAsync(response.PreviousValue).ConfigureAwait(false));
        return new CacheRemoveResult<T>(false, default);
    }

    public async ValueTask<bool> RemoveExpirationAsync(string owner, string cacheName, string key, CancellationToken cancellationToken)
    {
        var response = await ExecuteOwnerAsync(
            owner,
            async (client, ct) =>
            {
                var request = new RemoveExpirationAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = cacheName,
                    Key = key,
                };
                return await client.RemoveExpirationAsync(request, cancellationToken: ct).ResponseAsync.ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);

        return response.Found;
    }

    public async ValueTask SetEntryAsync(string owner, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        _ = await ExecuteOwnerAsync(
            owner,
            async (client, ct) =>
            {
                var request = new SetEntryAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = cacheName,
                    Key = key,
                    Entry = entry.MapToProto(),
                };
                return await client.SetEntryAsync(request, cancellationToken: ct).ResponseAsync.ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> TouchAsync(string owner, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        var response = await ExecuteOwnerAsync(
            owner,
            async (client, ct) =>
            {
                var request = new TouchAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = cacheName,
                    Key = key,
                    Expiration = Duration.FromTimeSpan(expiration),
                };
                return await client.TouchAsync(request, cancellationToken: ct).ResponseAsync.ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);

        return response.Found;
    }

    public async ValueTask<bool> TryAddEntryAsync(string owner, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var response = await ExecuteOwnerAsync(
            owner,
            async (client, ct) =>
            {
                var request = new TryAddEntryAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = cacheName,
                    Key = key,
                    Entry = entry.MapToProto(),
                };
                return await client.TryAddEntryAsync(request, cancellationToken: ct).ResponseAsync.ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);

        return response.Added;
    }

    public async ValueTask<bool> UpdateAsync(string owner, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        var response = await ExecuteOwnerAsync(
            owner,
            async (client, ct) =>
            {
                var request = new UpdateAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = cacheName,
                    Key = key,
                    Entry = new CacheEntry<T> { Value = value }.MapToProto(),
                };
                return await client.UpdateAsync(request, cancellationToken: ct).ResponseAsync.ConfigureAwait(false);
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

        return await ProtoEx.MapCacheValueAsync<T>(value).ConfigureAwait(false);
    }

    private ValueTask<TResponse> ExecuteOwnerAsync<TResponse>(
        string owner,
        Func<SquirixCacheService.SquirixCacheServiceClient, CancellationToken, ValueTask<TResponse>> action,
        CancellationToken cancellationToken)
    {
        var client = _clients.ForNode(owner);
        return _clients.PolicyFor(owner).ExecuteAsync(ct => action(client, ct), cancellationToken);
    }
}
