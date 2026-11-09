using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Attributes;
using Squirix.Core;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Internal;

/// <summary>Owns the native session used by the official distributed cache adapter.</summary>
[Immutable]
internal sealed class RemoteDistributedCache : IAsyncDisposable
{
    private readonly RemoteClientSession _session;
    private readonly string _cacheName;
    private readonly RemoteCache<byte[]>.RemoteCacheRpc _rpc;

    internal RemoteDistributedCache(RemoteClientSession session, string cacheName, RemoteCache<byte[]>.RemoteCacheRpc rpc)
    {
        _session = session;
        _cacheName = cacheName;
        _rpc = rpc;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _session.DisposeAsync();

    internal static string ValidateCacheName(string? name) => CacheName.ParsePublic(name).Canonical;

    internal static void ValidateKey(string key) => KeyInputValidator.Validate(key, nameof(key));

    internal static async Task<RemoteDistributedCache> ConnectAsync(Uri[] endpoints, Func<CancellationToken, ValueTask<string>>? tokenProvider, string cacheName, CancellationToken cancellationToken)
    {
        var name = ValidateCacheName(cacheName);
        var session = await RemoteClientSessionFactory.ConnectAsync(endpoints, tokenProvider, null, null, cancellationToken).ConfigureAwait(false);
        return session.OpenDistributedCache(name);
    }

    internal async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken)
    {
        var request = new GetDistributedAsyncRequest { CacheName = _cacheName, Key = key, OperationId = RpcOperationIdentity.New() };
        var reply = await _rpc.ExecuteAsync(
            static (client, wire, options) => new ValueTask<GetDistributedAsyncResponse>(client.GetDistributedAsync(wire, options).ResponseAsync),
            request,
            cancellationToken).ConfigureAwait(false);
        return reply.Found ? reply.Value.ToByteArray() : null;
    }

    /// <summary>Sends an owned payload and one normalized policy under a stable operation identity.</summary>
    /// <param name="key">Validated cache key.</param>
    /// <param name="ownedValue">Snapshot that the caller will never mutate.</param>
    /// <param name="expiresAtUnixMs">Canonical absolute cap.</param>
    /// <param name="slidingTicks">Sliding interval in ticks.</param>
    /// <param name="cancellationToken">Caller cancellation token.</param>
    /// <returns>A task that completes when the server confirms the Set.</returns>
    internal async Task SetAsync(string key, byte[] ownedValue, long? expiresAtUnixMs, long? slidingTicks, CancellationToken cancellationToken)
    {
        var request = new SetDistributedAsyncRequest
        {
            CacheName = _cacheName,
            Key = key,
            OperationId = RpcOperationIdentity.New(),
            Value = UnsafeByteOperations.UnsafeWrap(ownedValue),
        };
        if (expiresAtUnixMs is { } cap)
            request.ExpiresAtUnixMs = cap;
        if (slidingTicks is { } interval)
            request.SlidingTicks = interval;

        _ = await _rpc.ExecuteAsync(
            static (client, wire, options) => new ValueTask<SetAsyncResponse>(client.SetDistributedAsync(wire, options).ResponseAsync),
            request,
            cancellationToken).ConfigureAwait(false);
    }

    internal async Task RefreshAsync(string key, CancellationToken cancellationToken)
    {
        var request = new RefreshDistributedAsyncRequest { CacheName = _cacheName, Key = key, OperationId = RpcOperationIdentity.New() };
        _ = await _rpc.ExecuteAsync(
            static (client, wire, options) => new ValueTask<RefreshDistributedAsyncResponse>(client.RefreshDistributedAsync(wire, options).ResponseAsync),
            request,
            cancellationToken).ConfigureAwait(false);
    }

    internal async Task RemoveAsync(string key, CancellationToken cancellationToken)
    {
        var request = new RemoveAsyncRequest { CacheName = _cacheName, Key = key, OperationId = RpcOperationIdentity.New() };
        _ = await _rpc.ExecuteAsync(
            static (client, wire, options) => new ValueTask<RemoveAsyncResponse>(client.RemoveAsync(wire, options).ResponseAsync),
            request,
            cancellationToken).ConfigureAwait(false);
    }
}
