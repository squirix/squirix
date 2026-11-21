using System;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Core;
using Squirix.Internal.Cluster.Transport;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;

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

    internal static async Task<RemoteDistributedCache> ConnectAsync(Uri[] endpoints, Func<CancellationToken, ValueTask<string>>? tokenProvider, string cacheName, CancellationToken cancellationToken, SslClientAuthenticationOptions? tlsOptions = null)
    {
        var name = ValidateCacheName(cacheName);
        var session = await RemoteClientSessionFactory.ConnectAsync(endpoints, tokenProvider, null, tlsOptions, cancellationToken).ConfigureAwait(false);
        return session.OpenDistributedCache(name);
    }

    internal static SynchronousDistributedCache CreateSynchronous(Uri endpoint, Func<CancellationToken, ValueTask<string>>? provider, string cacheName, SslClientAuthenticationOptions? tlsOptions) =>
        new(endpoint, provider, cacheName, tlsOptions);

    internal async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken)
    {
        var request = new GetDistributedAsyncRequest { CacheName = _cacheName, Key = key, OperationId = RpcOperationIdentity.New() };
        var reply = await _rpc.ExecuteAsync(
            static (client, wire, options) => new ValueTask<GetDistributedAsyncResponse>(client.GetDistributedAsync(wire, in options)),
            request,
            cancellationToken).ConfigureAwait(false);
        return reply.Found ? reply.TakeValue() : null;
    }

    /// <summary>Sends an owned payload and one normalized policy under a stable operation identity.</summary>
    /// <param name="key">Validated cache key.</param>
    /// <param name="ownedValue">Snapshot that the caller will never mutate.</param>
    /// <param name="expiresAtUnixMs">Canonical absolute cap.</param>
    /// <param name="ttlTicks">Relative cap resolved by the owner, in ticks.</param>
    /// <param name="slidingTicks">Sliding interval in ticks.</param>
    /// <param name="cancellationToken">Caller cancellation token.</param>
    /// <returns>A task that completes when the server confirms the Set.</returns>
    internal async Task SetAsync(string key, byte[] ownedValue, long? expiresAtUnixMs, long? ttlTicks, long? slidingTicks, CancellationToken cancellationToken)
    {
        var request = BinaryClientProtocol.CreateSetRequest(_cacheName, key, ownedValue, expiresAtUnixMs, ttlTicks, slidingTicks);
        _ = await _rpc.ExecuteAsync(
            static (client, wire, options) => new ValueTask<SetAsyncResponse>(client.SetDistributedAsync(wire, in options)),
            request,
            cancellationToken).ConfigureAwait(false);
    }

    internal async Task RefreshAsync(string key, CancellationToken cancellationToken)
    {
        var request = new RefreshDistributedAsyncRequest { CacheName = _cacheName, Key = key, OperationId = RpcOperationIdentity.New() };
        _ = await _rpc.ExecuteAsync(
            static (client, wire, options) => new ValueTask<RefreshDistributedAsyncResponse>(client.RefreshDistributedAsync(wire, in options)),
            request,
            cancellationToken).ConfigureAwait(false);
    }

    internal async Task RemoveAsync(string key, CancellationToken cancellationToken)
    {
        var request = new RemoveAsyncRequest { CacheName = _cacheName, Key = key, OperationId = RpcOperationIdentity.New() };
        _ = await _rpc.ExecuteAsync(
            static (client, wire, options) => new ValueTask<RemoveAsyncResponse>(client.RemoveAsync(wire, in options)),
            request,
            cancellationToken).ConfigureAwait(false);
    }
}
