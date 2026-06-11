using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.AspNetCore;

namespace Squirix.E2ETests.Infrastructure;

internal sealed class EphemeralRestartableSingleNode : IAsyncDisposable
{
    private TestNodeHost? _host;
    private E2EClientHandle? _client;

    private EphemeralRestartableSingleNode(string address) => Address = address;

    private string Address { get; }

    public static async ValueTask<EphemeralRestartableSingleNode> StartAsync(CancellationToken cancellationToken)
    {
        var node = new EphemeralRestartableSingleNode(GetNextHttpUrl());
        await node.StartNodeAsync(cancellationToken).ConfigureAwait(false);
        return node;
    }

    public async ValueTask<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken)
    {
        _client ??= new E2EClientHandle(await E2ETestConnect.ConnectAsync(Address, cancellationToken).ConfigureAwait(false));
        return await _client.GetCacheAsync<T>(cacheName, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask RestartAsync(CancellationToken cancellationToken)
    {
        await StopNodeAsync().ConfigureAwait(false);
        await StartNodeAsync(cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync() => await StopNodeAsync().ConfigureAwait(false);

    private static int AllocatePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private static string GetNextHttpUrl() => $"https://127.0.0.1:{AllocatePort()}";

    private async ValueTask StartNodeAsync(CancellationToken cancellationToken)
    {
        var topology = new[] { ("nodeA", Address) };
        _host = await TestNodeHostFactory.StartNodeAsync("nodeA", Address, topology, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask StopNodeAsync()
    {
        if (_client is not null)
        {
            await _client.DisposeAsync().ConfigureAwait(false);
            _client = null;
        }

        if (_host is not null)
        {
            await _host.DisposeAsync().ConfigureAwait(false);
            _host = null;
        }
    }
}
