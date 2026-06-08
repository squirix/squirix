using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.AspNetCore;

namespace Squirix.E2ETests.Infrastructure;

internal sealed class E2ERestartableSingleNode : IAsyncDisposable
{
    private TestNodeHost? _host;
    private E2EClientHandle? _client;

    private E2ERestartableSingleNode(string dataDir, string address)
    {
        DataDir = dataDir;
        Address = address;
    }

    private string Address { get; }

    private string DataDir { get; }

    public static async ValueTask<E2ERestartableSingleNode> StartAsync(string testName, CancellationToken cancellationToken)
    {
        var root = Path.Combine(Path.GetTempPath(), "squirix-e2e", $"{testName}__{Environment.ProcessId}", "restartable", Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(root);
        var node = new E2ERestartableSingleNode(root, GetNextHttpUrl());
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
        _host = await TestNodeHostFactory.StartNodeAsync("nodeA", Address, topology, DataDir, cancellationToken).ConfigureAwait(false);
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
