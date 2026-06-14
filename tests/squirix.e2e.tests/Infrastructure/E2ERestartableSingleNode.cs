using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.AspNetCore;
using Squirix.Server.TestKit.IO;

namespace Squirix.E2ETests.Infrastructure;

internal sealed class E2ERestartableSingleNode : IAsyncDisposable
{
    private E2EClientHandle? _client;
    private TestNodeHost? _host;

    private E2ERestartableSingleNode(string dataDir, string address)
    {
        DataDir = dataDir;
        Address = address;
    }

    private string Address { get; }

    private string DataDir { get; }

    public static async ValueTask<E2ERestartableSingleNode> StartAsync(string testName, CancellationToken cancellationToken)
    {
        var root = PathKit.Combine(Path.GetTempPath(), "squirix-e2e", $"{testName}__{Environment.ProcessId}", "restartable", Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(root);
        var node = new E2ERestartableSingleNode(root, GetNextHttpUrl());
        await node.StartNodeAsync(cancellationToken);
        return node;
    }

    public async ValueTask<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken)
    {
        _client ??= new E2EClientHandle(await E2ETestConnect.ConnectAsync(Address, cancellationToken));
        return await _client.GetCacheAsync<T>(cacheName, cancellationToken);
    }

    public async ValueTask RestartAsync(CancellationToken cancellationToken)
    {
        await StopNodeAsync();
        await StartNodeAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync() => await StopNodeAsync();

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
        _host = await TestNodeHostFactory.StartNodeAsync("nodeA", Address, topology, DataDir, cancellationToken);
    }

    private async ValueTask StopNodeAsync()
    {
        if (_client is not null)
        {
            await _client.DisposeAsync();
            _client = null;
        }

        if (_host is not null)
        {
            await _host.DisposeAsync();
            _host = null;
        }
    }
}
