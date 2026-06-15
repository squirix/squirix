using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Support.Client;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2ETests.Support.Restart;

internal sealed class RestartableSingleNode : IAsyncDisposable
{
    private ISquirixClient? _client;
    private TestNodeHost? _host;

    private RestartableSingleNode(string dataDir, string address)
    {
        DataDir = dataDir;
        Address = address;
    }

    private string Address { get; }

    private string DataDir { get; }

    public static async ValueTask<RestartableSingleNode> StartAsync(string testName, CancellationToken cancellationToken)
    {
        var root = PathKit.Combine(Path.GetTempPath(), "squirix-e2e", $"{testName}__{Environment.ProcessId}", "restartable", Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(root);
        var node = new RestartableSingleNode(root, ListenPortPool.EndToEndTests.NextHttpAddress());
        await node.StartNodeAsync(cancellationToken);
        return node;
    }

    public async ValueTask<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken)
    {
        _client ??= await LoopbackConnect.ConnectAsync(Address, cancellationToken);
        return await _client.GetCacheAsync<T>(cacheName, cancellationToken);
    }

    public async ValueTask RestartAsync(CancellationToken cancellationToken)
    {
        await StopNodeAsync();
        await StartNodeAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync() => await StopNodeAsync();

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
