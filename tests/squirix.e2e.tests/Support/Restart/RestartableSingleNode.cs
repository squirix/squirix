using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Support.Client;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2ETests.Support.Restart;

internal sealed class RestartableSingleNode : IAsyncDisposable
{
    private readonly TempDirectory _dataDir;
    private ISquirixClient? _client;
    private TestNodeHost? _host;

    private RestartableSingleNode(TempDirectory dataDir, string address)
    {
        _dataDir = dataDir;
        Address = address;
    }

    private string Address { get; }

    private string DataDir => _dataDir.Path;

    public static async ValueTask<RestartableSingleNode> StartAsync(string testName, CancellationToken cancellationToken)
    {
        var dataDir = new TempDirectory("squirix-e2e-restartable", testName);
        var node = new RestartableSingleNode(dataDir, ListenPortPool.EndToEndTests.NextHttpAddress());
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

    public async ValueTask DisposeAsync()
    {
        await StopNodeAsync().ConfigureAwait(false);
        _dataDir.Dispose();
    }

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
