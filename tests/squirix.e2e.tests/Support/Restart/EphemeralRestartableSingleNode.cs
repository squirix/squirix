using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Support.Client;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2ETests.Support.Restart;

internal sealed class EphemeralRestartableSingleNode : IAsyncDisposable
{
    private ISquirixClient? _client;
    private TestNodeHost? _host;

    private EphemeralRestartableSingleNode(Uri uri)
    {
        Uri = uri;
    }

    private Uri Uri { get; }

    public static async ValueTask<EphemeralRestartableSingleNode> StartAsync(CancellationToken cancellationToken)
    {
        var node = new EphemeralRestartableSingleNode(ListenPortPool.EndToEndTests.NextHttpUri());
        await node.StartNodeAsync(cancellationToken);
        return node;
    }

    public async ValueTask<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken)
    {
        _client ??= await LoopbackConnect.ConnectAsync(Uri, cancellationToken);
        return await _client.GetCacheAsync<T>(cacheName, cancellationToken);
    }

    public async ValueTask RestartAsync(CancellationToken cancellationToken)
    {
        await StopNodeAsync();
        await StartNodeAsync(cancellationToken);
    }

    public ValueTask DisposeAsync() => StopNodeAsync();

    private async ValueTask StartNodeAsync(CancellationToken cancellationToken)
    {
        var topology = new[] { ("nodeA", Uri) };
        _host = await TestNodeHostFactory.StartNodeAsync("nodeA", Uri, topology, cancellationToken);
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
