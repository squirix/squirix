using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;
using Squirix.E2ETests.Cluster;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2ETests;

/// <summary>Single persistent node that can be stopped and restarted on the same data directory.</summary>
internal sealed class RestartableNode : IAsyncDisposable
{
    private readonly TempDirectory _dataDir;
    private ISquirixClient? _client;
    private TestNodeHost? _host;

    private RestartableNode(TempDirectory dataDir, Uri uri)
    {
        _dataDir = dataDir;
        Uri = uri;
    }

    /// <summary>Gets the node data directory path.</summary>
    internal string DataDir => _dataDir.Path;

    private Uri Uri { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _dataDir.Dispose();
    }

    /// <summary>Starts a persistent node for the calling test.</summary>
    /// <param name="testName">Test name used for the data directory hint.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The started node.</returns>
    internal static async ValueTask<RestartableNode> StartAsync(string testName, CancellationToken cancellationToken)
    {
        var dataDir = new TempDirectory("squirix-e2e-restartable", testName);
        var node = new RestartableNode(dataDir, ListenPortPool.EndToEndTests.NextHttpUri());
        try
        {
            await node.StartNodeAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await node.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return node;
    }

    /// <summary>Gets a typed cache facade, connecting the client on first use.</summary>
    /// <typeparam name="T">Cached value type.</typeparam>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The cache facade.</returns>
    internal async ValueTask<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken)
    {
        _client ??= await LoopbackConnect.ConnectAsync(Uri, cancellationToken).ConfigureAwait(false);
        return await _client.GetCacheAsync<T>(cacheName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Restarts the node on the same data directory.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the node is running again.</returns>
    internal async ValueTask RestartAsync(CancellationToken cancellationToken)
    {
        await StopAsync().ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        await StartNodeAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Stops the node while keeping the data directory.</summary>
    /// <returns>A task that completes when the node is stopped.</returns>
    internal async ValueTask StopAsync()
    {
        try
        {
            if (_client != null)
            {
                await _client.DisposeAsync().ConfigureAwait(false);
                _client = null;
            }
        }
        finally
        {
            if (_host != null)
            {
                await _host.DisposeAsync().ConfigureAwait(false);
                _host = null;
            }
        }
    }

    private async ValueTask StartNodeAsync(CancellationToken cancellationToken) =>
        _host = await TestNodeHostFactory.StartNodeAsync("nodeA", Uri, DataDir, cancellationToken).ConfigureAwait(false);
}
