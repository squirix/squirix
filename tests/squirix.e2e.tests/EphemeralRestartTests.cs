using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Client;
using Squirix.E2ETests.Cluster;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Xunit;

namespace Squirix.E2ETests;

/// <summary>Verifies ephemeral nodes do not restore cache state across restart.</summary>
[Immutable]
public sealed class EphemeralRestartTests : EndToEndTestBase
{
    /// <summary>Ensures a restarted ephemeral node does not restore previously written values.</summary>
    [Fact]
    public async Task RestartShouldNotRestoreValueInEphemeralMode()
    {
        await using var node = await EphemeralRestartableSingleNode.StartAsync(DefaultCancellationToken);
        var cache = await node.GetCacheAsync<string>("ephemeral-restart", DefaultCancellationToken);
        await cache.SetAsync("key", "value", cancellationToken: DefaultCancellationToken);

        await node.RestartAsync(DefaultCancellationToken);

        cache = await node.GetCacheAsync<string>("ephemeral-restart", DefaultCancellationToken);
        var result = await cache.GetValueAsync("key", DefaultCancellationToken);
        Assert.False(result.Found);
    }

    private sealed class EphemeralRestartableSingleNode : IAsyncDisposable
    {
        private ISquirixClient? _client;
        private TestNodeHost? _host;

        private EphemeralRestartableSingleNode(Uri uri)
        {
            Uri = uri;
        }

        private Uri Uri { get; }

        public ValueTask DisposeAsync() => StopNodeAsync();

        internal static async ValueTask<EphemeralRestartableSingleNode> StartAsync(CancellationToken cancellationToken)
        {
            var node = new EphemeralRestartableSingleNode(ListenPortPool.EndToEndTests.NextHttpUri());
            await node.StartNodeAsync(cancellationToken);
            return node;
        }

        internal async ValueTask<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken)
        {
            _client ??= await LoopbackConnect.ConnectAsync(Uri, cancellationToken);
            return await _client.GetCacheAsync<T>(cacheName, cancellationToken);
        }

        internal async ValueTask RestartAsync(CancellationToken cancellationToken)
        {
            await StopNodeAsync();
            await StartNodeAsync(cancellationToken);
        }

        private async ValueTask StartNodeAsync(CancellationToken cancellationToken) => _host = await TestNodeHostFactory.StartNodeAsync("nodeA", Uri, cancellationToken);

        private async ValueTask StopNodeAsync()
        {
            if (_client != null)
            {
                await _client.DisposeAsync();
                _client = null;
            }

            if (_host != null)
            {
                await _host.DisposeAsync();
                _host = null;
            }
        }
    }
}
