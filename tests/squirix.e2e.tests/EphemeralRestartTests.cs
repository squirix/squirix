using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Client;
using Squirix.E2ETests.Cluster;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>Verifies ephemeral nodes do not restore cache state across restart.</summary>
[Immutable]
public sealed class EphemeralRestartTests : EndToEndTestBase
{
    /// <summary>Ensures a restarted ephemeral node does not restore previously written values.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EphemeralModeDropsValuesOnRestart(CancellationToken cancellationToken)
    {
        await using var node = await EphemeralRestartableSingleNode.StartAsync(cancellationToken);
        var cache = await node.GetCacheAsync<string>("ephemeral-restart", cancellationToken);
        await cache.SetAsync("key", "value", cancellationToken: cancellationToken);

        await node.RestartAsync(cancellationToken);

        cache = await node.GetCacheAsync<string>("ephemeral-restart", cancellationToken);
        var result = await cache.GetValueAsync("key", cancellationToken);
        _ = await Assert.That(result.Found).IsFalse();
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
            var node = new EphemeralRestartableSingleNode(ListenPortPool.EndToEndTests.HoldHttpUri());
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
