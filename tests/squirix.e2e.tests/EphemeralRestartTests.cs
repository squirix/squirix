using System;
using System.Diagnostics.CodeAnalysis;
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
        private readonly TestCluster<ClusterStartOptions> _cluster;
        private ISquirixClient? _client;

        private EphemeralRestartableSingleNode(TestCluster<ClusterStartOptions> cluster)
        {
            _cluster = cluster;
        }

        public async ValueTask DisposeAsync()
        {
            if (_client != null)
            {
                await _client.DisposeAsync();
                _client = null;
            }

            await _cluster.DisposeAsync();
        }

        [SuppressMessage(
            "Reliability",
            "CA2000:Dispose objects before losing scope",
            Justification = "Ownership of the cluster transfers to the returned node, which disposes it.")]
        internal static async ValueTask<EphemeralRestartableSingleNode> StartAsync(CancellationToken cancellationToken)
        {
            var uri = ListenPortPool.EndToEndTests.HoldHttpUri();
            var cluster = TestCluster<ClusterStartOptions>.Create(new ClusterNode("nodeA", uri));
            _ = await cluster.StartNodeAsync("nodeA", cancellationToken: cancellationToken);
            return new EphemeralRestartableSingleNode(cluster);
        }

        internal async ValueTask<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken)
        {
            _client ??= await LoopbackConnect.ConnectAsync(_cluster["nodeA"].Uri, cancellationToken);
            return await _client.GetCacheAsync<T>(cacheName, cancellationToken);
        }

        internal async ValueTask RestartAsync(CancellationToken cancellationToken)
        {
            if (_client != null)
            {
                await _client.DisposeAsync();
                _client = null;
            }

            _ = await _cluster.RestartNodeAsync("nodeA", cancellationToken: cancellationToken);
        }
    }
}
