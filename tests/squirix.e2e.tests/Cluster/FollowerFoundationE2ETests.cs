using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cluster;

/// <summary>Closed follower-foundation persistence and activation safety scenarios.</summary>
public sealed class FollowerFoundationE2ETests : EndToEndTestBase
{
    /// <summary>Committed entries remain visible after a restart of the persistent node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommittedEntryRemainsVisibleAfterRestart(CancellationToken cancellationToken)
    {
        await using var node = await PersistentSingleNode.StartAsync(nameof(CommittedEntryRemainsVisibleAfterRestart), cancellationToken);
        var cache = await node.GetCacheAsync<string>("committed-prefix", cancellationToken);
        await cache.SetAsync("committed", "visible", cancellationToken: cancellationToken);

        await node.RestartAsync(cancellationToken);
        var restartedCache = await node.GetCacheAsync<string>("committed-prefix", cancellationToken);
        var result = await restartedCache.GetValueAsync("committed", cancellationToken);

        _ = await Assert.That(result.Found).IsTrue().Because("The committed entry was not visible after the restart.");
        _ = await Assert.That(result.Value).IsEqualTo("visible");
    }

    /// <summary>A node restart restores committed cache entries and their journal tail records.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartRestoresEntriesAndTail(CancellationToken cancellationToken)
    {
        await using var node = await PersistentSingleNode.StartAsync(nameof(RestartRestoresEntriesAndTail), cancellationToken);
        var cache = await node.GetCacheAsync<string>("snapshot-journal", cancellationToken);
        await cache.SetAsync("committed", "baseline", cancellationToken: cancellationToken);
        await cache.SetAsync("tail", "journal", cancellationToken: cancellationToken);

        await node.RestartAsync(cancellationToken);
        var restartedCache = await node.GetCacheAsync<string>("snapshot-journal", cancellationToken);
        var committed = await restartedCache.GetValueAsync("committed", cancellationToken);
        var tail = await restartedCache.GetValueAsync("tail", cancellationToken);

        _ = await Assert.That(committed.Found).IsTrue().Because("The committed baseline was not restored after the restart.");
        _ = await Assert.That(committed.Value).IsEqualTo("baseline");
        _ = await Assert.That(tail.Found).IsTrue().Because("The journal tail was not restored after the restart.");
        _ = await Assert.That(tail.Value).IsEqualTo("journal");
    }

    /// <summary>RF=2 starts when the closed follower foundation is available with prerequisites.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfTwoStartsWithFoundation(CancellationToken cancellationToken)
    {
        using var heldA = ListenPortPool.EndToEndTests.HoldPort();
        using var heldB = ListenPortPool.EndToEndTests.HoldPort();
        using var directory = new TempDirectory("squirix-e2e-follower-foundation");
        ClusterNode[] topology = [new("nodeA", heldA.HttpUri), new("nodeB", heldB.HttpUri)];
        await using var cluster = TestCluster<ClusterStartOptions>.Create(topology);
        var host = await cluster.StartNodeAsync("nodeA", new ClusterStartOptions { ReplicaCount = 2, DataDir = directory.Path }, cancellationToken);

        _ = await Assert.That(host.HasInterNodeMtlsListener).IsTrue();
    }

    /// <summary>Single persistent node that can be stopped and restarted in the same data directory.</summary>
    private sealed class PersistentSingleNode : IAsyncDisposable
    {
        private readonly TestCluster<ClusterStartOptions> _cluster;
        private readonly TempDirectory _dataDir;
        private ISquirixClient? _client;

        private PersistentSingleNode(TestCluster<ClusterStartOptions> cluster, TempDirectory dataDir)
        {
            _cluster = cluster;
            _dataDir = dataDir;
        }

        public async ValueTask DisposeAsync()
        {
            await StopNodeAsync().ConfigureAwait(false);
            await _cluster.DisposeAsync().ConfigureAwait(false);
            _dataDir.Dispose();
        }

        [SuppressMessage(
            "Reliability",
            "CA2000:Dispose objects before losing scope",
            Justification = "Ownership of the data directory transfers to the returned node, which disposes it.")]
        internal static async ValueTask<PersistentSingleNode> StartAsync(string testName, CancellationToken cancellationToken)
        {
            var dataDir = new TempDirectory("squirix-e2e-follower-foundation", testName);
            var uri = ListenPortPool.EndToEndTests.HoldHttpUri();
            var cluster = TestCluster<ClusterStartOptions>.Create(new ClusterNode("nodeA", uri));
            var node = new PersistentSingleNode(cluster, dataDir);
            try
            {
                _ = await node.StartNodeAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // The caller never receives the instance on a failed start, so the temp directory would leak.
                await node.DisposeAsync().ConfigureAwait(false);
                throw;
            }

            return node;
        }

        internal async ValueTask<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken)
        {
            _client ??= await LoopbackConnect.ConnectAsync(_cluster["nodeA"].Uri, cancellationToken);
            return await _client.GetCacheAsync<T>(cacheName, cancellationToken);
        }

        internal async ValueTask RestartAsync(CancellationToken cancellationToken)
        {
            // The stop must complete before the restart: a canceled wait would leave the previous host
            // shutting down while the new one binds the same URI and data directory.
            await StopNodeAsync().ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            _ = await StartNodeAsync(cancellationToken).ConfigureAwait(false);
        }

        private ValueTask<ITestNodeHost> StartNodeAsync(CancellationToken cancellationToken)
        {
            var options = new ClusterStartOptions { DataDir = _dataDir.Path };
            return _cluster.StartNodeAsync("nodeA", options, cancellationToken);
        }

        private async ValueTask StopNodeAsync()
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
                await _cluster.StopNodeAsync("nodeA").ConfigureAwait(false);
            }
        }
    }
}
