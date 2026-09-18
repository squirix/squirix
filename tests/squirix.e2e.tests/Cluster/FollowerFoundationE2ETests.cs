using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
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
        using var identity = new ClusterIdentity();
        using var dir = new TempDirectory("squirix-e2e-follower-foundation");
        var options = new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = dir };
        await using var host = await TestNodeHostFactory.StartNodeAsync(
            "nodeA",
            heldA.HttpUri,
            [("nodeA", heldA.HttpUri), ("nodeB", heldB.HttpUri)],
            options,
            identity,
            cancellationToken);

        _ = await Assert.That(host.HasInterNodeMtlsListener).IsTrue();
    }

    /// <summary>Single persistent node that can be stopped and restarted in the same data directory.</summary>
    private sealed class PersistentSingleNode : IAsyncDisposable
    {
        private readonly TempDirectory _dir;
        private ISquirixClient? _client;
        private TestNodeHost? _host;

        private PersistentSingleNode(TempDirectory dir, Uri uri)
        {
            _dir = dir;
            Uri = uri;
        }

        private string DataDir => _dir;

        private Uri Uri { get; }

        public async ValueTask DisposeAsync()
        {
            await StopNodeAsync().ConfigureAwait(false);
            _dir.Dispose();
        }

        internal static async ValueTask<PersistentSingleNode> StartAsync(string testName, CancellationToken cancellationToken)
        {
            var dir = new TempDirectory("squirix-e2e-follower-foundation", testName);
            var node = new PersistentSingleNode(dir, ListenPortPool.EndToEndTests.HoldHttpUri());
            try
            {
                await node.StartNodeAsync(cancellationToken);
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
            _client ??= await LoopbackConnect.ConnectAsync(Uri, cancellationToken);
            return await _client.GetCacheAsync<T>(cacheName, cancellationToken);
        }

        internal async ValueTask RestartAsync(CancellationToken cancellationToken)
        {
            // The stop must complete before the restart: a canceled wait would leave the previous host
            // shutting down while the new one binds the same URI and data directory.
            await StopNodeAsync();
            cancellationToken.ThrowIfCancellationRequested();
            await StartNodeAsync(cancellationToken);
        }

        private async ValueTask StartNodeAsync(CancellationToken cancellationToken) => _host = await TestNodeHostFactory.StartNodeAsync("nodeA", Uri, DataDir, cancellationToken);

        private async ValueTask StopNodeAsync()
        {
            try
            {
                if (_client != null)
                {
                    await _client.DisposeAsync();
                    _client = null;
                }
            }
            finally
            {
                if (_host != null)
                {
                    await _host.DisposeAsync();
                    _host = null;
                }
            }
        }
    }
}
