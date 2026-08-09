using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Xunit;

namespace Squirix.E2ETests.Cluster;

/// <summary>Closed follower-foundation persistence and activation safety scenarios.</summary>
public sealed class FollowerFoundationE2ETests : EndToEndTestBase
{
    /// <summary>Committed entries remain visible after a restart of the persistent node.</summary>
    [Fact]
    public async Task CommittedEntryRemainsVisibleAfterRestart()
    {
        await using var node = await PersistentSingleNode.StartAsync(nameof(CommittedEntryRemainsVisibleAfterRestart), DefaultCancellationToken);
        var cache = await node.GetCacheAsync<string>("committed-prefix", DefaultCancellationToken);
        await cache.SetAsync("committed", "visible", cancellationToken: DefaultCancellationToken);

        await node.RestartAsync(DefaultCancellationToken);
        var restartedCache = await node.GetCacheAsync<string>("committed-prefix", DefaultCancellationToken);
        var result = await restartedCache.GetValueAsync("committed", DefaultCancellationToken);

        Assert.True(result.Found, "The committed entry was not visible after the restart.");
        Assert.Equal("visible", result.Value);
    }

    /// <summary>A node restart restores committed cache entries and their journal tail records.</summary>
    [Fact]
    public async Task RestartRestoresEntriesAndTail()
    {
        await using var node = await PersistentSingleNode.StartAsync(nameof(RestartRestoresEntriesAndTail), DefaultCancellationToken);
        var cache = await node.GetCacheAsync<string>("snapshot-journal", DefaultCancellationToken);
        await cache.SetAsync("committed", "baseline", cancellationToken: DefaultCancellationToken);
        await cache.SetAsync("tail", "journal", cancellationToken: DefaultCancellationToken);

        await node.RestartAsync(DefaultCancellationToken);
        var restartedCache = await node.GetCacheAsync<string>("snapshot-journal", DefaultCancellationToken);
        var committed = await restartedCache.GetValueAsync("committed", DefaultCancellationToken);
        var tail = await restartedCache.GetValueAsync("tail", DefaultCancellationToken);

        Assert.True(committed.Found, "The committed baseline was not restored after the restart.");
        Assert.Equal("baseline", committed.Value);
        Assert.True(tail.Found, "The journal tail was not restored after the restart.");
        Assert.Equal("journal", tail.Value);
    }

    /// <summary>RF=2 remains rejected even when the closed follower foundation is available.</summary>
    [Fact]
    public async Task RfTwoStillRejectedWithFoundation()
    {
        var uriA = ListenPortPool.EndToEndTests.NextHttpUri();
        var uriB = ListenPortPool.EndToEndTests.NextHttpUri();
        using var mtls = new ClusterTls();
        using var dataDirectory = new TempDirectory("squirix-e2e-follower-foundation");
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestNodeHost>(
            TestNodeHostFactory.StartNodeAsync(
                "nodeA",
                uriA,
                [("nodeA", uriA), ("nodeB", uriB)],
                new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = dataDirectory.Path },
                mtls,
                DefaultCancellationToken));

        Assert.Contains("not activated", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Single persistent node that can be stopped and restarted on the same data directory.</summary>
    private sealed class PersistentSingleNode : IAsyncDisposable
    {
        private readonly TempDirectory _dataDir;
        private ISquirixClient? _client;
        private TestNodeHost? _host;

        private PersistentSingleNode(TempDirectory dataDir, Uri uri)
        {
            _dataDir = dataDir;
            Uri = uri;
        }

        private string DataDir => _dataDir.Path;

        private Uri Uri { get; }

        public async ValueTask DisposeAsync()
        {
            await StopNodeAsync().ConfigureAwait(false);
            _dataDir.Dispose();
        }

        internal static async ValueTask<PersistentSingleNode> StartAsync(string testName, CancellationToken cancellationToken)
        {
            var dataDir = new TempDirectory("squirix-e2e-follower-foundation", testName);
            var node = new PersistentSingleNode(dataDir, ListenPortPool.EndToEndTests.NextHttpUri());
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
            // The stop must complete before the restart: a cancelled wait would leave the previous host
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
