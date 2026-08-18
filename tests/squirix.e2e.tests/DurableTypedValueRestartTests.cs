using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Client;
using Squirix.E2ETests.Cluster;
using Squirix.E2ETests.Fixtures.TypedValues;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using Xunit;

namespace Squirix.E2ETests;

/// <summary>Integration tests for typed custom values restored through durable restart recovery.</summary>
[Immutable]
public sealed class DurableTypedValueRestartTests : EndToEndTestBase
{
    /// <summary>Verifies RestartShouldNotRestoreExpiredCustomRecord.</summary>
    [Fact]
    public async Task RestartShouldNotRestoreExpiredCustomRecord()
    {
        await using var node = await RestartableSingleNode.StartAsync(nameof(RestartShouldNotRestoreExpiredCustomRecord), DefaultCancellationToken);
        var cache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-expired", DefaultCancellationToken);

        await cache.SetAsync("k", TypedValueFactory.CreateProfile("expired"), new CacheEntryOptions { Expiration = TimeSpan.FromMilliseconds(100) }, DefaultCancellationToken);

        // Expiration is time-based; wait past the TTL before restart so recovery observes a deterministically expired entry.
        await Task.Delay(TimeSpan.FromMilliseconds(300), TimeProvider.System, DefaultCancellationToken);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);

        await node.RestartAsync(DefaultCancellationToken);
        var restartedCache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-expired", DefaultCancellationToken);

        Assert.False((await restartedCache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies RestartShouldRestoreCustomRecordFromJournal.</summary>
    [Fact]
    public async Task RestartShouldRestoreCustomRecordFromJournal()
    {
        await using var node = await RestartableSingleNode.StartAsync(nameof(RestartShouldRestoreCustomRecordFromJournal), DefaultCancellationToken);
        var cache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-record", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateProfile("journal-record");

        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);

        await node.RestartAsync(DefaultCancellationToken);
        var restartedCache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-record", DefaultCancellationToken);
        var result = await restartedCache.GetValueAsync("k", DefaultCancellationToken);

        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies RestartShouldRestoreMutableClassFromJournal.</summary>
    [Fact]
    public async Task RestartShouldRestoreMutableClassFromJournal()
    {
        await using var node = await RestartableSingleNode.StartAsync(nameof(RestartShouldRestoreMutableClassFromJournal), DefaultCancellationToken);
        var cache = await node.GetCacheAsync<TypedMutableCart>("typed-durable-cart", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateCart("journal-cart");

        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);

        await node.RestartAsync(DefaultCancellationToken);
        var restartedCache = await node.GetCacheAsync<TypedMutableCart>("typed-durable-cart", DefaultCancellationToken);
        var result = await restartedCache.GetValueAsync("k", DefaultCancellationToken);

        Assert.True(result.Found);
        TypedValueAssertions.AssertCartEquals(expected, result.Value!);
    }

    private sealed class RestartableSingleNode : IAsyncDisposable
    {
        private readonly TempDirectory _dataDir;
        private ISquirixClient? _client;
        private TestNodeHost? _host;

        private RestartableSingleNode(TempDirectory dataDir, Uri uri)
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

        internal static async ValueTask<RestartableSingleNode> StartAsync(string testName, CancellationToken cancellationToken)
        {
            var dataDir = new TempDirectory("squirix-e2e-restartable", testName);
            var node = new RestartableSingleNode(dataDir, ListenPortPool.EndToEndTests.NextHttpUri());
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

        private async ValueTask StartNodeAsync(CancellationToken cancellationToken) => _host = await TestNodeHostFactory.StartNodeAsync("nodeA", Uri, DataDir, cancellationToken);

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
