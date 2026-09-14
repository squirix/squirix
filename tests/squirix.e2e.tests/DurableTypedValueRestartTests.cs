using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
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
    /// <summary>Verifies RestartRestoresCustomRecordFromJournal.</summary>
    [Fact]
    public async Task RestartRestoresCustomRecordFromJournal()
    {
        await using var node = await RestartableSingleNode.StartAsync(nameof(RestartRestoresCustomRecordFromJournal), DefaultCancellationToken);
        var cache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-record", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateProfile("journal-record");
        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);
        await node.RestartAsync(DefaultCancellationToken);
        var restartedCache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-record", DefaultCancellationToken);
        var result = await restartedCache.GetValueAsync("k", DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies RestartRestoresMutableClassFromJournal.</summary>
    [Fact]
    public async Task RestartRestoresMutableClassFromJournal()
    {
        await using var node = await RestartableSingleNode.StartAsync(nameof(RestartRestoresMutableClassFromJournal), DefaultCancellationToken);
        var cache = await node.GetCacheAsync<TypedMutableCart>("typed-durable-cart", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateCart("journal-cart");
        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);
        await node.RestartAsync(DefaultCancellationToken);
        var restartedCache = await node.GetCacheAsync<TypedMutableCart>("typed-durable-cart", DefaultCancellationToken);
        var result = await restartedCache.GetValueAsync("k", DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertCartEquals(expected, result.Value!);
    }

    /// <summary>Verifies RestartSkipsExpiredCustomRecord.</summary>
    [Fact]
    public async Task RestartSkipsExpiredCustomRecord()
    {
        var clock = new FakeTimeProvider();
        await using var node = await RestartableSingleNode.StartAsync(nameof(RestartSkipsExpiredCustomRecord), clock, DefaultCancellationToken);
        var cache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-expired", DefaultCancellationToken);
        await cache.SetAsync("k", TypedValueFactory.CreateProfile("expired"), Expiry.In(TimeSpan.FromMilliseconds(500)), DefaultCancellationToken);

        // Advance the fake node clock past the TTL so the in-memory entry is deterministically expired before restart.
        clock.Advance(TimeSpan.FromMilliseconds(1800));
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
        await node.RestartAsync(DefaultCancellationToken);
        var restartedCache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-expired", DefaultCancellationToken);
        Assert.False((await restartedCache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    private sealed class RestartableSingleNode : IAsyncDisposable
    {
        private readonly FakeTimeProvider? _clock;
        private readonly TempDirectory _dataDir;
        private ISquirixClient? _client;
        private TestNodeHost? _host;

        private RestartableSingleNode(TempDirectory dataDir, Uri uri, FakeTimeProvider? clock)
        {
            _dataDir = dataDir;
            Uri = uri;
            _clock = clock;
        }

        private string DataDir => _dataDir.Path;

        private Uri Uri { get; }

        public async ValueTask DisposeAsync()
        {
            await StopNodeAsync().ConfigureAwait(false);
            _dataDir.Dispose();
        }

        internal static ValueTask<RestartableSingleNode> StartAsync(string testName, CancellationToken cancellationToken) => StartAsync(testName, null, cancellationToken);

        internal static async ValueTask<RestartableSingleNode> StartAsync(string testName, FakeTimeProvider? clock, CancellationToken cancellationToken)
        {
            var dataDir = new TempDirectory("squirix-e2e-restartable", testName);
            var node = new RestartableSingleNode(dataDir, ListenPortPool.EndToEndTests.NextHttpUri(), clock);
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

        private async ValueTask StartNodeAsync(CancellationToken cancellationToken) => _host = await TestNodeHostFactory.StartNodeAsync(
            "nodeA",
            Uri,
            [("nodeA", Uri)],
            new TestNodeHostStartOptions { DataDir = DataDir, TimeProvider = _clock },
            null,
            cancellationToken);

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
