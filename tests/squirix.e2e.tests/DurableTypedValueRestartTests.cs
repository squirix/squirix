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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>Integration tests for typed custom values restored through durable restart recovery.</summary>
[Immutable]
public sealed class DurableTypedValueRestartTests : EndToEndTestBase
{
    /// <summary>Verifies RestartRestoresCustomRecordFromJournal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartRestoresCustomRecordFromJournal(CancellationToken cancellationToken)
    {
        await using var node = await RestartableSingleNode.StartAsync(nameof(RestartRestoresCustomRecordFromJournal), cancellationToken);
        var cache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-record", cancellationToken);
        var expected = TypedValueFactory.CreateProfile("journal-record");
        await cache.SetAsync("k", expected, cancellationToken: cancellationToken);
        await node.RestartAsync(cancellationToken);
        var restartedCache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-record", cancellationToken);
        var result = await restartedCache.GetValueAsync("k", cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEqualsAsync(expected, result.Value!);
    }

    /// <summary>Verifies RestartRestoresMutableClassFromJournal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartRestoresMutableClassFromJournal(CancellationToken cancellationToken)
    {
        await using var node = await RestartableSingleNode.StartAsync(nameof(RestartRestoresMutableClassFromJournal), cancellationToken);
        var cache = await node.GetCacheAsync<TypedMutableCart>("typed-durable-cart", cancellationToken);
        var expected = TypedValueFactory.CreateCart("journal-cart");
        await cache.SetAsync("k", expected, cancellationToken: cancellationToken);
        await node.RestartAsync(cancellationToken);
        var restartedCache = await node.GetCacheAsync<TypedMutableCart>("typed-durable-cart", cancellationToken);
        var result = await restartedCache.GetValueAsync("k", cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        await TypedValueAssertions.AssertCartEqualsAsync(expected, result.Value!);
    }

    /// <summary>Verifies RestartSkipsExpiredCustomRecord.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartSkipsExpiredCustomRecord(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        await using var node = await RestartableSingleNode.StartAsync(nameof(RestartSkipsExpiredCustomRecord), clock, cancellationToken);
        var cache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-expired", cancellationToken);
        await cache.SetAsync("k", TypedValueFactory.CreateProfile("expired"), Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);

        // Advance the fake node clock past the TTL so the in-memory entry is deterministically expired before restart.
        clock.Advance(TimeSpan.FromMilliseconds(1800));
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
        await node.RestartAsync(cancellationToken);
        var restartedCache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-expired", cancellationToken);
        _ = await Assert.That((await restartedCache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }

    private sealed class RestartableSingleNode : IAsyncDisposable
    {
        private readonly FakeTimeProvider? _clock;
        private readonly TempDirectory _dir;
        private ISquirixClient? _client;
        private TestNodeHost? _host;

        private RestartableSingleNode(TempDirectory dir, Uri uri, FakeTimeProvider? clock)
        {
            _dir = dir;
            Uri = uri;
            _clock = clock;
        }

        private string DataDir => _dir;

        private Uri Uri { get; }

        public async ValueTask DisposeAsync()
        {
            await StopNodeAsync().ConfigureAwait(false);
            _dir.Dispose();
        }

        internal static ValueTask<RestartableSingleNode> StartAsync(string testName, CancellationToken cancellationToken) => StartAsync(testName, null, cancellationToken);

        internal static async ValueTask<RestartableSingleNode> StartAsync(string testName, FakeTimeProvider? clock, CancellationToken cancellationToken)
        {
            var dir = new TempDirectory("squirix-e2e-restartable", testName);
            var node = new RestartableSingleNode(dir, ListenPortPool.EndToEndTests.HoldHttpUri(), clock);
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
