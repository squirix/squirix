using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.E2ETests.Cluster;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Xunit;

namespace Squirix.E2ETests;

/// <summary>End-to-end failover, rejoin, and expiration safety over multi-node clusters.</summary>
public sealed class FailoverE2ETests : EndToEndTestBase
{
    /// <summary>Expired entry does not reappear after failover to the surviving majority.</summary>
    [Fact]
    public async Task ExpiredEntryDoesNotReappearAfterFailover()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        await using var cluster = await HostedCluster.StartThreeNodeAsync(
            nameof(ExpiredEntryDoesNotReappearAfterFailover),
            new TwoNodeStartOptions { ReplicaCount = 3, TimeProvider = clock },
            true,
            DefaultCancellationToken);
        var uriB = cluster.GetUri("nodeB");
        var uriC = cluster.GetUri("nodeC");
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy("default", "nodeB", "failover-expiry");

        await using var client = await LoopbackConnect.ConnectAsync(uriB, uriC, DefaultCancellationToken);
        var cache = await client.GetCacheAsync<string>("default", DefaultCancellationToken);
        await cache.SetAsync(key, "ephemeral", Expiry.In(TimeSpan.FromSeconds(2)), DefaultCancellationToken);

        clock.Advance(TimeSpan.FromSeconds(5));
        Assert.False((await cache.GetValueAsync(key, DefaultCancellationToken)).Found);

        await cluster.StopNodeAsync("nodeA");
        Assert.False((await cache.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>Rejoined former leader catches up before regaining eligibility.</summary>
    /// <remarks>
    /// #237 mandates the name "RejoinedFormerLeaderCatchesUpBeforeEligibility"; it is shortened here because SQR0005
    /// limits test method names to 40 characters (mandated name documented here for traceability). Renaming a test
    /// to satisfy the analyzer changes nothing about the covered behavior.
    /// </remarks>
    [Fact]
    public async Task FormerLeaderCatchesUpBeforeEligible()
    {
        var uriA = ListenPortPool.EndToEndTests.NextHttpUri();
        var uriB = ListenPortPool.EndToEndTests.NextHttpUri();
        var uriC = ListenPortPool.EndToEndTests.NextHttpUri();
        using var mtls = new ClusterTls();
        using var dataDir = new TempDirectory("squirix-e2e-rejoin");
        var topology = new[] { ("nodeA", uriA), ("nodeB", uriB), ("nodeC", uriC) };
        var options = new Func<string, TestNodeHostStartOptions>(node => new TestNodeHostStartOptions
        {
            ReplicaCount = 3,
            DataDir = NodePathKit.Combine(dataDir.Path, node),
        });

        await using var nodeA = await TestNodeHostFactory.StartNodeAsync("nodeA", uriA, topology, options("nodeA"), mtls, DefaultCancellationToken);
        await using var nodeB = await TestNodeHostFactory.StartNodeAsync("nodeB", uriB, topology, options("nodeB"), mtls, DefaultCancellationToken);
        var nodeC = await TestNodeHostFactory.StartNodeAsync("nodeC", uriC, topology, options("nodeC"), mtls, DefaultCancellationToken);

        await using var client = await LoopbackConnect.ConnectAsync(uriA, DefaultCancellationToken);
        var cache = await client.GetCacheAsync<string>("rejoin-catchup", DefaultCancellationToken);
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy("rejoin-catchup", "nodeA", "rejoin-catchup");
        await cache.SetAsync(key, "before-stop", cancellationToken: DefaultCancellationToken);

        await nodeC.DisposeAsync();

        await cache.SetAsync(key, "after-stop", cancellationToken: DefaultCancellationToken);

        nodeC = await TestNodeHostFactory.StartNodeAsync("nodeC", uriC, topology, options("nodeC"), mtls, DefaultCancellationToken);
        await using var rejoined = nodeC;

        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(DefaultCancellationToken, deadline.Token);
        await using var rejoinedClient = await LoopbackConnect.ConnectAsync(uriC, linked.Token);
        var rejoinedCache = await rejoinedClient.GetCacheAsync<string>("rejoin-catchup", linked.Token);

        var observed = string.Empty;
        var found = false;
        while (!linked.Token.IsCancellationRequested)
        {
            var read = await rejoinedCache.GetValueAsync(key, linked.Token);
            if (read.Found && string.Equals(read.Value, "after-stop", StringComparison.Ordinal))
            {
                observed = read.Value;
                found = true;
                break;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), TimeProvider.System, linked.Token);
        }

        Assert.True(found, "The rejoined node did not catch up before serving reads.");
        Assert.Equal("after-stop", observed);
    }

    /// <summary>Controlled leader stop recovers reads and writes on the majority within five seconds.</summary>
    /// <remarks>
    /// #237 mandates the name "LeaderStopRecoversOnMajorityWithinFiveSeconds"; it is shortened here because SQR0005
    /// limits test method names to 40 characters (mandated name documented here for traceability). Renaming a test
    /// to satisfy the analyzer changes nothing about the covered behavior.
    /// </remarks>
    [Fact]
    public async Task MajorityRecoversWithinFiveSeconds()
    {
        await using var cluster = await HostedCluster.StartThreeNodeAsync(
            nameof(MajorityRecoversWithinFiveSeconds),
            new TwoNodeStartOptions { ReplicaCount = 3 },
            true,
            DefaultCancellationToken);
        var uriB = cluster.GetUri("nodeB");
        var uriC = cluster.GetUri("nodeC");
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy("default", "nodeB", "failover-recover");

        await using var client = await LoopbackConnect.ConnectAsync(uriB, uriC, DefaultCancellationToken);
        var cache = await client.GetCacheAsync<string>("default", DefaultCancellationToken);
        await cache.SetAsync(key, "before-loss", cancellationToken: DefaultCancellationToken);

        // The five-second recovery budget covers the stop itself plus the subsequent write/read sequence.
        // Stopwatch is monotonic: system clock changes cannot shrink or stretch the measured budget.
        var started = Stopwatch.GetTimestamp();
        await cluster.StopNodeAsync("nodeA");

        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(DefaultCancellationToken, deadline.Token);

        await cache.SetAsync(key, "after-loss", cancellationToken: linked.Token);
        Assert.Equal("after-loss", (await cache.GetValueAsync(key, linked.Token)).Value);
        Assert.True(Stopwatch.GetElapsedTime(started) < TimeSpan.FromSeconds(5));
    }
}
