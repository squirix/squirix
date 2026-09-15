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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>End-to-end failover, rejoin, and expiration safety over multi-node clusters.</summary>
public sealed class FailoverE2ETests : EndToEndTestBase
{
    /// <summary>Expired entry does not reappear after failover to the surviving majority.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredEntryDoesNotReappearAfterFailover(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        await using var cluster = await HostedCluster.StartThreeNodeAsync(
            nameof(ExpiredEntryDoesNotReappearAfterFailover),
            new MultiNodeStartOptions { ReplicaCount = 3, TimeProvider = clock },
            true,
            cancellationToken);
        var uriB = cluster.GetUri("nodeB");
        var uriC = cluster.GetUri("nodeC");
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy("default", "nodeB", "failover-expiry");

        await using var client = await LoopbackConnect.ConnectAsync(uriB, uriC, cancellationToken);
        var cache = await client.GetCacheAsync<string>("default", cancellationToken);
        await cache.SetAsync(key, "ephemeral", Expiry.In(TimeSpan.FromSeconds(2)), cancellationToken);

        clock.Advance(TimeSpan.FromSeconds(5));
        _ = await Assert.That((await cache.GetValueAsync(key, cancellationToken)).Found).IsFalse();

        await cluster.StopNodeAsync("nodeA");
        _ = await Assert.That((await cache.GetValueAsync(key, cancellationToken)).Found).IsFalse();
    }

    /// <summary>Rejoined former leader catches up before regaining eligibility.</summary>
    /// <remarks>
    /// #237 mandates the name "RejoinedFormerLeaderCatchesUpBeforeEligibility"; it is shortened here because SQR0005
    /// limits test method names to 40 characters (mandated name documented here for traceability). Renaming a test
    /// to satisfy the analyzer changes nothing about the covered behavior.
    /// </remarks>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FormerLeaderCatchesUpBeforeEligible(CancellationToken cancellationToken)
    {
        var uriA = ListenPortPool.EndToEndTests.NextHttpUri();
        var uriB = ListenPortPool.EndToEndTests.NextHttpUri();
        var uriC = ListenPortPool.EndToEndTests.NextHttpUri();
        using var mtls = new ClusterTls();
        using var dataDir = new TempDirectory("squirix-e2e-rejoin");
        var topology = new[] { ("nodeA", uriA), ("nodeB", uriB), ("nodeC", uriC) };
        var options = new Func<string, string, TestNodeHostStartOptions>(static (node, dataDirPath) => new TestNodeHostStartOptions
        {
            ReplicaCount = 3,
            DataDir = NodePathKit.Combine(dataDirPath, node),
        });

        await using var nodeA = await TestNodeHostFactory.StartNodeAsync("nodeA", uriA, topology, options("nodeA", dataDir.Path), mtls, cancellationToken);
        await using var nodeB = await TestNodeHostFactory.StartNodeAsync("nodeB", uriB, topology, options("nodeB", dataDir.Path), mtls, cancellationToken);
        var nodeC = await TestNodeHostFactory.StartNodeAsync("nodeC", uriC, topology, options("nodeC", dataDir.Path), mtls, cancellationToken);

        await using var client = await LoopbackConnect.ConnectAsync(uriA, cancellationToken);
        var cache = await client.GetCacheAsync<string>("rejoin-catchup", cancellationToken);
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy("rejoin-catchup", "nodeA", "rejoin-catchup");
        await cache.SetAsync(key, "before-stop", cancellationToken: cancellationToken);

        await nodeC.DisposeAsync();

        await cache.SetAsync(key, "after-stop", cancellationToken: cancellationToken);

        nodeC = await TestNodeHostFactory.StartNodeAsync("nodeC", uriC, topology, options("nodeC", dataDir.Path), mtls, cancellationToken);
        await using var rejoined = nodeC;

        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);
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

        _ = await Assert.That(found).IsTrue().Because("The rejoined node did not catch up before serving reads.");
        _ = await Assert.That(observed).IsEqualTo("after-stop");
    }

    /// <summary>Controlled leader stop recovers reads and writes on the majority within five seconds.</summary>
    /// <remarks>
    /// #237 mandates the name "LeaderStopRecoversOnMajorityWithinFiveSeconds"; it is shortened here because SQR0005
    /// limits test method names to 40 characters (mandated name documented here for traceability). Renaming a test
    /// to satisfy the analyzer changes nothing about the covered behavior.
    /// </remarks>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MajorityRecoversWithinFiveSeconds(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartThreeNodeAsync(
            nameof(MajorityRecoversWithinFiveSeconds),
            new MultiNodeStartOptions { ReplicaCount = 3 },
            true,
            cancellationToken);
        var uriB = cluster.GetUri("nodeB");
        var uriC = cluster.GetUri("nodeC");
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy("default", "nodeB", "failover-recover");

        await using var client = await LoopbackConnect.ConnectAsync(uriB, uriC, cancellationToken);
        var cache = await client.GetCacheAsync<string>("default", cancellationToken);
        await cache.SetAsync(key, "before-loss", cancellationToken: cancellationToken);

        // The five-second recovery budget covers the stop itself plus the subsequent write/read sequence.
        // Stopwatch is monotonic: system clock changes cannot shrink or stretch the measured budget.
        var started = Stopwatch.GetTimestamp();
        await cluster.StopNodeAsync("nodeA");

        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);

        await cache.SetAsync(key, "after-loss", cancellationToken: linked.Token);
        _ = await Assert.That((await cache.GetValueAsync(key, linked.Token)).Value).IsEqualTo("after-loss");
        _ = await Assert.That(Stopwatch.GetElapsedTime(started) < TimeSpan.FromSeconds(5)).IsTrue();
    }
}
