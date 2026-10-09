using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.E2ETests.Fixtures;
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
}
