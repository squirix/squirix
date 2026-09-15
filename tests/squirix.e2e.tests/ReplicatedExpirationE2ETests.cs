using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.E2ETests.Cluster;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>End-to-end replicated expiration: expired entries never reappear after failover.</summary>
public sealed class ReplicatedExpirationE2ETests : EndToEndTestBase
{
    /// <summary>Expired entry stays missing after peer loss instead of reappearing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredEntryDoesNotReappear(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        await using var cluster = await HostedCluster.StartTwoNodeAsync(
            new MultiNodeStartOptions { ReplicaCount = 2, TimeProvider = clock },
            nameof(ExpiredEntryDoesNotReappear),
            true,
            cancellationToken);
        var uriA = cluster.GetUri("nodeA");
        var uriB = cluster.GetUri("nodeB");
        var key = KeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "nodeB", "replicated-expiry");

        await using var client = await LoopbackConnect.ConnectAsync(uriA, uriB, cancellationToken);
        var cache = await client.GetCacheAsync<string>("default", cancellationToken);
        await cache.SetAsync(key, "ephemeral", Expiry.In(TimeSpan.FromSeconds(2)), cancellationToken);

        clock.Advance(TimeSpan.FromSeconds(5));
        _ = await Assert.That((await cache.GetValueAsync(key, cancellationToken)).Found).IsFalse();

        await cluster.StopNodeAsync("nodeA");

        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);
        _ = await Assert.That((await cache.GetValueAsync(key, linked.Token)).Found).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync(key, linked.Token)).Found).IsFalse();
    }
}
