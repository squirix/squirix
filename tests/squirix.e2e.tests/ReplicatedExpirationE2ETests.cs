using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.E2ETests.Cluster;
using Xunit;

namespace Squirix.E2ETests;

/// <summary>End-to-end replicated expiration: expired entries never reappear after failover.</summary>
public sealed class ReplicatedExpirationE2ETests : EndToEndTestBase
{
    /// <summary>Expired entry stays missing after peer loss instead of reappearing.</summary>
    [Fact]
    public async Task ExpiredEntryDoesNotReappear()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        await using var cluster = await HostedCluster.StartTwoNodeAsync(
            new MultiNodeStartOptions { ReplicaCount = 2, TimeProvider = clock },
            nameof(ExpiredEntryDoesNotReappear),
            true,
            DefaultCancellationToken);
        var uriA = cluster.GetUri("nodeA");
        var uriB = cluster.GetUri("nodeB");
        var key = KeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "nodeB", "replicated-expiry");

        await using var client = await LoopbackConnect.ConnectAsync(uriA, uriB, DefaultCancellationToken);
        var cache = await client.GetCacheAsync<string>("default", DefaultCancellationToken);
        await cache.SetAsync(key, "ephemeral", Expiry.In(TimeSpan.FromSeconds(2)), DefaultCancellationToken);

        clock.Advance(TimeSpan.FromSeconds(5));
        Assert.False((await cache.GetValueAsync(key, DefaultCancellationToken)).Found);

        await cluster.StopNodeAsync("nodeA");

        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(DefaultCancellationToken, deadline.Token);
        Assert.False((await cache.GetValueAsync(key, linked.Token)).Found);
        Assert.False((await cache.GetValueAsync(key, linked.Token)).Found);
    }
}
