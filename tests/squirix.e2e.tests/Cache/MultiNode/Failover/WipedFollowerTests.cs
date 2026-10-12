using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>
/// A follower that restarts on an empty data directory holds unseeded copies: clients keep reading and writing through the other members,
/// and the node itself reports not ready.
/// </summary>
public sealed class WipedFollowerTests : EndToEndTestBase
{
    private const string CacheName = "wiped-follower";

    /// <summary>The group under test, named after its owner; the test finds its leader at run time.</summary>
    private const string Group = "nodeA";

    /// <summary>The follower restarts without its data; the group serves through the survivors, and the restarted node answers not ready with the unseeded text.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public async Task WipedFollowerReportsNotReady(CancellationToken cancellationToken)
    {
        const string testName = nameof(WipedFollowerReportsNotReady);
        await using var cluster = await HostedCluster.StartThreeNodeAsync(testName, FailoverSteps.Options(testName), true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (leader, _) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        var follower = FailoverSteps.Except(FailoverSteps.ThreeNodes, leader)[0];
        var survivors = FailoverSteps.Except(FailoverSteps.ThreeNodes, follower);
        var cache = await cluster.GetCacheAsync<long>(CacheName, survivors[0], cancellationToken);
        var key = FailoverSteps.KeysOf(CacheName, Group, "wiped", 1)[0];
        var attempts = new List<EventualAttempt>();
        await cache.SetAsync(key, 1L, cancellationToken: cancellationToken);

        await cluster.RestartWipedNodeAsync(follower, cancellationToken);
        await Eventually.SucceedsAsync((Cache: cache, Key: key), static (s, token) => s.Cache.SetAsync(s.Key, 2L, cancellationToken: token), FailoverSteps.Bound, attempts, cancellationToken);
        var read = await Eventually.SucceedsAsync((Cache: cache, Key: key), static (s, token) => s.Cache.GetValueAsync(s.Key, token), FailoverSteps.Bound, attempts, cancellationToken);
        var (status, description) = await cluster.Cluster[follower].ReplicaGroupsAsync(cancellationToken);
        using var handler = LoopbackHttp.CreateHandler();
        using var http = new HttpClient(handler, false);
        using var ready = await http.GetAsync(new Uri(cluster.GetUri(follower), "/health/ready"), cancellationToken);

        _ = await Assert.That(read).IsEqualTo(new CacheValueResult<long>(true, 2L)).Because(Eventually.Dump(attempts));
        _ = await Assert.That(status).IsEqualTo(HealthStatus.Unhealthy);
        _ = await Assert.That(description).Contains("is not seeded on this node", StringComparison.Ordinal);
        _ = await Assert.That(ready.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
    }
}
