using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Runtime;
using Squirix.Server.TestKit.Hosting;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Leader read authority against live nodes: RF=1 bypasses the gate and RF=2 never promotes after peer loss.</summary>
public sealed class LeaderReadAuthorityTests : NodeIntegrationTestBase
{
    /// <summary>An RF=1 node serves reads and writes without any quorum gate.</summary>
    [Fact]
    public async Task RfOneServesReadsWithoutQuorumGate()
    {
        await using var node = await StartNodeAsync(GetNextHttpUri(), "leader-read-rf1");
        var cache = GetCache(node);
        var key = $"leader-read-rf1:{Guid.NewGuid():N}";

        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), ServerCacheNames.DefaultNamespace, key, BuildEntry("v"), DefaultCancellationToken);
        var read = await cache.GetValueAsync(ServerCacheNames.DefaultNamespace, key, DefaultCancellationToken);
        Assert.True(read.Found);
        Assert.Equal("v", read.Value);

        var gated = LeaderAuthorityGate.CheckRead(1, false, false, 1, 1, new LeaderReadState(false, 0, 7));
        Assert.True(gated.Allowed);
    }

    /// <summary>An RF=2 survivor keeps serving local reads after peer loss without promoting a follower.</summary>
    [Fact]
    public async Task RfTwoKeepsReadsWithoutPromotion()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB)]);
        var options = new NodeStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = "leader-read-rf2" };

        await using var nodeA = await StartNodeAsync(uriA, peers, options);
        await using var nodeB = await StartNodeAsync(uriB, peers, options);

        var cache = nodeA.Services.GetRequiredService<ICacheRuntime>().GetCache<object?>("leader-read");
        var key = FindKeyOwnedBy(nodeA, "leader-read", "node-a");
        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), "leader-read", key, new NodeCacheEntry<object?> { Value = "v" }, DefaultCancellationToken);

        await nodeB.DisposeAsync();

        var read = await cache.GetValueAsync("leader-read", key, DefaultCancellationToken);
        Assert.True(read.Found);

        var minorityWrite = LeaderAuthorityGate.CheckWrite(2, false, true, 1, 1);
        Assert.False(minorityWrite.Allowed);
        Assert.Equal(LeaderAuthorityDenial.MinorityFenced, minorityWrite.Denial);
    }

    private static string FindKeyOwnedBy(TestNodeHost host, string cacheName, string owner)
    {
        var locator = host.Services.GetRequiredService<INodeLocator>();
        for (var i = 0; i < 10_000; i++)
        {
            var candidate = $"leader-read-{i}";
            if (string.Equals(locator.GetOwner(cacheName, candidate), owner, StringComparison.Ordinal))
                return candidate;
        }

        throw new InvalidOperationException($"No key owned by '{owner}' was found.");
    }
}
