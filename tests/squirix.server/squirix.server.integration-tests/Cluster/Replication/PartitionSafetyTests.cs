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

/// <summary>Partition safety: the connected majority keeps serving while the minority fails closed.</summary>
public sealed class PartitionSafetyTests : NodeIntegrationTestBase
{
    /// <summary>Majority continues after single loss while fenced minority refuses reads and writes.</summary>
    [Fact]
    public async Task MajorityContinuesAndMinorityFailsClosed()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var uriC = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB), ("node-c", uriC)]);
        var options = new NodeStartOptions { ReplicaCount = 3, UsePersistence = true, ExtraScope = "partition-safety" };

        await using var nodeA = await StartNodeAsync(uriA, peers, options);
        await using var nodeB = await StartNodeAsync(uriB, peers, options);
        await using var nodeC = await StartNodeAsync(uriC, peers, options);

        var cache = nodeA.Services.GetRequiredService<ICacheRuntime>().GetCache<object?>("partition-safety");
        var key = FindKeyOwnedBy(nodeA, "partition-safety", "node-a");
        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), "partition-safety", key, new NodeCacheEntry<object?> { Value = "v" }, DefaultCancellationToken);

        // ReSharper disable once DisposeOnUsingVariable — intentional single loss: the test covers the connected majority keeping service.
        await nodeC.DisposeAsync();

        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), "partition-safety", key, new NodeCacheEntry<object?> { Value = "majority" }, DefaultCancellationToken);
        var majorityRead = await cache.GetValueAsync("partition-safety", key, DefaultCancellationToken);
        Assert.True(majorityRead.Found);

        var minorityWrite = LeaderAuthorityGate.CheckWrite(3, false, true, 1, 1);
        Assert.False(minorityWrite.Allowed);
        Assert.Equal(LeaderAuthorityDenial.MinorityFenced, minorityWrite.Denial);

        var minorityRead = LeaderAuthorityGate.CheckRead(3, false, true, 1, 1, new LeaderReadState(true, 8, 8));
        Assert.False(minorityRead.Allowed);
        Assert.Equal(LeaderAuthorityDenial.MinorityFenced, minorityRead.Denial);

        var majorityWrite = LeaderAuthorityGate.CheckWrite(3, true, true, 1, 1);
        Assert.True(majorityWrite.Allowed);
    }

    private static string FindKeyOwnedBy(TestNodeHost host, string cacheName, string owner)
    {
        var locator = host.Services.GetRequiredService<INodeLocator>();
        for (var i = 0; i < 10_000; i++)
        {
            var candidate = $"partition-safety-{i}";
            if (string.Equals(locator.GetOwner(cacheName, candidate), owner, StringComparison.Ordinal))
                return candidate;
        }

        throw new InvalidOperationException($"No key owned by '{owner}' was found.");
    }
}
