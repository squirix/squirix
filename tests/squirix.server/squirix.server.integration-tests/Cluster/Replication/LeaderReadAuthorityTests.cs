using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Runtime;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Leader read authority against live nodes: RF=1 bypasses the gate and RF=2 never promotes after peer loss.</summary>
public sealed class LeaderReadAuthorityTests : NodeIntegrationTestBase
{
    /// <summary>An RF=1 node serves reads and writes without any quorum gate.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfOneServesReadsWithoutQuorumGate(CancellationToken cancellationToken)
    {
        await using var node = await StartNodeAsync(GetNextHttpUri(), "leader-read-rf1", cancellationToken: cancellationToken);
        var cache = GetCache(node);
        var key = $"leader-read-rf1:{Guid.NewGuid():N}";

        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), ServerCacheNames.DefaultNamespace, key, BuildEntry("v"), cancellationToken);
        var read = await cache.GetValueAsync(ServerCacheNames.DefaultNamespace, key, cancellationToken);
        _ = await Assert.That(read.Found).IsTrue();
        _ = await Assert.That(read.Value).IsEqualTo("v");

        var gated = LeaderAuthorityGate.CheckRead(1, false, false, 1, 1, new LeaderReadState(false, 0, 7));
        _ = await Assert.That(gated.Allowed).IsTrue();
    }

    /// <summary>An RF=2 survivor keeps serving local reads after peer loss without promoting a follower.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfTwoKeepsReadsWithoutPromotion(CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB)]);
        var options = new NodeStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = "leader-read-rf2" };

        await using var nodeA = await StartNodeAsync(uriA, peers, options, cancellationToken);
        await using var nodeB = await StartNodeAsync(uriB, peers, options, cancellationToken);

        var cache = nodeA.Services.GetRequiredService<ICacheRuntime>().GetCache<object?>("leader-read");
        var key = FindKeyOwnedBy(nodeA, "leader-read", "node-a");
        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), "leader-read", key, new NodeCacheEntry<object?> { Value = "v" }, cancellationToken);

        // ReSharper disable once DisposeOnUsingVariable — intentional peer loss: the test covers the RF=2 survivor serving local reads.
        await nodeB.DisposeAsync();

        var read = await cache.GetValueAsync("leader-read", key, cancellationToken);
        _ = await Assert.That(read.Found).IsTrue();

        var minorityWrite = LeaderAuthorityGate.CheckWrite(2, false, true, 1, 1);
        _ = await Assert.That(minorityWrite.Allowed).IsFalse();
        _ = await Assert.That(minorityWrite.Denial).IsEqualTo(LeaderAuthorityDenial.MinorityFenced);
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
