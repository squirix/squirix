using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Partition safety: the connected majority keeps serving while the minority fails closed.</summary>
public sealed class PartitionSafetyTests : NodeIntegrationTestBase
{
    /// <summary>Majority continues after single loss while fenced minority refuses reads and writes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MajorityContinuesAndMinorityFailsClosed(CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions { ReplicaCount = 3, UsePersistence = true, ExtraScope = "partition-safety" };

        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", options, cancellationToken);
        var nodeA = cluster["node-a"];

        var cache = nodeA.GetCache<object?>("partition-safety");
        var key = nodeA.FindKeyOwnedBy("partition-safety", "node-a");
        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), "partition-safety", key, new NodeCacheEntry<object?> { Value = "v" }, cancellationToken);

        await cluster.StopNodeAsync("node-c");

        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), "partition-safety", key, new NodeCacheEntry<object?> { Value = "majority" }, cancellationToken);
        var majorityRead = await cache.GetValueAsync("partition-safety", key, cancellationToken);
        _ = await Assert.That(majorityRead.Found).IsTrue();

        var minorityWrite = LeaderAuthorityGate.CheckWrite(3, false, true, 1, 1);
        _ = await Assert.That(minorityWrite.Allowed).IsFalse();
        _ = await Assert.That(minorityWrite.Denial).IsEqualTo(LeaderAuthorityDenial.MinorityFenced);

        var minorityRead = LeaderAuthorityGate.CheckRead(3, false, true, 1, 1, new LeaderReadState(true, 8, 8));
        _ = await Assert.That(minorityRead.Allowed).IsFalse();
        _ = await Assert.That(minorityRead.Denial).IsEqualTo(LeaderAuthorityDenial.MinorityFenced);

        var majorityWrite = LeaderAuthorityGate.CheckWrite(3, true, true, 1, 1);
        _ = await Assert.That(majorityWrite.Allowed).IsTrue();
    }
}
