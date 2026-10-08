using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>A node serves only the replica groups it is a member of.</summary>
[Immutable]
public sealed class ReplicaGroupMembershipTests : ServerUnitTestBase
{
    private static readonly string[] Nodes = ["a", "b", "c", "d", "e"];

    private static readonly byte[] Fingerprint = [9];

    /// <summary>The served groups are exactly the owners whose replica group on the ring contains the node.</summary>
    /// <param name="replicaCount">Replica factor.</param>
    /// <returns>A task that completes when the assertions finish.</returns>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    [Arguments(5)]
    public async Task GroupsServedByMatchesRing(int replicaCount)
    {
        var ring = new PhysicalNodeRing(Nodes);
        var locator = new ReplicaGroupLocator(ring, replicaCount);

        for (var n = 0; n < Nodes.Length; n++)
        {
            var expected = new List<string>();
            var group = new string[replicaCount];
            for (var o = 0; o < Nodes.Length; o++)
            {
                ring.WriteReplicaGroup(Nodes[o], replicaCount, group);
                if (IndexOf(group, Nodes[n]) >= 0)
                    expected.Add(Nodes[o]);
            }

            var served = ReplicaGroupMembership.GroupsServedBy(locator, Nodes, Nodes[n]);

            await SequenceAssert.EqualAsync(expected, served, StringComparer.Ordinal);
            _ = await Assert.That(IndexOf(served, Nodes[n])).IsGreaterThanOrEqualTo(0);
        }
    }

    /// <summary>The replica set of a served group holds exactly its ring members; a group the node does not serve has none.</summary>
    /// <returns>A task that completes when the assertions finish.</returns>
    [Test]
    public async Task MembersFollowRingReplicaSets()
    {
        var ring = new PhysicalNodeRing(Nodes);
        var locator = new ReplicaGroupLocator(ring, 2);
        var served = ReplicaGroupMembership.GroupsServedBy(locator, Nodes, "c");
        var members = new ReplicaMembership(locator, served);
        var group = new string[2];

        for (var o = 0; o < Nodes.Length; o++)
        {
            ring.WriteReplicaGroup(Nodes[o], 2, group);
            var isServed = IndexOf(served, Nodes[o]) >= 0;
            for (var n = 0; n < Nodes.Length; n++)
                _ = await Assert.That(members.IsMember(Nodes[o], Nodes[n])).IsEqualTo(isServed && IndexOf(group, Nodes[n]) >= 0);
        }

        _ = await Assert.That(served.Length).IsLessThan(Nodes.Length);
    }

    /// <summary>The registry opens a log for each member group and none for the other groups.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RegistryOpensOnlyMemberGroups(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-registry-members");
        var locator = new ReplicaGroupLocator(new PhysicalNodeRing(Nodes), 2);
        var served = ReplicaGroupMembership.GroupsServedBy(locator, Nodes, "c");
        await using var registry = new ReplicaGroupRegistry(dir, served, 2, Fingerprint, 1, NullLoggerFactory.Instance);

        await registry.OpenAsync(cancellationToken);

        for (var i = 0; i < Nodes.Length; i++)
        {
            var isMember = IndexOf(served, Nodes[i]) >= 0;
            _ = await Assert.That(registry.TryGetLog(Nodes[i], out _)).IsEqualTo(isMember);
        }

        _ = await Assert.That(served.Length).IsLessThan(Nodes.Length);
    }

    private static int IndexOf(string[] values, string value)
    {
        for (var i = 0; i < values.Length; i++)
        {
            if (string.Equals(values[i], value, StringComparison.Ordinal))
                return i;
        }

        return -1;
    }
}
