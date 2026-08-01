using System;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Property-style placement checks for the physical replica ring.</summary>
public sealed class ReplicaPlacementPropertyTests
{
    /// <summary>Vnode ownership remains a single original owner string.</summary>
    [Fact]
    public void VnodeRingOnlySelectsOriginalOwner()
    {
        var nodes = new[] { "node-a", "node-b", "node-c", "node-d" };
        var locator = RuntimeServiceRegistration.CreateHashLocator(nodes);
        for (var i = 0; i < 2_000; i++)
        {
            var owner = locator.GetOwner("cache", "k" + i.ToString(System.Globalization.CultureInfo.InvariantCulture));
            Assert.Contains(owner, nodes, StringComparer.Ordinal);
            Assert.DoesNotContain(',', owner);
        }
    }

    /// <summary>All keys owned by the same original owner share one ordered replica group.</summary>
    [Fact]
    public void AllRangesForOwnerShareOrderedReplicaGroup()
    {
        var nodes = new[] { "node-a", "node-b", "node-c", "node-d" };
        var locator = RuntimeServiceRegistration.CreateHashLocator(nodes);
        var groupLocator = new ReplicaGroupLocator(new PhysicalNodeRing(nodes), 3);
        string? expected0 = null;
        string? expected1 = null;
        string? expected2 = null;
        var matched = 0;
        var group = new string[3];
        for (var i = 0; i < 20_000 && matched < 40; i++)
        {
            var key = "k" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
            var owner = locator.GetOwner("cache", key);
            if (!string.Equals(owner, "node-a", StringComparison.Ordinal))
                continue;

            groupLocator.GetReplicaGroup(owner, group);
            if (expected0 is null)
            {
                expected0 = group[0];
                expected1 = group[1];
                expected2 = group[2];
            }
            else
            {
                Assert.Equal(expected0, group[0]);
                Assert.Equal(expected1, group[1]);
                Assert.Equal(expected2, group[2]);
            }

            matched++;
        }

        Assert.True(matched >= 10, "Expected enough keys owned by node-a.");
    }

    /// <summary>Followers are the next distinct physical nodes after the owner.</summary>
    [Fact]
    public void ReturnsNextDistinctPhysicalNodes()
    {
        var ring = new PhysicalNodeRing(["node-d", "node-b", "node-a", "node-c"]);
        var group = new string[3];
        ring.WriteReplicaGroup("node-a", 3, group);
        Assert.Equal("node-a", group[0]);
        Assert.Equal("node-b", group[1]);
        Assert.Equal("node-c", group[2]);
    }

    /// <summary>Physical selection wraps at the end of the ordinal ring.</summary>
    [Fact]
    public void WrapsAtPhysicalRingEnd()
    {
        var ring = new PhysicalNodeRing(["node-a", "node-b", "node-c", "node-d"]);
        var group = new string[3];
        ring.WriteReplicaGroup("node-c", 3, group);
        Assert.Equal("node-c", group[0]);
        Assert.Equal("node-d", group[1]);
        Assert.Equal("node-a", group[2]);
        ring.WriteReplicaGroup("node-d", 3, group);
        Assert.Equal("node-d", group[0]);
        Assert.Equal("node-a", group[1]);
        Assert.Equal("node-b", group[2]);
    }

    /// <summary>Peer list permutation does not change ordered replica groups.</summary>
    [Fact]
    public void PeerPermutationDoesNotChangeReplicaGroup()
    {
        var left = new ReplicaGroupLocator(new PhysicalNodeRing(["node-a", "node-b", "node-c"]), 2);
        var right = new ReplicaGroupLocator(new PhysicalNodeRing(["node-c", "node-a", "node-b"]), 2);
        var a = new string[2];
        var b = new string[2];
        left.GetReplicaGroup("node-b", a);
        right.GetReplicaGroup("node-b", b);
        Assert.Equal(a[0], b[0]);
        Assert.Equal(a[1], b[1]);
    }

    /// <summary>Original owner appears once in the group.</summary>
    [Fact]
    public void NeverIncludesOriginalOwnerTwice()
    {
        var ring = new PhysicalNodeRing(["node-a", "node-b", "node-c", "node-d", "node-e"]);
        var group = new string[5];
        ring.WriteReplicaGroup("node-c", 5, group);
        Assert.Equal(1, CountOccurrences(group, "node-c"));
    }

    /// <summary>Group cardinality equals the configured replica count.</summary>
    [Fact]
    public void GroupCardinalityEqualsReplicaCount()
    {
        var locator = new ReplicaGroupLocator(new PhysicalNodeRing(["a", "b", "c", "d", "e"]), 4);
        Assert.Equal(4, locator.ReplicaCount);
        var group = new string[4];
        locator.GetReplicaGroup("a", group);
        Assert.Equal(4, group.Length);
    }

    /// <summary>Product locator matches the independent ordinal model from the design table.</summary>
    [Fact]
    public void ProductLocatorMatchesIndependentModel()
    {
        var locator = new ReplicaGroupLocator(new PhysicalNodeRing(["node-a", "node-b", "node-c", "node-d"]), 3);
        var group = new string[3];
        locator.GetReplicaGroup("node-a", group);
        Assert.Equal("node-a", group[0]);
        Assert.Equal("node-b", group[1]);
        Assert.Equal("node-c", group[2]);
        locator.GetReplicaGroup("node-d", group);
        Assert.Equal("node-d", group[0]);
        Assert.Equal("node-a", group[1]);
        Assert.Equal("node-b", group[2]);
    }

    private static int CountOccurrences(ReadOnlySpan<string> values, string expected)
    {
        var count = 0;
        for (var i = 0; i < values.Length; i++)
        {
            if (string.Equals(values[i], expected, StringComparison.Ordinal))
                count++;
        }

        return count;
    }
}
