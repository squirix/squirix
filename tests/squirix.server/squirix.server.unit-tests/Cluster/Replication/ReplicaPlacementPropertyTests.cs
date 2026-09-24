using System;
using System.Globalization;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Property-style placement checks for the physical replica ring.</summary>
[Immutable]
public sealed class ReplicaPlacementPropertyTests
{
    /// <summary>Whitespace-only and duplicate peer ids are filtered before sorting.</summary>
    [Test]
    public async Task FiltersWhitespaceAndDuplicates()
    {
        var ring = new PhysicalNodeRing(["node-b", "node-a", "node-a", string.Empty, "node-c", "   "]);
        _ = await Assert.That(ring.Count).IsEqualTo(3);
        var group = new string[3];
        ring.WriteReplicaGroup("node-a", 3, group);
        _ = await Assert.That(group[0]).IsEqualTo("node-a");
        _ = await Assert.That(group[1]).IsEqualTo("node-b");
        _ = await Assert.That(group[2]).IsEqualTo("node-c");
    }

    /// <summary>Group cardinality equals the configured replica count.</summary>
    [Test]
    public async Task GroupCardinalityEqualsReplicaCount()
    {
        var locator = new ReplicaGroupLocator(new PhysicalNodeRing(["a", "b", "c", "d", "e"]), 4);
        _ = await Assert.That(locator.ReplicaCount).IsEqualTo(4);
        var group = new string[4];
        locator.GetReplicaGroup("a", group);
        _ = await Assert.That(group.Length).IsEqualTo(4);
    }

    /// <summary>Original owner appears once in the group.</summary>
    [Test]
    public async Task NeverIncludesOriginalOwnerTwice()
    {
        var ring = new PhysicalNodeRing(["node-a", "node-b", "node-c", "node-d", "node-e"]);
        var group = new string[5];
        ring.WriteReplicaGroup("node-c", 5, group);
        _ = await Assert.That(CountOccurrences(group, "node-c")).IsEqualTo(1);
    }

    /// <summary>All keys owned by the same original owner share one ordered replica group.</summary>
    [Test]
    public async Task OwnerRangesShareOrderedReplicaGroup()
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
            var key = "k" + i.ToString(CultureInfo.InvariantCulture);
            var owner = locator.GetOwner("cache", key);
            if (!string.Equals(owner, "node-a", StringComparison.Ordinal))
                continue;

            groupLocator.GetReplicaGroup(owner, group);
            if (expected0 == null)
            {
                expected0 = group[0];
                expected1 = group[1];
                expected2 = group[2];
            }
            else
            {
                _ = await Assert.That(group[0]).IsEqualTo(expected0);
                _ = await Assert.That(group[1]).IsEqualTo(expected1);
                _ = await Assert.That(group[2]).IsEqualTo(expected2);
            }

            matched++;
        }

        _ = await Assert.That(matched >= 10).IsTrue().Because("Expected enough keys owned by node-a.");
    }

    /// <summary>Peer list permutation does not change ordered replica groups.</summary>
    [Test]
    public async Task PeerPermutationDoesNotChangeReplicaGroup()
    {
        var left = new ReplicaGroupLocator(new PhysicalNodeRing(["node-a", "node-b", "node-c"]), 2);
        var right = new ReplicaGroupLocator(new PhysicalNodeRing(["node-c", "node-a", "node-b"]), 2);
        var a = new string[2];
        var b = new string[2];
        left.GetReplicaGroup("node-b", a);
        right.GetReplicaGroup("node-b", b);
        _ = await Assert.That(b[0]).IsEqualTo(a[0]);
        _ = await Assert.That(b[1]).IsEqualTo(a[1]);
    }

    /// <summary>Product locator matches the independent ordinal model from the design table.</summary>
    [Test]
    public async Task ProductLocatorMatchesIndependentModel()
    {
        var locator = new ReplicaGroupLocator(new PhysicalNodeRing(["node-a", "node-b", "node-c", "node-d"]), 3);
        var group = new string[3];
        locator.GetReplicaGroup("node-a", group);
        _ = await Assert.That(group[0]).IsEqualTo("node-a");
        _ = await Assert.That(group[1]).IsEqualTo("node-b");
        _ = await Assert.That(group[2]).IsEqualTo("node-c");
        locator.GetReplicaGroup("node-d", group);
        _ = await Assert.That(group[0]).IsEqualTo("node-d");
        _ = await Assert.That(group[1]).IsEqualTo("node-a");
        _ = await Assert.That(group[2]).IsEqualTo("node-b");
    }

    /// <summary>Destination length must match replica count.</summary>
    [Test]
    public void RejectsDestinationLengthMismatch()
    {
        var ring = new PhysicalNodeRing(["node-a", "node-b"]);
        var group = new string[1];
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(ring, group, static (r, g) => r.WriteReplicaGroup("node-a", 2, g));
    }

    /// <summary>Empty input is rejected.</summary>
    [Test]
    public void RejectsEmptyNodeList() => _ = NodeExceptionAssert.For<ArgumentException>().Throws<string[]>([], static nodes => _ = new PhysicalNodeRing(nodes));

    /// <summary>Replica count must fit the ring and policy max.</summary>
    [Test]
    public void RejectsReplicaCountOutOfRange()
    {
        var ring = new PhysicalNodeRing(["node-a", "node-b"]);
        var group = new string[3];
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(ring, group, static (r, g) => r.WriteReplicaGroup("node-a", 0, g));
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(ring, group, static (r, g) => r.WriteReplicaGroup("node-a", 3, g));
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(ring, static r => _ = new ReplicaGroupLocator(r, 0));
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(ring, static r => _ = new ReplicaGroupLocator(r, 3));
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(ring, static r => _ = new ReplicaGroupLocator(r, PolicyOptions.MaxReplicaCount + 1));
    }

    /// <summary>Unknown owners are rejected.</summary>
    [Test]
    public void RejectsUnknownOwner()
    {
        var ring = new PhysicalNodeRing(["node-a", "node-b"]);
        var group = new string[2];
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(ring, group, static (r, g) => r.WriteReplicaGroup("missing", 2, g));
    }

    /// <summary>RF=1/2/3/5 replica groups hold the owner plus ordinal successors with wrap.</summary>
    [Test]
    public async Task ReplicaGroupMatrixCoversOneTwoThreeFive()
    {
        var ring = new PhysicalNodeRing(["n1", "n2", "n3", "n4", "n5"]);

        await AssertGroupAsync(ring, "n3", 1, ["n3"]);
        await AssertGroupAsync(ring, "n3", 2, ["n3", "n4"]);
        await AssertGroupAsync(ring, "n3", 3, ["n3", "n4", "n5"]);
        await AssertGroupAsync(ring, "n4", 3, ["n4", "n5", "n1"]);
        await AssertGroupAsync(ring, "n3", 5, ["n3", "n4", "n5", "n1", "n2"]);
    }

    /// <summary>Followers are the next distinct physical nodes after the owner.</summary>
    [Test]
    public async Task ReturnsNextDistinctPhysicalNodes()
    {
        var ring = new PhysicalNodeRing(["node-d", "node-b", "node-a", "node-c"]);
        var group = new string[3];
        ring.WriteReplicaGroup("node-a", 3, group);
        _ = await Assert.That(group[0]).IsEqualTo("node-a");
        _ = await Assert.That(group[1]).IsEqualTo("node-b");
        _ = await Assert.That(group[2]).IsEqualTo("node-c");
    }

    /// <summary>Vnode ownership remains a single original owner string.</summary>
    [Test]
    public async Task VnodeRingOnlySelectsOriginalOwner()
    {
        var nodes = new[] { "node-a", "node-b", "node-c", "node-d" };
        var locator = RuntimeServiceRegistration.CreateHashLocator(nodes);
        for (var i = 0; i < 2_000; i++)
        {
            var owner = locator.GetOwner("cache", "k" + i.ToString(CultureInfo.InvariantCulture));
            _ = await Assert.That(nodes).Contains(owner, StringComparer.Ordinal);
            _ = await Assert.That(owner).DoesNotContain(',');
        }
    }

    /// <summary>Physical selection wraps at the end of the ordinal ring.</summary>
    [Test]
    public async Task WrapsAtPhysicalRingEnd()
    {
        var ring = new PhysicalNodeRing(["node-a", "node-b", "node-c", "node-d"]);
        var group = new string[3];
        ring.WriteReplicaGroup("node-c", 3, group);
        _ = await Assert.That(group[0]).IsEqualTo("node-c");
        _ = await Assert.That(group[1]).IsEqualTo("node-d");
        _ = await Assert.That(group[2]).IsEqualTo("node-a");
        ring.WriteReplicaGroup("node-d", 3, group);
        _ = await Assert.That(group[0]).IsEqualTo("node-d");
        _ = await Assert.That(group[1]).IsEqualTo("node-a");
        _ = await Assert.That(group[2]).IsEqualTo("node-b");
    }

    private static Task AssertGroupAsync(PhysicalNodeRing ring, string owner, int replicaCount, ReadOnlySpan<string> expected)
    {
        var group = new string[replicaCount];
        ring.WriteReplicaGroup(owner, replicaCount, group);
        return SequenceAssert.EqualAsync(expected, group, StringComparer.Ordinal);
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
