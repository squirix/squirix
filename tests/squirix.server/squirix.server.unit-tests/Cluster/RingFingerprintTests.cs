using System;
using System.Globalization;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Unit tests for <see cref="RingFingerprint" />.</summary>
[Immutable]
public sealed class RingFingerprintTests : ServerUnitTestBase
{
    private const string ClusterId = "cluster";

    private const int VirtualNodes = 128;

    /// <summary>An added node changes the fingerprint.</summary>
    [Test]
    public async Task AddedNodeChangesFingerprint()
    {
        var baseline = Fingerprint(["n1", "n2"]);
        _ = await Assert.That(Fingerprint(["n1", "n2", "n3"])).IsNotEqualTo(baseline);
    }

    /// <summary>A different cluster id changes the fingerprint.</summary>
    [Test]
    public async Task ClusterIdChangesFingerprint()
    {
        var baseline = Fingerprint(["n1", "n2"]);
        _ = await Assert.That(RingFingerprint.Create("other", ["n1", "n2"], VirtualNodes).Value).IsNotEqualTo(baseline);
    }

    /// <summary>Duplicate node ids do not change the fingerprint.</summary>
    [Test]
    public async Task DuplicatesDoNotChangeFingerprint()
    {
        var baseline = Fingerprint(["n1", "n2"]);
        _ = await Assert.That(Fingerprint(["n1", "n2", "n1", "n2"])).IsEqualTo(baseline);
    }

    /// <summary>Equal fingerprints assign sample keys to the same owners.</summary>
    [Test]
    public async Task EqualFingerprintsYieldEqualOwners()
    {
        string[] first = ["n3", "n1", "n2", "n1"];
        string[] second = ["n2", "n3", "n1"];
        _ = await Assert.That(Fingerprint(first)).IsEqualTo(Fingerprint(second));

        var left = RuntimeServiceRegistration.CreateHashLocator(first);
        var right = RuntimeServiceRegistration.CreateHashLocator(second);
        for (var i = 0; i < 200; i++)
        {
            var key = $"key-{i.ToString(CultureInfo.InvariantCulture)}";
            _ = await Assert.That(left.GetOwner("cache", key)).IsEqualTo(right.GetOwner("cache", key));
        }
    }

    /// <summary>The fingerprint of a fixed input is pinned, so an accidental change of the encoding is caught.</summary>
    [Test]
    public async Task FingerprintMatchesGoldenVector()
    {
        var value = Fingerprint(["n2", "n1"]);
        _ = await Assert.That(value).IsEqualTo("F34273F8A8A6F585B9BAB884F36960B2C23792823F97CADAA89DC57FEC5A3583");
    }

    /// <summary>Node ids are compared ordinally, so case differences change the fingerprint.</summary>
    [Test]
    public async Task NodeIdComparisonIsOrdinal()
    {
        var baseline = Fingerprint(["n1", "n2"]);
        _ = await Assert.That(Fingerprint(["N1", "n2"])).IsNotEqualTo(baseline);
    }

    /// <summary>Peer order does not change the fingerprint.</summary>
    [Test]
    public async Task PeerOrderDoesNotChangeFingerprint()
    {
        var sorted = Fingerprint(["n1", "n2", "n3"]);
        _ = await Assert.That(Fingerprint(["n3", "n1", "n2"])).IsEqualTo(sorted);
    }

    /// <summary>A removed node changes the fingerprint.</summary>
    [Test]
    public async Task RemovedNodeChangesFingerprint()
    {
        var baseline = Fingerprint(["n1", "n2"]);
        _ = await Assert.That(Fingerprint(["n1"])).IsNotEqualTo(baseline);
    }

    /// <summary>A renamed node changes the fingerprint.</summary>
    [Test]
    public async Task RenamedNodeChangesFingerprint()
    {
        var baseline = Fingerprint(["n1", "n2"]);
        _ = await Assert.That(Fingerprint(["n1", "n9"])).IsNotEqualTo(baseline);
    }

    /// <summary>The same inputs produce the same fingerprint.</summary>
    [Test]
    public async Task SameInputsProduceSameFingerprint()
    {
        var baseline = Fingerprint(["n1", "n2"]);
        _ = await Assert.That(Fingerprint(["n1", "n2"])).IsEqualTo(baseline);
    }

    /// <summary>The value is 64 uppercase hex characters.</summary>
    [Test]
    public async Task ValueIsUppercaseHex()
    {
        var baseline = Fingerprint(["n1", "n2"]);
        _ = await Assert.That(IsUppercaseHex64(baseline)).IsTrue();
    }

    /// <summary>A different virtual node count changes the fingerprint.</summary>
    [Test]
    public async Task VirtualNodesChangeFingerprint()
    {
        var baseline = Fingerprint(["n1", "n2"]);
        _ = await Assert.That(RingFingerprint.Create(ClusterId, ["n1", "n2"], VirtualNodes + 1).Value).IsNotEqualTo(baseline);
    }

    /// <summary>Whitespace around a node id does not change the fingerprint, because the ring trims ids.</summary>
    [Test]
    public async Task WhitespaceDoesNotChangeFingerprint()
    {
        var trimmed = Fingerprint(["n1", "n2"]);
        _ = await Assert.That(Fingerprint(["n1", " n2"])).IsEqualTo(trimmed);
    }

    private static string Fingerprint(ReadOnlySpan<string> nodeIds) => RingFingerprint.Create(ClusterId, nodeIds, VirtualNodes).Value;

    private static bool IsUppercaseHex64(string value)
    {
        if (value.Length != 64)
            return false;

        foreach (var c in value)
        {
            if (c is not ((>= '0' and <= '9') or (>= 'A' and <= 'F')))
                return false;
        }

        return true;
    }
}
