using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Canonical topology fingerprint stability checks.</summary>
[Immutable]
public sealed class TopologyFingerprintTests
{
    /// <summary>Compute matches the independently derived golden digest for a fixed single-peer vector.</summary>
    [Test]
    public async Task ComputeMatchesGoldenVector()
    {
        var inputs = new FingerprintInputs
        {
            ClusterId = "cluster",
            ConfigurationGeneration = 1,
            ReplicaCount = 1,
            VirtualNodes = 128,
            Peers = [new FingerprintPeer("node-a", new Uri("https://localhost:6001/"), new Uri("https://localhost:6001/"))],
            Policy = FingerprintPolicy.Default,
            MinClusterPackageVersion = "0.1.0-preview.8",
            QuorumAckMode = "majority-no-lease",
        };

        // Golden SHA-256 over the documented canonical layout, derived outside the
        // production code, so a systematic hashing bug cannot stay green on both sides.
        _ = await Assert.That(TopologyFingerprint.Compute(inputs).ToString()).IsEqualTo("1DE62DAF83BD5D2129BFF07DFDCEF1DAA7C3361B48F565688FF2D5800EC133A5", StringComparer.Ordinal);
    }

    /// <summary>Equals and ToString are stable for identical digests.</summary>
    [Test]
    public async Task EqualsHashCodeAndToStringAreStable()
    {
        var left = TopologyFingerprint.Compute(CreateInputs(CreatePeers()));
        var right = TopologyFingerprint.Compute(CreateInputs(CreatePeers()));
        _ = await Assert.That(left.Equals(right)).IsTrue();
        _ = await Assert.That(right.GetHashCode()).IsEqualTo(left.GetHashCode());
        _ = await Assert.That(left.ToString().Length).IsEqualTo(64);
        _ = await Assert.That(right.ToString()).IsEqualTo(left.ToString(), StringComparer.Ordinal);
    }

    /// <summary>Changing a peer client URI changes the fingerprint.</summary>
    [Test]
    public async Task FingerprintChangesWhenPeerUriChanges()
    {
        var left = TopologyFingerprint.Compute(
            CreateInputs(
            [
                new FingerprintPeer("node-a", new Uri("https://a:1/"), new Uri("https://a:2/")),
                new FingerprintPeer("node-b", new Uri("https://b:1/"), new Uri("https://b:2/")),
            ]));
        var right = TopologyFingerprint.Compute(
            CreateInputs(
            [
                new FingerprintPeer("node-a", new Uri("https://a:1/"), new Uri("https://a:2/")),
                new FingerprintPeer("node-b", new Uri("https://b:9/"), new Uri("https://b:2/")),
            ]));
        _ = await Assert.That(right).IsNotEqualTo(left);
    }

    /// <summary>Changing configuration generation changes the fingerprint.</summary>
    [Test]
    public async Task FingerprintTracksGenerationChange()
    {
        var peers = CreatePeers();
        var left = TopologyFingerprint.Compute(CreateInputs(peers));
        var fingerprintInputs = new FingerprintInputs
        {
            ClusterId = "cluster",
            ConfigurationGeneration = 2,
            ReplicaCount = 2,
            VirtualNodes = 128,
            Peers = peers,
            Policy = FingerprintPolicy.Default,
            MinClusterPackageVersion = PolicyOptions.MinClusterPackageVersion,
            QuorumAckMode = PolicyOptions.QuorumAckMode,
        };
        var right = TopologyFingerprint.Compute(fingerprintInputs);
        _ = await Assert.That(right).IsNotEqualTo(left);
    }

    /// <summary>Changing RF&gt;1 idempotency policy changes the fingerprint.</summary>
    [Test]
    public async Task FingerprintTracksIdempotencyPolicyChange()
    {
        var peers = CreatePeers();
        var left = TopologyFingerprint.Compute(CreateInputs(peers));
        var fingerprintInputs = new FingerprintInputs
        {
            ClusterId = "cluster",
            ConfigurationGeneration = 1,
            ReplicaCount = 2,
            VirtualNodes = 128,
            Peers = peers,
            Policy = FingerprintPolicy.Default with { RfIdempotencyMaxInFlightRecords = PolicyOptions.RfIdempotencyMaxInFlightRecords + 1 },
            MinClusterPackageVersion = PolicyOptions.MinClusterPackageVersion,
            QuorumAckMode = PolicyOptions.QuorumAckMode,
        };
        var right = TopologyFingerprint.Compute(fingerprintInputs);
        _ = await Assert.That(right).IsNotEqualTo(left);
    }

    /// <summary>Changing the cluster package version changes the fingerprint.</summary>
    /// <remarks>
    /// The legacy version below derives from <see cref="PolicyOptions.MinClusterPackageVersion" /> so the test
    /// stays version-agnostic: any differing version diverges the fingerprint the same way.
    /// </remarks>
    [Test]
    public async Task FingerprintTracksPackageVersionChange()
    {
        var peers = CreatePeers();
        var left = TopologyFingerprint.Compute(CreateInputs(peers));
        var fingerprintInputs = new FingerprintInputs
        {
            ClusterId = "cluster",
            ConfigurationGeneration = 1,
            ReplicaCount = 2,
            VirtualNodes = 128,
            Peers = peers,
            Policy = FingerprintPolicy.Default,
            MinClusterPackageVersion = PolicyOptions.MinClusterPackageVersion + "-legacy",
            QuorumAckMode = PolicyOptions.QuorumAckMode,
        };
        var right = TopologyFingerprint.Compute(fingerprintInputs);
        _ = await Assert.That(right).IsNotEqualTo(left);
    }

    /// <summary>Changing replica count changes the fingerprint.</summary>
    [Test]
    public async Task FingerprintTracksReplicaCountChange()
    {
        var peers = CreatePeers();
        var rf1 = TopologyFingerprint.Compute(CreateInputs(peers, 1));
        var rf2 = TopologyFingerprint.Compute(CreateInputs(peers));
        _ = await Assert.That(rf2).IsNotEqualTo(rf1);
    }

    /// <summary>Changing replication policy constants changes the fingerprint.</summary>
    [Test]
    public async Task FingerprintTracksReplicationPolicyChange()
    {
        var peers = CreatePeers();
        var left = TopologyFingerprint.Compute(CreateInputs(peers));
        var fingerprintInputs = new FingerprintInputs
        {
            ClusterId = "cluster",
            ConfigurationGeneration = 1,
            ReplicaCount = 2,
            VirtualNodes = 128,
            Peers = peers,
            Policy = FingerprintPolicy.Default with { ProtocolAlgorithmVersion = PolicyOptions.ProtocolAlgorithmVersion + 1 },
            MinClusterPackageVersion = PolicyOptions.MinClusterPackageVersion,
            QuorumAckMode = PolicyOptions.QuorumAckMode,
        };
        var right = TopologyFingerprint.Compute(fingerprintInputs);
        _ = await Assert.That(right).IsNotEqualTo(left);
    }

    /// <summary>Node ids are compared with ordinal sorting, not culture rules.</summary>
    [Test]
    public async Task FingerprintUsesOrdinalNodeIds()
    {
        var left = TopologyFingerprint.Compute(
            CreateInputs(
            [
                new FingerprintPeer("Node-a", new Uri("https://a:1/"), new Uri("https://a:2/")),
                new FingerprintPeer("node-b", new Uri("https://b:1/"), new Uri("https://b:2/")),
            ]));
        var right = TopologyFingerprint.Compute(
            CreateInputs(
            [
                new FingerprintPeer("node-b", new Uri("https://b:1/"), new Uri("https://b:2/")),
                new FingerprintPeer("Node-a", new Uri("https://a:1/"), new Uri("https://a:2/")),
            ]));
        _ = await Assert.That(right).IsEqualTo(left);
    }

    /// <summary>group_id is stable for a fixed fingerprint vector and owner.</summary>
    [Test]
    public async Task GroupIdIsStableForFixedVector()
    {
        var fingerprint = TopologyFingerprint.Compute(CreateInputs(CreatePeers()));
        var first = fingerprint.CreateGroupId("cluster", "node-a");
        var second = fingerprint.CreateGroupId("cluster", "node-a");
        _ = await Assert.That(second).IsEqualTo(first, StringComparer.Ordinal);
        _ = await Assert.That(string.Equals(first, fingerprint.CreateGroupId("cluster", "node-b"), StringComparison.Ordinal)).IsFalse();
        _ = await Assert.That(first.Length).IsEqualTo(64);
    }

    /// <summary>group_id matches the independently derived golden digest for a fixed vector and owner.</summary>
    [Test]
    public async Task GroupIdMatchesGoldenVector()
    {
        var inputs = new FingerprintInputs
        {
            ClusterId = "cluster",
            ConfigurationGeneration = 1,
            ReplicaCount = 1,
            VirtualNodes = 128,
            Peers = [new FingerprintPeer("node-a", new Uri("https://localhost:6001/"), new Uri("https://localhost:6001/"))],
            Policy = FingerprintPolicy.Default,
            MinClusterPackageVersion = "0.1.0-preview.8",
            QuorumAckMode = "majority-no-lease",
        };
        var fingerprint = TopologyFingerprint.Compute(inputs);

        _ = await Assert.That(fingerprint.CreateGroupId("cluster", "node-a")).IsEqualTo("AB03238272D47286AA55CD55C199CB281324BC470DB9A2493A2DB727DEC407F2", StringComparer.Ordinal);
    }

    /// <summary>Peers[] permutation produces the same fingerprint bytes.</summary>
    [Test]
    public async Task PeerPermutationProducesSameFingerprint()
    {
        var left = TopologyFingerprint.Compute(
            CreateInputs(
            [
                new FingerprintPeer("node-a", new Uri("https://a:1/"), new Uri("https://a:2/")),
                new FingerprintPeer("node-b", new Uri("https://b:1/"), new Uri("https://b:2/")),
                new FingerprintPeer("node-c", new Uri("https://c:1/"), new Uri("https://c:2/")),
            ]));
        var right = TopologyFingerprint.Compute(
            CreateInputs(
            [
                new FingerprintPeer("node-c", new Uri("https://c:1/"), new Uri("https://c:2/")),
                new FingerprintPeer("node-a", new Uri("https://a:1/"), new Uri("https://a:2/")),
                new FingerprintPeer("node-b", new Uri("https://b:1/"), new Uri("https://b:2/")),
            ]));
        _ = await Assert.That(right).IsEqualTo(left);
        _ = await Assert.That(left.Bytes.SequenceEqual(right.Bytes)).IsTrue();
    }

    private static FingerprintInputs CreateInputs(ReadOnlySpan<FingerprintPeer> peers, int replicaCount = 2)
    {
        var copy = new FingerprintPeer[peers.Length];
        peers.CopyTo(copy);
        return new()
        {
            ClusterId = "cluster",
            ConfigurationGeneration = 1,
            ReplicaCount = replicaCount,
            VirtualNodes = 128,
            Peers = copy,
            Policy = FingerprintPolicy.Default,
            MinClusterPackageVersion = PolicyOptions.MinClusterPackageVersion,
            QuorumAckMode = PolicyOptions.QuorumAckMode,
        };
    }

    private static FingerprintPeer[] CreatePeers() =>
    [
        new("node-a", new Uri("https://a:1/"), new Uri("https://a:2/")),
        new("node-b", new Uri("https://b:1/"), new Uri("https://b:2/")),
    ];
}
