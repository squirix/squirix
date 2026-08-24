using System;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Canonical topology fingerprint stability checks.</summary>
[Immutable]
public sealed class TopologyFingerprintTests
{
    /// <summary>Peers[] permutation produces the same fingerprint bytes.</summary>
    [Fact]
    public void PeerPermutationProducesSameFingerprint()
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
        Assert.Equal(left, right);
        Assert.True(left.Bytes.SequenceEqual(right.Bytes));
    }

    /// <summary>Changing replica count changes the fingerprint.</summary>
    [Fact]
    public void FingerprintTracksReplicaCountChange()
    {
        var peers = CreatePeers();
        var rf1 = TopologyFingerprint.Compute(CreateInputs(peers, 1));
        var rf2 = TopologyFingerprint.Compute(CreateInputs(peers));
        Assert.NotEqual(rf1, rf2);
    }

    /// <summary>Changing configuration generation changes the fingerprint.</summary>
    [Fact]
    public void FingerprintTracksGenerationChange()
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
            MinClusterPackageVersion = PolicyOptions.MinClusterPackageVersion,
            QuorumAckMode = PolicyOptions.QuorumAckMode,
        };
        var right = TopologyFingerprint.Compute(fingerprintInputs);
        Assert.NotEqual(left, right);
    }

    /// <summary>Changing a peer client URI changes the fingerprint.</summary>
    [Fact]
    public void FingerprintChangesWhenPeerUriChanges()
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
        Assert.NotEqual(left, right);
    }

    /// <summary>Changing replication policy constants changes the fingerprint.</summary>
    [Fact]
    public void FingerprintTracksReplicationPolicyChange()
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
            MinClusterPackageVersion = PolicyOptions.MinClusterPackageVersion,
            QuorumAckMode = PolicyOptions.QuorumAckMode,
            ProtocolAlgorithmVersion = PolicyOptions.ProtocolAlgorithmVersion + 1,
        };
        var right = TopologyFingerprint.Compute(fingerprintInputs);
        Assert.NotEqual(left, right);
    }

    /// <summary>Changing RF&gt;1 idempotency policy changes the fingerprint.</summary>
    [Fact]
    public void FingerprintTracksIdempotencyPolicyChange()
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
            MinClusterPackageVersion = PolicyOptions.MinClusterPackageVersion,
            QuorumAckMode = PolicyOptions.QuorumAckMode,
            RfIdempotencyMaxInFlightRecords = PolicyOptions.RfIdempotencyMaxInFlightRecords + 1,
        };
        var right = TopologyFingerprint.Compute(fingerprintInputs);
        Assert.NotEqual(left, right);
    }

    /// <summary>Node ids are compared with ordinal sorting, not culture rules.</summary>
    [Fact]
    public void FingerprintUsesOrdinalNodeIds()
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
        Assert.Equal(left, right);
    }

    /// <summary>group_id is stable for a fixed fingerprint vector and owner.</summary>
    [Fact]
    public void GroupIdIsStableForFixedVector()
    {
        var fingerprint = TopologyFingerprint.Compute(CreateInputs(CreatePeers()));
        var first = fingerprint.CreateGroupId("cluster", "node-a");
        var second = fingerprint.CreateGroupId("cluster", "node-a");
        Assert.Equal(first, second, StringComparer.Ordinal);
        Assert.False(string.Equals(first, fingerprint.CreateGroupId("cluster", "node-b"), StringComparison.Ordinal));
        Assert.Equal(64, first.Length);
    }

    /// <summary>Equals and ToString are stable for identical digests.</summary>
    [Fact]
    public void EqualsHashCodeAndToStringAreStable()
    {
        var left = TopologyFingerprint.Compute(CreateInputs(CreatePeers()));
        var right = TopologyFingerprint.Compute(CreateInputs(CreatePeers()));
        Assert.True(left.Equals(right));
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.Equal(64, left.ToString().Length);
        Assert.Equal(left.ToString(), right.ToString(), StringComparer.Ordinal);
    }

    private static FingerprintPeer[] CreatePeers() =>
    [
        new("node-a", new Uri("https://a:1/"), new Uri("https://a:2/")),
        new("node-b", new Uri("https://b:1/"), new Uri("https://b:2/")),
    ];

    private static FingerprintInputs CreateInputs(FingerprintPeer[] peers, int replicaCount = 2) => new()
    {
        ClusterId = "cluster",
        ConfigurationGeneration = 1,
        ReplicaCount = replicaCount,
        VirtualNodes = 128,
        Peers = peers,
        MinClusterPackageVersion = PolicyOptions.MinClusterPackageVersion,
        QuorumAckMode = PolicyOptions.QuorumAckMode,
    };
}
