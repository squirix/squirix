using System;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>ConfigurationGeneration defaults and fingerprint sensitivity (REQ-UPG-001 config portion).</summary>
public sealed class ConfigurationGenerationTests : ServerUnitTestBase
{
    /// <summary>Default ConfigurationGeneration is one.</summary>
    [Fact]
    public void DefaultsToOne()
    {
        Assert.Equal(1u, new SquirixServerOptions().ConfigurationGeneration);
        var topology = new TopologyOptions(new ServerPeer { NodeId = "n1", Uri = new Uri("https://localhost:6001") })
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = new Uri("https://localhost:6001"),
        };
        Assert.Equal(1u, topology.ConfigurationGeneration);
    }

    /// <summary>Zero ConfigurationGeneration is rejected.</summary>
    [Fact]
    public void RejectsZeroGeneration()
    {
        var topology = new TopologyOptions(new ServerPeer { NodeId = "n1", Uri = new Uri("https://localhost:6001") })
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = new Uri("https://localhost:6001"),
            ConfigurationGeneration = 0,
        };

        Assert.False(TopologyValidator.TryValidate(topology, out var errors));
        Assert.Contains("ConfigurationGeneration must be greater than zero.", errors, StringComparer.Ordinal);
    }

    /// <summary>Fingerprint changes when ConfigurationGeneration changes.</summary>
    [Fact]
    public void FingerprintChangesWhenGenerationChanges()
    {
        var peers = new[]
        {
            new FingerprintPeer("n1", new Uri("https://localhost:6001"), new Uri("https://localhost:6101")),
            new FingerprintPeer("n2", new Uri("https://localhost:6002"), new Uri("https://localhost:6102")),
        };
        var left = TopologyFingerprint.Compute(
            new FingerprintInputs
            {
                ClusterId = "cluster",
                Peers = peers,
                ConfigurationGeneration = 1,
                ReplicaCount = 1,
                VirtualNodes = 128,
                MinClusterPackageVersion = PolicyOptions.MinClusterPackageVersion,
                QuorumAckMode = PolicyOptions.QuorumAckMode,
            });
        var right = TopologyFingerprint.Compute(
            new FingerprintInputs
            {
                ClusterId = "cluster",
                Peers = peers,
                ConfigurationGeneration = 2,
                ReplicaCount = 1,
                VirtualNodes = 128,
                MinClusterPackageVersion = PolicyOptions.MinClusterPackageVersion,
                QuorumAckMode = PolicyOptions.QuorumAckMode,
            });

        Assert.False(left.Equals(right));
    }
}
