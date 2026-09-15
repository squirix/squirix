using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>ConfigurationGeneration defaults and fingerprint sensitivity (REQ-UPG-001 config portion).</summary>
[Immutable]
public sealed class ConfigurationGenerationTests : ServerUnitTestBase
{
    /// <summary>Default ConfigurationGeneration is one.</summary>
    [Test]
    public async Task DefaultsToOne()
    {
        _ = await Assert.That(new SquirixServerOptions().ConfigurationGeneration).IsEqualTo(1u);
        var topology = new TopologyOptions(new ServerPeer { NodeId = "n1", Uri = new Uri("https://localhost:6001") })
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = new Uri("https://localhost:6001"),
        };
        _ = await Assert.That(topology.ConfigurationGeneration).IsEqualTo(1u);
    }

    /// <summary>Fingerprint changes when ConfigurationGeneration changes.</summary>
    [Test]
    public async Task FingerprintChangesWhenGenerationChanges()
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
                Policy = FingerprintPolicy.Default,
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
                Policy = FingerprintPolicy.Default,
                ConfigurationGeneration = 2,
                ReplicaCount = 1,
                VirtualNodes = 128,
                MinClusterPackageVersion = PolicyOptions.MinClusterPackageVersion,
                QuorumAckMode = PolicyOptions.QuorumAckMode,
            });

        _ = await Assert.That(left.Equals(right)).IsFalse();
    }

    /// <summary>Zero ConfigurationGeneration is rejected.</summary>
    [Test]
    public async Task RejectsZeroGeneration()
    {
        var topology = new TopologyOptions(new ServerPeer { NodeId = "n1", Uri = new Uri("https://localhost:6001") })
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = new Uri("https://localhost:6001"),
            ConfigurationGeneration = 0,
        };

        _ = await Assert.That(TopologyValidator.TryValidate(topology, out var errors)).IsFalse();
        _ = await Assert.That(errors).Contains("ConfigurationGeneration must be greater than zero.", StringComparer.Ordinal);
    }
}
