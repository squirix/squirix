using System;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Covers TopologyOptions fingerprint and inter-node URI resolution.</summary>
public sealed class TopologyOptionsFingerprintTests
{
    /// <summary>Single-node topology fingerprints without rewriting peer URIs.</summary>
    [Fact]
    public void CreateFingerprintSingleNodeUsesPeerUri()
    {
        var peer = new ServerPeer { NodeId = "n1", Uri = new Uri("https://localhost:6001") };
        var topology = new TopologyOptions(peer)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peer.Uri,
            ReplicaCount = 1,
            ConfigurationGeneration = 1,
        };

        var fingerprint = topology.CreateFingerprint(new MtlsOptions());
        Assert.Equal(64, fingerprint.ToString().Length);
    }

    /// <summary>Multi-node fingerprints rewrite inter-node URIs from InternalListenPort when unset.</summary>
    [Fact]
    public void CreateFingerprintRewritesInterNodePort()
    {
        ServerPeer[] peers =
        [
            new() { NodeId = "n1", Uri = new Uri("https://127.0.0.1:6001") },
            new() { NodeId = "n2", Uri = new Uri("https://127.0.0.1:6002") },
        ];
        var topology = new TopologyOptions(peers)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = 2,
            ConfigurationGeneration = 3,
            VirtualNodes = 64,
        };
        var withPort = topology.CreateFingerprint(new MtlsOptions { InternalListenPort = 7001 });
        var withoutPort = topology.CreateFingerprint(new MtlsOptions());
        Assert.NotEqual(withPort, withoutPort);
    }

    /// <summary>Explicit InterNodeUri is preferred over InternalListenPort rewriting.</summary>
    [Fact]
    public void CreateFingerprintPrefersConfiguredInterNodeUri()
    {
        var interNode = new Uri("https://127.0.0.1:7100");
        ServerPeer[] peers =
        [
            new() { NodeId = "n1", Uri = new Uri("https://127.0.0.1:6001"), InterNodeUri = interNode },
            new() { NodeId = "n2", Uri = new Uri("https://127.0.0.1:6002"), InterNodeUri = new Uri("https://127.0.0.1:7101") },
        ];
        var topology = new TopologyOptions(peers)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = 2,
            ConfigurationGeneration = 1,
        };
        var withExplicit = topology.CreateFingerprint(new MtlsOptions { InternalListenPort = 9999 });
        var baselinePeers = new[]
        {
            new ServerPeer { NodeId = "n1", Uri = peers[0].Uri, InterNodeUri = interNode },
            new ServerPeer { NodeId = "n2", Uri = peers[1].Uri, InterNodeUri = peers[1].InterNodeUri },
        };
        var baseline = new TopologyOptions(baselinePeers)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = 2,
            ConfigurationGeneration = 1,
        }.CreateFingerprint(new MtlsOptions { InternalListenPort = 1 });
        Assert.Equal(withExplicit, baseline);
    }
}
