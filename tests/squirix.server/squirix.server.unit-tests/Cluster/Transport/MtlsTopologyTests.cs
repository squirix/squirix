using System;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Unit tests for topology-driven inter-node mTLS requirements.</summary>
public sealed class MtlsTopologyTests
{
    private static readonly Uri NodeAUrl = new("https://localhost:6001");
    private static readonly Uri NodeBUrl = new("https://localhost:6002");
    private static readonly Uri NodeCUrl = new("https://localhost:6003");

    /// <summary>Ensures remote peer node identifiers exclude the local node.</summary>
    [Fact]
    public void GetRemotePeerNodeIdsReturnsOnlyRemotePeers()
    {
        var cluster = CreateCluster(
            "node-a",
            NodeAUrl,
            [
                new Peer { NodeId = "node-a", Url = NodeAUrl },
                new Peer { NodeId = "node-b", Url = NodeBUrl },
                new Peer { NodeId = "node-c", Url = NodeCUrl },
            ]);

        Assert.Equal(["node-b", "node-c"], MtlsTopology.GetRemotePeerNodeIds(cluster));
    }

    /// <summary>Ensures a standalone node with only the local peer does not require inter-node mTLS.</summary>
    [Fact]
    public void RequiresInterNodeMtlsReturnsFalseForStandaloneTopology()
    {
        var cluster = CreateCluster("node-a", NodeAUrl, [new Peer { NodeId = "node-a", Url = NodeAUrl }]);

        Assert.False(MtlsTopology.RequiresInterNodeMtls(cluster));
    }

    /// <summary>Ensures a multi-node topology with remote peers requires inter-node mTLS.</summary>
    [Fact]
    public void RequiresInterNodeMtlsReturnsTrueWhenRemotePeersAreConfigured()
    {
        var cluster = CreateCluster(
            "node-a",
            NodeAUrl,
            [
                new Peer { NodeId = "node-a", Url = NodeAUrl },
                new Peer { NodeId = "node-b", Url = NodeBUrl },
            ]);

        Assert.True(MtlsTopology.RequiresInterNodeMtls(cluster));
    }

    private static ClusterConfig CreateCluster(string nodeId, Uri url, Peer[] peers) => new()
    {
        ClusterId = "test",
        NodeId = nodeId,
        Url = url,
        Peers = peers,
    };
}
