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
                new ServerPeer { NodeId = "node-a", Uri = NodeAUrl },
                new ServerPeer { NodeId = "node-b", Uri = NodeBUrl },
                new ServerPeer { NodeId = "node-c", Uri = NodeCUrl },
            ]);

        Assert.Equal(["node-b", "node-c"], MtlsTopology.GetRemotePeerNodeIds(cluster));
    }

    /// <summary>Ensures a standalone node with only the local peer does not require inter-node mTLS.</summary>
    [Fact]
    public void RequiresInterNodeMtlsFalseStandaloneTopology()
    {
        var cluster = CreateCluster("node-a", NodeAUrl, [new ServerPeer { NodeId = "node-a", Uri = NodeAUrl }]);

        Assert.False(MtlsTopology.RequiresInterNodeMtls(cluster));
    }

    /// <summary>Ensures a multi-node topology with remote peers requires inter-node mTLS.</summary>
    [Fact]
    public void RequiresInterNodeMtlsTrueRemotePeersConfigured()
    {
        var cluster = CreateCluster(
            "node-a",
            NodeAUrl,
            [
                new ServerPeer { NodeId = "node-a", Uri = NodeAUrl },
                new ServerPeer { NodeId = "node-b", Uri = NodeBUrl },
            ]);

        Assert.True(MtlsTopology.RequiresInterNodeMtls(cluster));
    }

    private static ClusterConfig CreateCluster(string nodeId, Uri uri, ServerPeer[] peers) => new(peers)
    {
        ClusterId = "test",
        NodeId = nodeId,
        Uri = uri,
    };
}
