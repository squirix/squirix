using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>
/// Unit tests for topology-driven inter-node mTLS requirements.
/// </summary>
public sealed class MtlsTopologyTests
{
    /// <summary>
    /// Ensures remote peer node identifiers exclude the local node.
    /// </summary>
    [Fact]
    public void GetRemotePeerNodeIdsReturnsOnlyRemotePeers()
    {
        var cluster = CreateCluster(
            "node-a",
            "https://localhost:6001",
            [
                new Peer { NodeId = "node-a", Url = "https://localhost:6001" },
                new Peer { NodeId = "node-b", Url = "https://localhost:6002" },
                new Peer { NodeId = "node-c", Url = "https://localhost:6003" },
            ]);

        Assert.Equal(["node-b", "node-c"], MtlsTopology.GetRemotePeerNodeIds(cluster));
    }

    /// <summary>
    /// Ensures a standalone node with only the local peer does not require inter-node mTLS.
    /// </summary>
    [Fact]
    public void RequiresInterNodeMtlsReturnsFalseForStandaloneTopology()
    {
        var cluster = CreateCluster("node-a", "https://localhost:6001", [new Peer { NodeId = "node-a", Url = "https://localhost:6001" }]);

        Assert.False(MtlsTopology.RequiresInterNodeMtls(cluster));
    }

    /// <summary>
    /// Ensures a multi-node topology with remote peers requires inter-node mTLS.
    /// </summary>
    [Fact]
    public void RequiresInterNodeMtlsReturnsTrueWhenRemotePeersAreConfigured()
    {
        var cluster = CreateCluster(
            "node-a",
            "https://localhost:6001",
            [
                new Peer { NodeId = "node-a", Url = "https://localhost:6001" },
                new Peer { NodeId = "node-b", Url = "https://localhost:6002" },
            ]);

        Assert.True(MtlsTopology.RequiresInterNodeMtls(cluster));
    }

    private static ClusterConfig CreateCluster(string nodeId, string url, Peer[] peers) => new()
    {
        ClusterId = "test",
        NodeId = nodeId,
        Url = url,
        Peers = peers,
    };
}
