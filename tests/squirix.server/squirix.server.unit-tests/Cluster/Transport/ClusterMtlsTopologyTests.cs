using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>
/// Unit tests for topology-driven inter-node mTLS requirements.
/// </summary>
public sealed class ClusterMtlsTopologyTests
{
    /// <summary>
    /// Ensures a standalone node with only the local peer does not require inter-node mTLS.
    /// </summary>
    [Fact]
    public void RequiresInterNodeMtlsReturnsFalseForStandaloneTopology()
    {
        var cluster = CreateCluster(
            "node-a",
            "https://localhost:6001",
            [new Peer { NodeId = "node-a", Url = "https://localhost:6001" }]);

        Assert.False(ClusterMtlsTopology.RequiresInterNodeMtls(cluster));
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

        Assert.True(ClusterMtlsTopology.RequiresInterNodeMtls(cluster));
    }

    private static ClusterConfig CreateCluster(string nodeId, string url, Peer[] peers) =>
        new()
        {
            ClusterId = "test",
            NodeId = nodeId,
            Url = url,
            Peers = peers,
        };
}
