using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Unit tests for topology-driven internode mTLS requirements.</summary>
[Immutable]
public sealed class MtlsTopologyTests
{
    private static readonly Uri NodeAUrl = new("https://localhost:6001");
    private static readonly Uri NodeBUrl = new("https://localhost:6002");
    private static readonly Uri NodeCUrl = new("https://localhost:6003");

    /// <summary>Ensures remote peer node identifiers exclude the local node.</summary>
    [Test]
    public Task RemotePeerIdsExcludeLocalNode()
    {
        var cluster = CreateCluster(
            "node-a",
            NodeAUrl,
            [
                new ServerPeer { NodeId = "node-a", Uri = NodeAUrl },
                new ServerPeer { NodeId = "node-b", Uri = NodeBUrl },
                new ServerPeer { NodeId = "node-c", Uri = NodeCUrl },
            ]);

        return SequenceAssert.EqualAsync(["node-b", "node-c"], MtlsTopology.GetRemotePeerNodeIds(cluster), StringComparer.Ordinal);
    }

    /// <summary>Ensures a multi-node topology with remote peers requires internode mTLS.</summary>
    [Test]
    public async Task RemotePeersRequireInterNodeMtls()
    {
        var cluster = CreateCluster(
            "node-a",
            NodeAUrl,
            [
                new ServerPeer { NodeId = "node-a", Uri = NodeAUrl },
                new ServerPeer { NodeId = "node-b", Uri = NodeBUrl },
            ]);

        _ = await Assert.That(MtlsTopology.RequiresInterNodeMtls(cluster)).IsTrue();
    }

    /// <summary>Ensures a standalone node with only the local peer does not require internode mTLS.</summary>
    [Test]
    public async Task StandaloneTopologyNeedsNoInterNodeMtls()
    {
        var cluster = CreateCluster("node-a", NodeAUrl, new ServerPeer { NodeId = "node-a", Uri = NodeAUrl });

        _ = await Assert.That(MtlsTopology.RequiresInterNodeMtls(cluster)).IsFalse();
    }

    private static TopologyOptions CreateCluster(string nodeId, Uri uri, ServerPeer peer) => new(peer)
    {
        ClusterId = "test",
        NodeId = nodeId,
        Uri = uri,
    };

    private static TopologyOptions CreateCluster(string nodeId, Uri uri, ServerPeer[] peers) => new(peers)
    {
        ClusterId = "test",
        NodeId = nodeId,
        Uri = uri,
    };
}
