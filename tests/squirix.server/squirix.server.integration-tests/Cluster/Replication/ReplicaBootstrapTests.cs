using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Replication;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Offline RF=1 to RF&gt;1 bootstrap seeding through the durable manifest.</summary>
public sealed class ReplicaBootstrapTests : NodeIntegrationTestBase
{
    /// <summary>Bootstrap preparation seeds replica groups with pending state and preserves source data.</summary>
    [Fact(DisplayName = "Squirix.Server.IntegrationTests.Cluster.Replication.ReplicaBootstrapTests.OfflineRfOneBootstrapSeedsReplicaGroups")]
    public async Task OfflineRfOneBootstrapSeedsReplicaGroups()
    {
        using var dir = new TempDirectory("squirix-bootstrap-seed");
        var sourcePath = Path.Join(dir, "journal-000001.sqr");
        await File.WriteAllBytesAsync(sourcePath, [1, 3, 3, 7], DefaultCancellationToken);
        var before = await File.ReadAllBytesAsync(sourcePath, DefaultCancellationToken);

        var prepared = await new BootstrapPlanner().PrepareAsync(Request(dir), DefaultCancellationToken);
        var decoded = await new BootstrapManifestStore(dir).ReadAsync(DefaultCancellationToken);

        Assert.False(prepared.Resumed);
        Assert.NotNull(decoded);
        Assert.Equal(3, decoded.TargetReplicaCount);
        Assert.Equal(2UL, decoded.TargetGeneration);
        Assert.Equal(2, decoded.Groups.Count);
        Assert.Equal("group-a", decoded.Groups[0].GroupId);
        Assert.Equal("group-b", decoded.Groups[1].GroupId);
        Assert.All(decoded.Groups, static group => Assert.Equal(BootstrapGroupState.Pending, group.State));
        Assert.Equal(before, await File.ReadAllBytesAsync(sourcePath, DefaultCancellationToken));

        var resumed = await new BootstrapPlanner().PrepareAsync(Request(dir), DefaultCancellationToken);
        Assert.True(resumed.Resumed);
        Assert.Equal(prepared.Manifest.TargetGeneration, resumed.Manifest.TargetGeneration);
    }

    private static BootstrapPreparationRequest Request(string dataDirectory)
    {
        return new BootstrapPreparationRequest
        {
            GroupIds = ["group-a", "group-b"],
            LegacyOutcomes = [],
            Persistence = new PersistenceOptions { DataDir = dataDirectory },
            SourceMtls = new MtlsOptions { InternalListenPort = 7000 },
            SourceTopology = Topology(1, 1UL),
            TargetMtls = new MtlsOptions { InternalListenPort = 7000 },
            TargetTopology = Topology(3, 2UL),
        };
    }

    private static TopologyOptions Topology(int replicaCount, ulong generation)
    {
        var peers = new[]
        {
            Peer("node-a", 6001, 7001),
            Peer("node-b", 6002, 7002),
            Peer("node-c", 6003, 7003),
        };
        return new TopologyOptions(peers)
        {
            ClusterId = "cluster-a",
            ConfigurationGeneration = generation,
            NodeId = "node-a",
            ReplicaCount = replicaCount,
            Uri = peers[0].Uri,
            VirtualNodes = 128,
        };
    }

    private static ServerPeer Peer(string nodeId, int clientPort, int internalPort)
    {
        return new ServerPeer
        {
            InterNodeUri = new Uri($"https://127.0.0.1:{internalPort}"),
            NodeId = nodeId,
            Uri = new Uri($"https://127.0.0.1:{clientPort}"),
        };
    }
}
