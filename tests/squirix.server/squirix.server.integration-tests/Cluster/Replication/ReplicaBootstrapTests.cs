using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Replication;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Offline RF=1 to RF&gt;1 bootstrap seeding through the durable manifest.</summary>
public sealed class ReplicaBootstrapTests : NodeIntegrationTestBase
{
    /// <summary>Bootstrap preparation seeds replica groups with pending state and preserves source data.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OfflineRfOneBootstrapSeedsReplicaGroups(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-seed");
        var sourcePath = Path.Join(dir, "journal-000001.sqr");
        await File.WriteAllBytesAsync(sourcePath, [1, 3, 3, 7], cancellationToken);
        var before = await File.ReadAllBytesAsync(sourcePath, cancellationToken);

        var prepared = await new BootstrapPlanner().PrepareAsync(Request(dir), cancellationToken);
        var decoded = await new BootstrapManifestStore(dir).ReadAsync(cancellationToken);

        _ = await Assert.That(prepared.Resumed).IsFalse();
        _ = await Assert.That(decoded).IsNotNull();
        _ = await Assert.That(decoded.TargetReplicaCount).IsEqualTo(3);
        _ = await Assert.That(decoded.TargetGeneration).IsEqualTo(2UL);
        _ = await Assert.That(decoded.Groups.Count).IsEqualTo(2);
        _ = await Assert.That(decoded.Groups[0].GroupId).IsEqualTo("group-a");
        _ = await Assert.That(decoded.Groups[1].GroupId).IsEqualTo("group-b");
        _ = await Assert.That(decoded.Groups).All(static group => group.State == BootstrapGroupState.Pending);
        await SequenceAssert.EqualAsync(before, await File.ReadAllBytesAsync(sourcePath, cancellationToken));

        var resumed = await new BootstrapPlanner().PrepareAsync(Request(dir), cancellationToken);
        _ = await Assert.That(resumed.Resumed).IsTrue();
        _ = await Assert.That(resumed.Manifest.TargetGeneration).IsEqualTo(prepared.Manifest.TargetGeneration);
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
}
