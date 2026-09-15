using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Replication;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Replication;

/// <summary>Stopped RF=1 bootstrap validation and durable manifest behavior.</summary>
public sealed class BootstrapPlannerTests : ServerUnitTestBase
{
    /// <summary>Preparation publishes a readable checksummed manifest without changing source data.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CreatesManifestAndPreservesSource(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-create");
        var sourcePath = Path.Join(dir, "journal-000001.sqr");
        await File.WriteAllBytesAsync(sourcePath, [1, 3, 3, 7], cancellationToken);
        var before = await File.ReadAllBytesAsync(sourcePath, cancellationToken);

        var result = await new BootstrapPlanner().PrepareAsync(Request(dir), cancellationToken);
        var decoded = await new BootstrapManifestStore(dir).ReadAsync(cancellationToken);

        _ = await Assert.That(result.Resumed).IsFalse();
        _ = await Assert.That(decoded).IsNotNull();
        const ushort expectedFormatVersion = 1;
        _ = await Assert.That(decoded.FormatVersion).IsEqualTo(expectedFormatVersion);
        _ = await Assert.That(decoded.TargetReplicaCount).IsEqualTo(3);
        _ = await Assert.That(decoded.TargetGeneration).IsEqualTo(2UL);
        _ = await Assert.That(decoded.Groups).All(static group => group.State == BootstrapGroupState.Pending);
        var after = await File.ReadAllBytesAsync(sourcePath, cancellationToken);
        await SequenceAssert.Equal(before, after);
    }

    /// <summary>A different generation and a corrupted manifest both fail closed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RejectsDifferentOrCorruptManifest(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-reject");
        var planner = new BootstrapPlanner();
        var prepared = await planner.PrepareAsync(Request(dir), cancellationToken);
        var different = Request(dir, true, 1, 3, 3UL);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(planner.PrepareAsync(different, cancellationToken)));

        var bytes = await File.ReadAllBytesAsync(prepared.ManifestPath, cancellationToken);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(prepared.ManifestPath, bytes, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(planner.PrepareAsync(Request(dir), cancellationToken)));
    }

    /// <summary>Changing a fingerprint input other than RF and generation is rejected.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RejectsTopologyInputChanges(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-topology");
        var request = Request(dir);
        request = request.WithTarget(Topology(3, 2UL, 256));

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(new BootstrapPlanner().PrepareAsync(request, cancellationToken)));
    }

    /// <summary>Unscoped legacy outcomes report the earliest time at which all blockers have expired.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RejectsUnscopedLegacyOutcomes(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-legacy");
        var retry = DateTimeOffset.UtcNow.AddHours(2);
        var request = Request(dir, true, 1, 3, 2UL, [new BootstrapLegacyOutcome(false, retry)]);

        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(new BootstrapPlanner().PrepareAsync(request, cancellationToken)));

        _ = await Assert.That(exception.Message).Contains(retry.ToString("O"), StringComparison.Ordinal);
        _ = await Assert.That(File.Exists(Path.Join(dir, "bootstrap.manifest"))).IsFalse();
    }

    /// <summary>An existing exclusive owner proves the cluster is not stopped and blocks preparation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RequiresExclusiveDirectoryOwnership(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-lock");
        var lockPath = Path.Join(dir, "bootstrap.lock");
        using var ownership = File.OpenHandle(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(new BootstrapPlanner().PrepareAsync(Request(dir), cancellationToken)));
    }

    /// <summary>Persistence, RF=1 source, RF&gt;1 target, and generation increase are mandatory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RequiresRfAndPersistenceInvariants(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-invariants");
        var planner = new BootstrapPlanner();
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(planner.PrepareAsync(Request(dir, false), cancellationToken)));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(planner.PrepareAsync(Request(dir, true, 2), cancellationToken)));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(planner.PrepareAsync(Request(dir, true, 1, 1), cancellationToken)));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(planner.PrepareAsync(Request(dir, true, 1, 3, 1UL), cancellationToken)));
    }

    /// <summary>An identical rerun resumes the same generation and leaves manifest bytes unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SameTargetResumesManifest(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-resume");
        var planner = new BootstrapPlanner();
        var first = await planner.PrepareAsync(Request(dir), cancellationToken);
        var before = await File.ReadAllBytesAsync(first.ManifestPath, cancellationToken);

        var resumed = await planner.PrepareAsync(Request(dir), cancellationToken);

        _ = await Assert.That(resumed.Resumed).IsTrue();
        _ = await Assert.That(resumed.Manifest.TargetGeneration).IsEqualTo(first.Manifest.TargetGeneration);
        var manifestBytes = await File.ReadAllBytesAsync(first.ManifestPath, cancellationToken);
        await SequenceAssert.Equal(before, manifestBytes);
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

    private static BootstrapPreparationRequest Request(
        string dataDirectory,
        bool persistence = true,
        int sourceReplicaCount = 1,
        int targetReplicaCount = 3,
        ulong targetGeneration = 2UL,
        BootstrapLegacyOutcome[]? legacyOutcomes = null)
    {
        return new BootstrapPreparationRequest
        {
            GroupIds = ["group-a", "group-b"],
            LegacyOutcomes = legacyOutcomes ?? [],
            Persistence = persistence ? new PersistenceOptions { DataDir = dataDirectory } : null,
            SourceMtls = new MtlsOptions { InternalListenPort = 7000 },
            SourceTopology = Topology(sourceReplicaCount, 1UL),
            TargetMtls = new MtlsOptions { InternalListenPort = 7000 },
            TargetTopology = Topology(targetReplicaCount, targetGeneration),
        };
    }

    private static TopologyOptions Topology(int replicaCount, ulong generation, int virtualNodes = 128)
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
            VirtualNodes = virtualNodes,
        };
    }
}
