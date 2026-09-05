using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Replication;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Replication;

/// <summary>Stopped RF=1 bootstrap validation and durable manifest behavior.</summary>
public sealed class BootstrapPlannerTests : ServerUnitTestBase
{
    /// <summary>Preparation publishes a readable checksummed manifest without changing source data.</summary>
    [Fact]
    public async Task CreatesManifestAndPreservesSource()
    {
        using var dir = new TempDirectory("squirix-bootstrap-create");
        var sourcePath = Path.Join(dir, "journal-000001.sqr");
        await File.WriteAllBytesAsync(sourcePath, [1, 3, 3, 7], DefaultCancellationToken);
        var before = await File.ReadAllBytesAsync(sourcePath, DefaultCancellationToken);

        var result = await new BootstrapPlanner().PrepareAsync(Request(dir), DefaultCancellationToken);
        var decoded = await new BootstrapManifestStore(dir).ReadAsync(DefaultCancellationToken);

        Assert.False(result.Resumed);
        Assert.NotNull(decoded);
        Assert.Equal<ushort>(1, decoded.FormatVersion);
        Assert.Equal(3, decoded.TargetReplicaCount);
        Assert.Equal(2UL, decoded.TargetGeneration);
        Assert.All(decoded.Groups, static group => Assert.Equal(BootstrapGroupState.Pending, group.State));
        Assert.Equal(before, await File.ReadAllBytesAsync(sourcePath, DefaultCancellationToken));
    }

    /// <summary>An identical rerun resumes the same generation and leaves manifest bytes unchanged.</summary>
    [Fact]
    public async Task SameTargetResumesManifest()
    {
        using var dir = new TempDirectory("squirix-bootstrap-resume");
        var planner = new BootstrapPlanner();
        var first = await planner.PrepareAsync(Request(dir), DefaultCancellationToken);
        var before = await File.ReadAllBytesAsync(first.ManifestPath, DefaultCancellationToken);

        var resumed = await planner.PrepareAsync(Request(dir), DefaultCancellationToken);

        Assert.True(resumed.Resumed);
        Assert.Equal(first.Manifest.TargetGeneration, resumed.Manifest.TargetGeneration);
        Assert.Equal(before, await File.ReadAllBytesAsync(first.ManifestPath, DefaultCancellationToken));
    }

    /// <summary>A different generation and a corrupted manifest both fail closed.</summary>
    [Fact]
    public async Task RejectsDifferentOrCorruptManifest()
    {
        using var dir = new TempDirectory("squirix-bootstrap-reject");
        var planner = new BootstrapPlanner();
        var prepared = await planner.PrepareAsync(Request(dir), DefaultCancellationToken);
        var different = Request(dir, true, 1, 3, 3UL);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(planner.PrepareAsync(different, DefaultCancellationToken)));

        var bytes = await File.ReadAllBytesAsync(prepared.ManifestPath, DefaultCancellationToken);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(prepared.ManifestPath, bytes, DefaultCancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(planner.PrepareAsync(Request(dir), DefaultCancellationToken)));
    }

    /// <summary>Persistence, RF=1 source, RF&gt;1 target, and generation increase are mandatory.</summary>
    [Fact]
    public async Task RequiresRfAndPersistenceInvariants()
    {
        using var dir = new TempDirectory("squirix-bootstrap-invariants");
        var planner = new BootstrapPlanner();
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(planner.PrepareAsync(Request(dir, false), DefaultCancellationToken)));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(planner.PrepareAsync(Request(dir, true, 2), DefaultCancellationToken)));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(planner.PrepareAsync(Request(dir, true, 1, 1), DefaultCancellationToken)));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(planner.PrepareAsync(Request(dir, true, 1, 3, 1UL), DefaultCancellationToken)));
    }

    /// <summary>Changing a fingerprint input other than RF and generation is rejected.</summary>
    [Fact]
    public async Task RejectsTopologyInputChanges()
    {
        using var dir = new TempDirectory("squirix-bootstrap-topology");
        var request = Request(dir);
        request = request.WithTarget(Topology(3, 2UL, 256));

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(new BootstrapPlanner().PrepareAsync(request, DefaultCancellationToken)));
    }

    /// <summary>An existing exclusive owner proves the cluster is not stopped and blocks preparation.</summary>
    [Fact]
    public async Task RequiresExclusiveDirectoryOwnership()
    {
        using var dir = new TempDirectory("squirix-bootstrap-lock");
        var lockPath = Path.Join(dir, "bootstrap.lock");
        using SafeFileHandle ownership = File.OpenHandle(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(new BootstrapPlanner().PrepareAsync(Request(dir), DefaultCancellationToken)));
    }

    /// <summary>Unscoped legacy outcomes report the earliest time at which all blockers have expired.</summary>
    [Fact]
    public async Task RejectsUnscopedLegacyOutcomes()
    {
        using var dir = new TempDirectory("squirix-bootstrap-legacy");
        var retry = DateTimeOffset.UtcNow.AddHours(2);
        var request = Request(dir, true, 1, 3, 2UL, [new BootstrapLegacyOutcome("opaque", false, retry)]);

        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, BootstrapPreparationResult>(
            new ValueTask<BootstrapPreparationResult>(new BootstrapPlanner().PrepareAsync(request, DefaultCancellationToken)));

        Assert.Contains(retry.ToString("O"), exception.Message, StringComparison.Ordinal);
        Assert.False(File.Exists(Path.Join(dir, "bootstrap.manifest")));
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
