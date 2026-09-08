using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Verifies offline replica diagnostics report fingerprint mismatch and group lag.</summary>
[Immutable]
public sealed class DoctorReplicaStatusTests : ServerUnitTestBase
{
    /// <summary>Verifies a stamped fingerprint disagreeing with settings reports mismatch.</summary>
    [Fact]
    public async Task ReportsFingerprintMismatch()
    {
        using var dir = new TempDirectory("squirix-doctor-mismatch");
        var options = CreateOptions();
        var mtls = new MtlsOptions();
        await PublishStampAsync(dir.Path, WrongFingerprintBytes(options, mtls), 5, 2);

        var report = await BuildReportAsync(options, mtls, dir.Path);

        Assert.True(report.HasMismatch);
        Assert.Contains("fingerprint MISMATCH", string.Join('\n', report.Lines), StringComparison.Ordinal);
        Assert.Contains("group 'n1': no durable state", string.Join('\n', report.Lines), StringComparison.Ordinal);
        Assert.Contains("group 'n2': no durable state", string.Join('\n', report.Lines), StringComparison.Ordinal);
    }

    /// <summary>Verifies an aligned stamp and generation report a match without mismatch.</summary>
    [Fact]
    public async Task ReportsAlignedTopologyAsMatch()
    {
        using var dir = new TempDirectory("squirix-doctor-match");
        var options = CreateOptions();
        var mtls = new MtlsOptions();
        await PublishStampAsync(dir.Path, CorrectFingerprintBytes(options, mtls), 5, 2);

        var report = await BuildReportAsync(options, mtls, dir.Path);

        Assert.False(report.HasMismatch);
        var text = string.Join('\n', report.Lines);
        Assert.Contains("fingerprint match", text, StringComparison.Ordinal);
        Assert.Contains("generation match", text, StringComparison.Ordinal);
        Assert.Contains("replica count match", text, StringComparison.Ordinal);
    }

    /// <summary>Verifies a missing stamp reports an inactive replica set without mismatch.</summary>
    [Fact]
    public async Task ReportsMissingStampAsInactive()
    {
        using var dir = new TempDirectory("squirix-doctor-inactive");
        var options = CreateOptions();

        var report = await BuildReportAsync(options, new MtlsOptions(), dir.Path);

        Assert.False(report.HasMismatch);
        Assert.Contains("not activated", string.Join('\n', report.Lines), StringComparison.Ordinal);
    }

    /// <summary>Verifies a corrupt stamp reports an unreadable identity as mismatch.</summary>
    [Fact]
    public async Task ReportsCorruptStampAsMismatch()
    {
        using var dir = new TempDirectory("squirix-doctor-corrupt");
        var store = new ActivatedTopologyStampStore(dir.Path);
        await File.WriteAllTextAsync(store.StampPath, "corrupt", DefaultCancellationToken);

        var report = await BuildReportAsync(CreateOptions(), new MtlsOptions(), dir.Path);

        Assert.True(report.HasMismatch);
        Assert.Contains("UNREADABLE", string.Join('\n', report.Lines), StringComparison.Ordinal);
    }

    /// <summary>Verifies durable group metadata reports term, commit, applied lag, and mismatch.</summary>
    [Fact]
    public async Task ReportsGroupFingerprintMismatch()
    {
        using var dir = new TempDirectory("squirix-doctor-group");
        var options = CreateOptions();
        var mtls = new MtlsOptions();
        await PublishStampAsync(dir.Path, CorrectFingerprintBytes(options, mtls), 5, 2);
        await WriteGroupMetadataAsync(dir.Path, "n1", new ReadOnlyMemory<byte>(WrongFingerprintBytes(options, mtls)), 5);

        var beforePaths = Directory.GetFiles(dir.Path, "*", SearchOption.AllDirectories);
        Array.Sort(beforePaths, StringComparer.Ordinal);
        var beforeContents = new string[beforePaths.Length];
        for (var i = 0; i < beforePaths.Length; i++)
            beforeContents[i] = Convert.ToHexString(await File.ReadAllBytesAsync(beforePaths[i], DefaultCancellationToken));

        var report = await BuildReportAsync(options, mtls, dir.Path);

        Assert.True(report.HasMismatch);
        var text = string.Join('\n', report.Lines);
        Assert.Contains("group 'n1': term 9 commit 10 applied 7 apply-lag 3", text, StringComparison.Ordinal);
        Assert.Contains("fingerprint MISMATCH", text, StringComparison.Ordinal);
        Assert.Contains("generation match", text, StringComparison.Ordinal);
        Assert.Contains("group 'n2': no durable state", text, StringComparison.Ordinal);

        // Diagnostics are read-only: the durable file set and contents are unchanged.
        var afterPaths = Directory.GetFiles(dir.Path, "*", SearchOption.AllDirectories);
        Array.Sort(afterPaths, StringComparer.Ordinal);
        Assert.Equal(beforePaths.Length, afterPaths.Length);
        for (var i = 0; i < afterPaths.Length; i++)
        {
            Assert.Equal(beforePaths[i], afterPaths[i]);
            Assert.Equal(beforeContents[i], Convert.ToHexString(await File.ReadAllBytesAsync(afterPaths[i], DefaultCancellationToken)));
        }
    }

    /// <summary>Verifies a stamped generation disagreeing with settings reports mismatch.</summary>
    [Fact]
    public async Task ReportsGenerationMismatch()
    {
        using var dir = new TempDirectory("squirix-doctor-generation");
        var options = CreateOptions();
        var mtls = new MtlsOptions();
        await PublishStampAsync(dir.Path, CorrectFingerprintBytes(options, mtls), 6, 2);

        var report = await BuildReportAsync(options, mtls, dir.Path);

        Assert.True(report.HasMismatch);
        Assert.Contains("generation MISMATCH", string.Join('\n', report.Lines), StringComparison.Ordinal);
    }

    /// <summary>Verifies a stamped replica count disagreeing with settings reports mismatch.</summary>
    [Fact]
    public async Task ReportsReplicaCountMismatch()
    {
        using var dir = new TempDirectory("squirix-doctor-count");
        var options = CreateOptions();
        var mtls = new MtlsOptions();
        await PublishStampAsync(dir.Path, CorrectFingerprintBytes(options, mtls), 5, 3);

        var report = await BuildReportAsync(options, mtls, dir.Path);

        Assert.True(report.HasMismatch);
        Assert.Contains("replica count MISMATCH", string.Join('\n', report.Lines), StringComparison.Ordinal);
    }

    /// <summary>Verifies undecodable group metadata reports an unreadable group as mismatch.</summary>
    [Fact]
    public async Task ReportsUnreadableGroupMetadata()
    {
        using var dir = new TempDirectory("squirix-doctor-unreadable");
        var options = CreateOptions();
        var mtls = new MtlsOptions();
        await PublishStampAsync(dir.Path, CorrectFingerprintBytes(options, mtls), 5, 2);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir.Path, "n1"));
        await File.WriteAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir.Path, "n1"), [1, 2, 3], DefaultCancellationToken);

        var report = await BuildReportAsync(options, mtls, dir.Path);

        Assert.True(report.HasMismatch);
        Assert.Contains("group 'n1': metadata UNREADABLE", string.Join('\n', report.Lines), StringComparison.Ordinal);
    }

    /// <summary>Verifies the public doctor facade builds a report from server options.</summary>
    [Fact]
    public async Task PublicFacadeBuildsReport()
    {
        using var dir = new TempDirectory("squirix-doctor-facade");

        var report = await ReplicaDoctor.BuildReportAsync(CreateOptions(), dir.Path, DefaultCancellationToken);

        Assert.False(report.HasMismatch);
        Assert.Contains("not activated", string.Join('\n', report.Lines), StringComparison.Ordinal);
    }

    /// <summary>Verifies missing report inputs are rejected.</summary>
    [Fact]
    public async Task RejectsNullArguments()
    {
        using var dir = new TempDirectory("squirix-doctor-guards");
        const string? missingHex = null;
        const IReadOnlyList<string>? missingGroups = null;
        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentNullException>(BuildReportAsync(missingHex!, 5, 2, ["n1"], dir.Path));
        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentNullException>(BuildReportAsync(ExpectedHex(), 5, 2, missingGroups!, dir.Path));
    }

    private static async Task<ReplicaDoctorReport> BuildReportAsync(SquirixServerOptions options, MtlsOptions mtls, string dir)
    {
        var topology = Configurator.ToClusterConfig(options);
        var groupIds = new string[topology.Peers.Length];
        for (var i = 0; i < groupIds.Length; i++)
            groupIds[i] = topology.Peers[i].NodeId;

        var (hasMismatch, lines) = await ReplicaDoctorReportBuilder.BuildAsync(
            TopologyFingerprint.CreateFromTopology(topology, mtls).ToString(),
            topology.ConfigurationGeneration,
            topology.ReplicaCount,
            groupIds,
            dir,
            DefaultCancellationToken);
        return new ReplicaDoctorReport(hasMismatch, lines);
    }

    private static SquirixServerOptions CreateOptions() => new()
    {
        ClusterId = "doctor-c",
        NodeId = "n1",
        Uri = new Uri("https://localhost:6121"),
        ReplicaCount = 2,
        ConfigurationGeneration = 5,
        PersistenceEnabled = true,
        Peers =
        [
            new SquirixServerPeerOptions { NodeId = "n1", Uri = new Uri("https://localhost:6121") },
            new SquirixServerPeerOptions { NodeId = "n2", Uri = new Uri("https://localhost:6122") },
        ],
    };

    private static Task<(bool HasMismatch, List<string> Lines)> BuildReportAsync(string expectedHex, ulong generation, int replicaCount, IReadOnlyList<string> groupIds, string dir) =>
        ReplicaDoctorReportBuilder.BuildAsync(expectedHex, generation, replicaCount, groupIds, dir, DefaultCancellationToken);

    private static string ExpectedHex()
    {
        var options = CreateOptions();
        var topology = Configurator.ToClusterConfig(options);
        return TopologyFingerprint.CreateFromTopology(topology, new MtlsOptions()).ToString();
    }

    private static byte[] CorrectFingerprintBytes(SquirixServerOptions options, MtlsOptions mtls)
    {
        var fingerprint = TopologyFingerprint.CreateFromTopology(Configurator.ToClusterConfig(options), mtls);
        var bytes = new byte[fingerprint.Bytes.Length];
        fingerprint.Bytes.CopyTo(bytes);
        return bytes;
    }

    private static byte[] WrongFingerprintBytes(SquirixServerOptions options, MtlsOptions mtls)
    {
        var bytes = CorrectFingerprintBytes(options, mtls);
        bytes[0] ^= 0xFF;
        return bytes;
    }

    private static Task PublishStampAsync(string dir, byte[] fingerprint, ulong generation, int replicaCount)
    {
        var store = new ActivatedTopologyStampStore(dir);
        return store.PublishAsync(
            new ActivatedTopologyStamp { Generation = generation, Fingerprint = new ReadOnlyMemory<byte>(fingerprint), ReplicaCount = replicaCount },
            DefaultCancellationToken);
    }

    private static Task WriteGroupMetadataAsync(string dir, string groupId, ReadOnlyMemory<byte> fingerprint, ulong generation)
    {
        var meta = new GroupLogMetadata(groupId, fingerprint, generation, 9, string.Empty, 12, 10, 7);
        var buffer = new byte[GroupLogCodec.ComputeMetaEncodedLength(meta)];
        GroupLogCodec.EncodeMeta(meta, buffer);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, groupId));
        return File.WriteAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, groupId), buffer, DefaultCancellationToken);
    }
}
