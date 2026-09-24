using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Verifies offline replica diagnostics report fingerprint mismatch and group lag.</summary>
[Immutable]
public sealed class DoctorReplicaStatusTests : ServerUnitTestBase
{
    /// <summary>Verifies the public doctor facade builds a report from server options.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublicFacadeBuildsReport(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-facade");

        var report = await ReplicaDoctor.BuildReportAsync(CreateOptions(), dir, cancellationToken);

        _ = await Assert.That(report.HasMismatch).IsFalse();
        _ = await Assert.That(string.Join('\n', report.Lines)).Contains("not activated", StringComparison.Ordinal);
    }

    /// <summary>Verifies missing report inputs are rejected.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RejectsNullArguments(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-guards");
        const string? missingHex = null;
        const IReadOnlyList<string>? missingGroups = null;
        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentNullException>(BuildReportAsync(missingHex!, 5, 2, ["n1"], dir, cancellationToken));
        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentNullException>(BuildReportAsync(ExpectedHex(), 5, 2, missingGroups!, dir, cancellationToken));
    }

    /// <summary>Verifies an aligned stamp and generation report a match without mismatch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReportsAlignedTopologyAsMatch(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-match");
        var options = CreateOptions();
        var mtls = new MtlsOptions();
        await PublishStampAsync(dir, CorrectFingerprintBytes(options, mtls), 5, 2, cancellationToken);

        var report = await BuildReportAsync(options, mtls, dir, cancellationToken);

        _ = await Assert.That(report.HasMismatch).IsFalse();
        var text = string.Join('\n', report.Lines);
        _ = await Assert.That(text).Contains("fingerprint match", StringComparison.Ordinal);
        _ = await Assert.That(text).Contains("generation match", StringComparison.Ordinal);
        _ = await Assert.That(text).Contains("replica count match", StringComparison.Ordinal);
    }

    /// <summary>Verifies a corrupt stamp reports an unreadable identity as mismatch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReportsCorruptStampAsMismatch(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-corrupt");
        var store = new ActivatedTopologyStampStore(dir);
        await File.WriteAllTextAsync(store.StampPath, "corrupt", cancellationToken);

        var report = await BuildReportAsync(CreateOptions(), new MtlsOptions(), dir, cancellationToken);

        _ = await Assert.That(report.HasMismatch).IsTrue();
        _ = await Assert.That(string.Join('\n', report.Lines)).Contains("UNREADABLE", StringComparison.Ordinal);
    }

    /// <summary>Verifies a stamped fingerprint disagreeing with settings reports mismatch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReportsFingerprintMismatch(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-mismatch");
        var options = CreateOptions();
        var mtls = new MtlsOptions();
        await PublishStampAsync(dir, WrongFingerprintBytes(options, mtls), 5, 2, cancellationToken);

        var report = await BuildReportAsync(options, mtls, dir, cancellationToken);

        _ = await Assert.That(report.HasMismatch).IsTrue();
        _ = await Assert.That(string.Join('\n', report.Lines)).Contains("fingerprint MISMATCH", StringComparison.Ordinal);
        _ = await Assert.That(string.Join('\n', report.Lines)).Contains("group 'n1': no durable state", StringComparison.Ordinal);
        _ = await Assert.That(string.Join('\n', report.Lines)).Contains("group 'n2': no durable state", StringComparison.Ordinal);
    }

    /// <summary>Verifies a stamped generation disagreeing with settings reports mismatch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReportsGenerationMismatch(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-generation");
        var options = CreateOptions();
        var mtls = new MtlsOptions();
        await PublishStampAsync(dir, CorrectFingerprintBytes(options, mtls), 6, 2, cancellationToken);

        var report = await BuildReportAsync(options, mtls, dir, cancellationToken);

        _ = await Assert.That(report.HasMismatch).IsTrue();
        _ = await Assert.That(string.Join('\n', report.Lines)).Contains("generation MISMATCH", StringComparison.Ordinal);
    }

    /// <summary>Verifies durable group metadata reports term, commit, applied lag, and mismatch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReportsGroupFingerprintMismatch(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-group");
        var options = CreateOptions();
        var mtls = new MtlsOptions();
        await PublishStampAsync(dir, CorrectFingerprintBytes(options, mtls), 5, 2, cancellationToken);
        await WriteGroupMetadataAsync(dir, "n1", new ReadOnlyMemory<byte>(WrongFingerprintBytes(options, mtls)), 5, cancellationToken);

        var beforePaths = Directory.GetFiles(dir, "*", SearchOption.AllDirectories);
        Array.Sort(beforePaths, StringComparer.Ordinal);
        var beforeContents = new string[beforePaths.Length];
        for (var i = 0; i < beforePaths.Length; i++)
            beforeContents[i] = Convert.ToHexString(await File.ReadAllBytesAsync(beforePaths[i], cancellationToken));

        var report = await BuildReportAsync(options, mtls, dir, cancellationToken);

        _ = await Assert.That(report.HasMismatch).IsTrue();
        var text = string.Join('\n', report.Lines);
        _ = await Assert.That(text).Contains("group 'n1': term 9 commit 10 applied 7 apply-lag 3", StringComparison.Ordinal);
        _ = await Assert.That(text).Contains("fingerprint MISMATCH", StringComparison.Ordinal);
        _ = await Assert.That(text).Contains("generation match", StringComparison.Ordinal);
        _ = await Assert.That(text).Contains("group 'n2': no durable state", StringComparison.Ordinal);

        // Diagnostics are read-only: the durable file set and contents are unchanged.
        var afterPaths = Directory.GetFiles(dir, "*", SearchOption.AllDirectories);
        Array.Sort(afterPaths, StringComparer.Ordinal);
        _ = await Assert.That(afterPaths.Length).IsEqualTo(beforePaths.Length);
        for (var i = 0; i < afterPaths.Length; i++)
        {
            _ = await Assert.That(afterPaths[i]).IsEqualTo(beforePaths[i]);
            _ = await Assert.That(Convert.ToHexString(await File.ReadAllBytesAsync(afterPaths[i], cancellationToken))).IsEqualTo(beforeContents[i]);
        }
    }

    /// <summary>Verifies a missing stamp reports an inactive replica set without mismatch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReportsMissingStampAsInactive(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-inactive");
        var options = CreateOptions();

        var report = await BuildReportAsync(options, new MtlsOptions(), dir, cancellationToken);

        _ = await Assert.That(report.HasMismatch).IsFalse();
        _ = await Assert.That(string.Join('\n', report.Lines)).Contains("not activated", StringComparison.Ordinal);
    }

    /// <summary>Verifies a stamped replica count disagreeing with settings reports mismatch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReportsReplicaCountMismatch(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-count");
        var options = CreateOptions();
        var mtls = new MtlsOptions();
        await PublishStampAsync(dir, CorrectFingerprintBytes(options, mtls), 5, 3, cancellationToken);

        var report = await BuildReportAsync(options, mtls, dir, cancellationToken);

        _ = await Assert.That(report.HasMismatch).IsTrue();
        _ = await Assert.That(string.Join('\n', report.Lines)).Contains("replica count MISMATCH", StringComparison.Ordinal);
    }

    /// <summary>Verifies undecodable group metadata reports an unreadable group as mismatch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReportsUnreadableGroupMetadata(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-unreadable");
        var options = CreateOptions();
        var mtls = new MtlsOptions();
        await PublishStampAsync(dir, CorrectFingerprintBytes(options, mtls), 5, 2, cancellationToken);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, "n1"));
        await File.WriteAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, "n1"), ReadOnlyMemory<byte>.Of(1, 2, 3), cancellationToken);

        var report = await BuildReportAsync(options, mtls, dir, cancellationToken);

        _ = await Assert.That(report.HasMismatch).IsTrue();
        _ = await Assert.That(string.Join('\n', report.Lines)).Contains("group 'n1': metadata UNREADABLE", StringComparison.Ordinal);
    }

    /// <summary>Builds a replica doctor report from server options.</summary>
    /// <param name="options">The server options.</param>
    /// <param name="mtls">The mutual TLS options.</param>
    /// <param name="dir">The replica working directory.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private static async Task<ReplicaDoctorReport> BuildReportAsync(SquirixServerOptions options, MtlsOptions mtls, string dir, CancellationToken cancellationToken)
    {
        var topology = Configurator.ToClusterConfig(options);
        var groupIds = new string[topology.Peers.Count];
        for (var i = 0; i < groupIds.Length; i++)
            groupIds[i] = topology.Peers[i].NodeId;

        var (hasMismatch, lines) = await ReplicaDoctorReportBuilder.BuildAsync(
            TopologyFingerprint.CreateFromTopology(topology, mtls).ToString(),
            topology.ConfigurationGeneration,
            topology.ReplicaCount,
            groupIds,
            dir,
            cancellationToken);
        return new ReplicaDoctorReport(hasMismatch, lines);
    }

    /// <summary>Builds a replica doctor report from explicit inputs.</summary>
    /// <param name="expectedHex">The expected topology fingerprint.</param>
    /// <param name="generation">The configuration generation.</param>
    /// <param name="replicaCount">The replica count.</param>
    /// <param name="groupIds">The group identifiers.</param>
    /// <param name="dir">The replica working directory.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private static Task<(bool HasMismatch, List<string> Lines)> BuildReportAsync(
        string expectedHex,
        ulong generation,
        int replicaCount,
        IReadOnlyList<string> groupIds,
        string dir,
        CancellationToken cancellationToken) => ReplicaDoctorReportBuilder.BuildAsync(expectedHex, generation, replicaCount, groupIds, dir, cancellationToken);

    private static byte[] CorrectFingerprintBytes(SquirixServerOptions options, MtlsOptions mtls)
    {
        var fingerprint = TopologyFingerprint.CreateFromTopology(Configurator.ToClusterConfig(options), mtls);
        var bytes = new byte[fingerprint.Bytes.Length];
        fingerprint.Bytes.CopyTo(bytes);
        return bytes;
    }

    private static SquirixServerOptions CreateOptions() => new()
    {
        ClusterId = "doctor-c",
        NodeId = "n1",
        Uri = new Uri("https://localhost:6121"),
        ReplicaCount = 2,
        ConfigurationGeneration = 5,
        PersistenceEnabled = true,
        ReplicationEnabled = true,
        Peers =
        [
            new SquirixServerPeerOptions { NodeId = "n1", Uri = new Uri("https://localhost:6121") },
            new SquirixServerPeerOptions { NodeId = "n2", Uri = new Uri("https://localhost:6122") },
        ],
    };

    private static string ExpectedHex()
    {
        var options = CreateOptions();
        var topology = Configurator.ToClusterConfig(options);
        return TopologyFingerprint.CreateFromTopology(topology, new MtlsOptions()).ToString();
    }

    /// <summary>Publishes an activated topology stamp for the replica directory.</summary>
    /// <param name="dir">The replica working directory.</param>
    /// <param name="fingerprint">The topology fingerprint bytes.</param>
    /// <param name="generation">The configuration generation.</param>
    /// <param name="replicaCount">The replica count.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private static Task PublishStampAsync(string dir, byte[] fingerprint, ulong generation, int replicaCount, CancellationToken cancellationToken)
    {
        var store = new ActivatedTopologyStampStore(dir);
        return store.PublishAsync(
            new ActivatedTopologyStamp { Generation = generation, Fingerprint = new ReadOnlyMemory<byte>(fingerprint), ReplicaCount = replicaCount },
            cancellationToken);
    }

    /// <summary>Writes group log metadata for the replica directory.</summary>
    /// <param name="dir">The replica working directory.</param>
    /// <param name="groupId">The group identifier.</param>
    /// <param name="fingerprint">The topology fingerprint.</param>
    /// <param name="generation">The configuration generation.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private static Task WriteGroupMetadataAsync(string dir, string groupId, ReadOnlyMemory<byte> fingerprint, ulong generation, CancellationToken cancellationToken)
    {
        var meta = new GroupLogMetadata(groupId, fingerprint, generation, 9, string.Empty, 12, 10, 7);
        var buffer = new byte[GroupLogCodec.ComputeMetaEncodedLength(meta)];
        GroupLogCodec.EncodeMeta(meta, buffer);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, groupId));
        return File.WriteAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, groupId), buffer, cancellationToken);
    }

    private static byte[] WrongFingerprintBytes(SquirixServerOptions options, MtlsOptions mtls)
    {
        var bytes = CorrectFingerprintBytes(options, mtls);
        bytes[0] ^= 0xFF;
        return bytes;
    }
}
