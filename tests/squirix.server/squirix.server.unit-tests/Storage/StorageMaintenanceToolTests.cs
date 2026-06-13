using System;
using System.Text.Json;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Maintenance;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Storage;

/// <summary>
/// Unit tests for <see cref="StorageMaintenanceTool" />.
/// </summary>
public sealed class StorageMaintenanceToolTests : ServerUnitTestBase, IAsyncLifetime
{
    private string _dir = null!;

    /// <summary>
    /// Inspect reports discontinuous journal segments using on-disk segment enumeration.
    /// </summary>
    [Fact]
    public void InspectReportsDiscontinuousJournalSegments()
    {
        FileKit.WriteAllText(PathKit.Combine(false, _dir, $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}"), string.Empty);
        FileKit.WriteAllText(PathKit.Combine(false, _dir, $"{StorageFilePrefixes.Journal}000003{StorageFileExtensions.Journal}"), string.Empty);

        var report = StorageMaintenanceTool.Inspect(_dir);

        Assert.Equal(_dir, report.DataDir);
        Assert.Equal([1, 3], report.JournalSegments);
        Assert.Contains(report.Issues, static issue => issue.Contains("discontinuous", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Inspect reports journal and manifest issues for a directory with missing CURRENT pointer.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task InspectReportsMissingCurrentPointer()
    {
        var options = new PersistenceOptions { DataDir = _dir };
        var manifestStore = new ManifestStore(options);

        await using (var journal = new JournalWriter(options, new Manifest(), manifestStore, new JournalStartupGate()))
        {
            await journal.AppendPutAsync(CacheKey.Default("k1"), BuildEntryJsonBytes("x"), null, DefaultCancellationToken);
            await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
        }

        FileKit.TryDelete(PathKit.Combine(false, _dir, "man-current"));

        var report = StorageMaintenanceTool.Inspect(_dir);

        Assert.Equal(_dir, report.DataDir);
        Assert.False(report.ManifestReadable);
        Assert.Contains(report.Issues, static issue => issue.Contains("CURRENT pointer", StringComparison.OrdinalIgnoreCase));
        Assert.Empty(report.SnapshotIndices);
        Assert.NotEmpty(report.JournalSegments);
    }

    /// <summary>
    /// Inspect reports when CURRENT points to a missing manifest file.
    /// </summary>
    [Fact]
    public void InspectReportsMissingCurrentPointerTarget()
    {
        FileKit.WriteAllText(PathKit.Combine(false, _dir, "man-current"), "man-000123.msqx");

        var report = StorageMaintenanceTool.Inspect(_dir);

        Assert.Equal(_dir, report.DataDir);
        Assert.True(report.CurrentPointerExists);
        Assert.Equal("man-000123.msqx", report.CurrentPointerTarget);
        Assert.Contains(report.Issues, static issue => issue.Contains("target is missing", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Inspect reports when manifest metadata references a missing snapshot file.
    /// </summary>
    [Fact]
    public void InspectReportsMissingSnapshotPath()
    {
        var options = new PersistenceOptions { DataDir = _dir };
        var manifestStore = new ManifestStore(options);
        var snapshotPath = PathKit.Combine(false, _dir, "snp-000001.ssqx");
        manifestStore.Write(
            new Manifest
            {
                CurrentJournal = 1,
                NextSequence = 10,
                LastSnapshot = new Manifest.SnapshotRef
                {
                    Index = 1,
                    Path = snapshotPath,
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = 9,
                    ReplayFromJournalSegment = 1,
                },
            });

        var report = StorageMaintenanceTool.Inspect(_dir);

        Assert.Equal(_dir, report.DataDir);
        Assert.True(report.ManifestReadable);
        Assert.Empty(report.SnapshotIndices);
        Assert.Equal(snapshotPath, report.LastSnapshotPath);
        Assert.Contains(report.Issues, static issue => issue.Contains("snapshot path is missing", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Inspect flags a CURRENT file that contains only whitespace as unreadable metadata.
    /// </summary>
    [Fact]
    public void InspectReportsWhitespaceOnlyCurrentPointer()
    {
        FileKit.WriteAllText(PathKit.Combine(false, _dir, "man-current"), "   \r\n  ");

        var report = StorageMaintenanceTool.Inspect(_dir);

        Assert.True(report.CurrentPointerExists);
        Assert.False(report.ManifestReadable);
        Assert.Contains(report.Issues, static issue => issue.Contains("empty", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Repair preserves snapshot watermark metadata when manifest is still readable.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task RepairPreservesReadableSnapshotWatermark()
    {
        var options = new PersistenceOptions { DataDir = _dir };
        var manifestStore = new ManifestStore(options);
        var snapshotWriter = new SnapshotWriter(_dir);
        var snapshotPath = await snapshotWriter.WriteAsync(2, [("k1", BuildEntryJsonElement("v1"))], DefaultCancellationToken);
        const ulong watermark = 77UL;

        manifestStore.Write(
            new Manifest
            {
                CurrentJournal = 4,
                NextSequence = watermark + 10,
                LastSnapshot = new Manifest.SnapshotRef
                {
                    Index = 2,
                    Path = snapshotPath,
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = watermark,
                    ReplayFromJournalSegment = 3,
                },
            });

        var result = await StorageMaintenanceTool.RepairAsync(_dir, DefaultCancellationToken);

        Assert.True(result.Report.ManifestReadable);
        Assert.Equal(watermark, result.Report.LastAppliedSequence);
        Assert.Equal(3, result.Report.ReplayFromJournalSegment);
        Assert.Equal(2, result.Report.LastSnapshotIndex);
    }

    /// <summary>
    /// Repair recreates manifest/CURRENT metadata from journal-only state.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task RepairRebuildsManifestFromJournalOnlyState()
    {
        var options = new PersistenceOptions { DataDir = _dir };
        var manifestStore = new ManifestStore(options);

        await using (var journal = new JournalWriter(options, new Manifest(), manifestStore, new JournalStartupGate()))
        {
            await journal.AppendPutAsync(CacheKey.Default("k1"), BuildEntryJsonBytes("x"), null, DefaultCancellationToken);
            await journal.AppendPutAsync(CacheKey.Default("k2"), BuildEntryJsonBytes("y"), null, DefaultCancellationToken);
            await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
        }

        FileKit.TryDelete(PathKit.Combine(false, _dir, "man-current"));

        var result = await StorageMaintenanceTool.RepairAsync(_dir, DefaultCancellationToken);

        Assert.Equal("repair", result.Action);
        Assert.True(result.Report.CurrentPointerExists);
        Assert.True(result.Report.ManifestReadable);
        Assert.Equal(result.Report.JournalSegments[^1], result.Report.CurrentJournal);
        Assert.True(result.Report.NextSequence > 1);
    }

    /// <summary>
    /// Repair recreates snapshot-only manifest metadata when CURRENT is missing and no journal remains.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task RepairRebuildsSnapshotOnlyManifest()
    {
        var snapshotWriter = new SnapshotWriter(_dir);
        var snapshotPath = await snapshotWriter.WriteAsync(3, [("k1", BuildEntryJsonElement("snap"))], DefaultCancellationToken);

        var result = await StorageMaintenanceTool.RepairAsync(_dir, DefaultCancellationToken);

        Assert.Equal("repair", result.Action);
        Assert.True(result.Report.ManifestReadable);
        Assert.Equal(3, result.Report.LastSnapshotIndex);
        Assert.Equal(snapshotPath, result.Report.LastSnapshotPath);
        Assert.Empty(result.Report.JournalSegments);
        Assert.Equal(1, result.Report.CurrentJournal);
        Assert.Equal(1UL, result.Report.NextSequence);
    }

    /// <summary>
    /// Deletes the temporary working directory after each test.
    /// </summary>
    /// <returns>A completed disposal task.</returns>
    public ValueTask DisposeAsync()
    {
        DirectoryKit.TryDeleteDirectory(_dir);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Creates a fresh temporary working directory for each test.
    /// </summary>
    /// <returns>A completed initialization task.</returns>
    public ValueTask InitializeAsync()
    {
        _dir = DirectoryKit.CreateTempDirectory("squirix");
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Builds a discriminated entry payload for journal append helpers.
    /// </summary>
    /// <param name="value">Value to encode into the entry JSON.</param>
    /// <returns>UTF-8 JSON payload formatted for durability components.</returns>
    private static byte[] BuildEntryJsonBytes(object? value) => DiscriminatedEntryJsonWriter.BuildEntryJson(value, null, null, 1, null);

    /// <summary>
    /// Builds a snapshot entry JSON element for snapshot writer helpers.
    /// </summary>
    /// <param name="value">Value to encode into the snapshot entry JSON.</param>
    /// <returns>A detached JSON element representing the encoded entry.</returns>
    private static JsonElement BuildEntryJsonElement(object? value)
    {
        var bytes = DiscriminatedEntryJsonWriter.BuildEntryJson(value, null, null, 1, null);
        using var doc = JsonDocument.Parse(bytes);
        return doc.RootElement.Clone();
    }
}
