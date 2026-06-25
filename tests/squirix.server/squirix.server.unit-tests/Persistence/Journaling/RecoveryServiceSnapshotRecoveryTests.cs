using System;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Recovery with binary snapshots and missing snapshot path fallbacks.</summary>
public sealed class RecoveryServiceSnapshotRecoveryTests : UnitTestBase
{
    /// <summary>Loads a binary snapshot watermark and replays only journal records after it.</summary>
    [Fact]
    public async Task BinarySnapshotRecoveryReplaysJournalTailAfterWatermark()
    {
        await using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-binary-snapshot");
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 };
        var writer = SnapshotStoreFactory.CreateWriter(persistence);
        var snapshotPath = await writer.WriteAsync(
            1,
            [(CacheKey.Default("base"), new CacheEntry<object?> { Value = "from-snapshot", Version = 1 })],
            [],
            DefaultCancellationToken);

        const ulong snapshotSequence = 10UL;
        var baseRecord = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(snapshotSequence, "base", "ignored-by-snapshot");
        var tailRecord = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(11UL, "tail", "from-journal");
        await BinaryJournalTestSegmentWriter.WriteJournalSegmentAsync(scenario.DataDir, 1, [baseRecord, tailRecord]);

        await scenario.ManifestStore.WriteAsync(
            new ManifestState
            {
                Format = 1,
                CurrentJournal = 1,
                NextSequence = 12,
                LastSnapshot = new ManifestState.SnapshotRef
                {
                    Index = 1,
                    Path = snapshotPath,
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = snapshotSequence,
                    ReplayFromJournalSegment = 1,
                },
            },
            DefaultCancellationToken);

        await RunRecoveryAsync(scenario);

        var baseEntry = await scenario.Cache.GetValueAsync(CacheKey.Default("base"), DefaultCancellationToken);
        Assert.True(baseEntry.Found);
        Assert.Equal("from-snapshot", baseEntry.Value);

        var tailEntry = await scenario.Cache.GetValueAsync(CacheKey.Default("tail"), DefaultCancellationToken);
        Assert.True(tailEntry.Found);
        Assert.Equal("from-journal", tailEntry.Value);
    }

    /// <summary>Manifest pointing at a missing snapshot path falls back to journal-only recovery.</summary>
    [Fact]
    public async Task MissingSnapshotPathFallsBackToJournalOnlyRecovery()
    {
        await using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-missing-snapshot");
        var missingSnapshotPath = PathKit.Combine(
            scenario.DataDir,
            $"{StorageFilePrefixes.Snapshot}{1.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Snapshot}");

        var record = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(1UL, "recovered", "yes");
        await BinaryJournalTestSegmentWriter.WriteJournalSegmentAsync(scenario.DataDir, 1, [record]);

        await scenario.ManifestStore.WriteAsync(
            new ManifestState
            {
                Format = 1,
                CurrentJournal = 1,
                NextSequence = 2,
                LastSnapshot = new ManifestState.SnapshotRef
                {
                    Index = 1,
                    Path = missingSnapshotPath,
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = 99,
                    ReplayFromJournalSegment = 1,
                },
            },
            DefaultCancellationToken);

        await RunRecoveryAsync(scenario);

        var recovered = await scenario.Cache.GetValueAsync(CacheKey.Default("recovered"), DefaultCancellationToken);
        Assert.True(recovered.Found);
        Assert.Equal("yes", recovered.Value);
    }

    private static Task RunRecoveryAsync(RecoveryScenarioBuilder scenario)
    {
        var gate = new JournalStartupGate(false);
        var recovery = new RecoveryService<object?>(
            new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 },
            scenario.ManifestStore,
            scenario.Cache,
            new RecoveryOptions { BlockOnStart = true },
            gate,
            NullLogger<RecoveryService<object?>>.Instance);
        return recovery.StartAsync(DefaultCancellationToken);
    }
}
