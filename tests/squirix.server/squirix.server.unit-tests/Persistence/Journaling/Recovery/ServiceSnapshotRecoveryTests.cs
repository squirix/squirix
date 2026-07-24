using System;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Recovery;

/// <summary>Recovery with binary snapshots and missing snapshot path fallbacks.</summary>
public sealed class ServiceSnapshotRecoveryTests : ServerUnitTestBase
{
    /// <summary>Loads a binary snapshot watermark and replays only journal records after it.</summary>
    [Fact]
    public async Task BinarySnapshotRecoveryReplaysJournalTailWatermark()
    {
        await using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-binary-snapshot");
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 };
        var writer = StoreFactory.CreateWriter(persistence);
        var snapshotPath = await writer.WriteAsync(
            1,
            [(CacheKey.Default("base"), new NodeCacheEntry<object?> { Value = "from-snapshot", Version = 1 })],
            [],
            DefaultCancellationToken);

        const ulong snapshotSequence = 10UL;
        var baseRecord = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(snapshotSequence, "base", "ignored-by-snapshot");
        var tailRecord = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(11UL, "tail", "from-journal");
        await BinaryJournalTestSegmentWriter.WriteJournalSegmentAsync(scenario.DataDir, 1, [baseRecord, tailRecord]);

        await scenario.ManifestStore.WriteAsync(
            new State
            {
                Format = 1,
                CurrentJournal = 1,
                NextSequence = 12,
                LastSnapshot = new SnapshotRef
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
        var missingSnapshotPath = NodePathKit.Combine(
            scenario.DataDir,
            $"{FilePrefixes.Snapshot}{1.ToString("000000", CultureInfo.InvariantCulture)}{FileExtensions.Snapshot}");

        var record = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(1UL, "recovered", "yes");
        await BinaryJournalTestSegmentWriter.WriteJournalSegmentAsync(scenario.DataDir, 1, [record]);

        await scenario.ManifestStore.WriteAsync(
            new State
            {
                Format = 1,
                CurrentJournal = 1,
                NextSequence = 2,
                LastSnapshot = new SnapshotRef
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
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 };
        var recovery = new RecoveryService<object?>(
            new RecoveryOptions { BlockOnStart = true },
            NullLogger<RecoveryService<object?>>.Instance,
            new RecoveryDependencies<object?>(
                persistence,
                scenario.ManifestStore,
                scenario.Cache,
                gate,
                new RpcMutationIdempotencyStore(),
                StoreFactory.CreateReader(persistence)));
        return recovery.StartAsync(DefaultCancellationToken);
    }
}
