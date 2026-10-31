using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Recovery with binary snapshots and missing snapshot path fallbacks.</summary>
[Immutable]
public sealed class ServiceSnapshotRecoveryTests : DisposableServerUnitTestBase
{
    private const int SnapshotVersionOffset = 4;
    private const int JournalRecoveryFailedEventId = 2004;

    private readonly Meter _testMeter = new("test");

    /// <summary>Manifest pointing at a missing snapshot path falls back to journal-only recovery.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MissingSnapshotFallsBackToJournal(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-missing-snapshot");

        var record = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "recovered", "yes");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, record);

        var manifest = new State
        {
            CurrentJournal = 1,
            NextSequence = 2,
            LastSnapshot = new SnapshotRef
            {
                Index = 1,
                CreatedUtc = DateTime.UtcNow,
                LastAppliedSequence = 99,
                ReplayFromJournalSegment = 1,
            },
        };
        await scenario.Ledger.WriteAsync(manifest, cancellationToken);

        await RunRecoveryAsync(scenario, cancellationToken);

        var recovered = await scenario.Cache.GetValueAsync(CacheKey.Default("recovered"), cancellationToken);
        _ = await Assert.That(recovered.Found).IsTrue();
        _ = await Assert.That(recovered.Value).IsEqualTo("yes");
    }

    /// <summary>Loads a binary snapshot watermark and replays only journal records after it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotRecoveryReplaysJournalTail(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-binary-snapshot");
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16 };
        var writer = StoreFactory.CreateWriter(persistence);
        var e = new NodeCacheEntry<object?> { Value = "from-snapshot", Version = 1 };
        IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> readOnlyList = [(CacheKey.Default("base"), e)];
        _ = await writer.WriteAsync(1, readOnlyList, [], cancellationToken);

        const ulong snapshotSequence = 10UL;
        var baseRecord = BinaryJournalTestSegmentWriter.BuildPutRecord(snapshotSequence, "base", "ignored-by-snapshot");
        var tailRecord = BinaryJournalTestSegmentWriter.BuildPutRecord(11UL, "tail", "from-journal");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, [baseRecord, tailRecord]);

        var manifest = new State
        {
            CurrentJournal = 1,
            NextSequence = 12,
            LastSnapshot = new SnapshotRef
            {
                Index = 1,
                CreatedUtc = DateTime.UtcNow,
                LastAppliedSequence = snapshotSequence,
                ReplayFromJournalSegment = 1,
            },
        };
        await scenario.Ledger.WriteAsync(manifest, cancellationToken);

        await RunRecoveryAsync(scenario, cancellationToken);

        var entry = await scenario.Cache.GetValueAsync(CacheKey.Default("base"), cancellationToken);
        _ = await Assert.That(entry.Found).IsTrue();
        _ = await Assert.That(entry.Value).IsEqualTo("from-snapshot");

        var tailEntry = await scenario.Cache.GetValueAsync(CacheKey.Default("tail"), cancellationToken);
        _ = await Assert.That(tailEntry.Found).IsTrue();
        _ = await Assert.That(tailEntry.Value).IsEqualTo("from-journal");
    }

    /// <summary>Recovery loads the snapshot by its index from a data directory copied to a new location.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CopiedDirectoryRecoversSnapshot(CancellationToken cancellationToken)
    {
        using var source = RecoveryScenarioBuilder.Create("squirix-recovery-copy-source");
        var persistence = new PersistenceOptions { DataDir = source.DataDir, JournalMaxSegmentMb = 16 };
        var entry = new NodeCacheEntry<object?> { Value = "from-snapshot", Version = 1 };
        IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> items = [(CacheKey.Default("base"), entry)];
        _ = await StoreFactory.CreateWriter(persistence).WriteAsync(1, items, [], cancellationToken);
        BinaryJournalTestSegmentWriter.WriteJournalSegment(source.DataDir, 1, BinaryJournalTestSegmentWriter.BuildPutRecord(10UL, "base", "ignored-by-snapshot"));
        await source.Ledger.WriteAsync(
            new State
            {
                CurrentJournal = 1,
                NextSequence = 11,
                LastSnapshot = new SnapshotRef { Index = 1, CreatedUtc = DateTime.UtcNow, LastAppliedSequence = 10UL, ReplayFromJournalSegment = 1 },
            },
            cancellationToken);
        source.Ledger.Dispose();

        using var copy = RecoveryScenarioBuilder.Create("squirix-recovery-copy-target");
        foreach (var file in Directory.GetFiles(source.DataDir))
            File.Copy(file, Path.Join(copy.DataDir, Path.GetFileName(file)), true);

        await RunRecoveryAsync(copy, cancellationToken);

        var recovered = await copy.Cache.GetValueAsync(CacheKey.Default("base"), cancellationToken);
        _ = await Assert.That(recovered.Found).IsTrue();
        _ = await Assert.That(recovered.Value).IsEqualTo("from-snapshot");
    }

    /// <summary>A torn newest manifest slot leaves the fallback slot, which names the snapshot and journal segments recovery needs.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TornSlotRecoversFromFallbackSnapshot(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-torn-slot");
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16 };
        var entry = new NodeCacheEntry<object?> { Value = "from-snapshot", Version = 1 };
        IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> items = [(CacheKey.Default("base"), entry)];
        _ = await StoreFactory.CreateWriter(persistence).WriteAsync(1, items, [], cancellationToken);
        BinaryJournalTestSegmentWriter.WriteJournalSegment(
            scenario.DataDir,
            1,
            [BinaryJournalTestSegmentWriter.BuildPutRecord(10UL, "base", "ignored-by-snapshot"), BinaryJournalTestSegmentWriter.BuildPutRecord(11UL, "tail", "from-journal")]);

        var manifest = new State
        {
            CurrentJournal = 1,
            NextSequence = 12,
            LastSnapshot = new SnapshotRef { Index = 1, CreatedUtc = DateTime.UtcNow, LastAppliedSequence = 10UL, ReplayFromJournalSegment = 1 },
        };
        var image = GC.AllocateArray<byte>(SlotFormat.FileSize);
        var newer = new State
        {
            CurrentJournal = 2,
            NextSequence = 12,
            LastSnapshot = new SnapshotRef { Index = 2, CreatedUtc = DateTime.UtcNow, LastAppliedSequence = 11UL, ReplayFromJournalSegment = 2 },
        };
        SlotFormat.Encode(image.AsSpan(SlotFormat.SlotSize), manifest, 1UL);
        SlotFormat.Encode(image, newer, 2UL);
        image[SlotFormat.RecordSize - 1] ^= 0xFF;
        await File.WriteAllBytesAsync(Path.Join(scenario.DataDir, "man-current"), image, cancellationToken);

        await RunRecoveryAsync(scenario, cancellationToken);

        var baseEntry = await scenario.Cache.GetValueAsync(CacheKey.Default("base"), cancellationToken);
        _ = await Assert.That(baseEntry.Value).IsEqualTo("from-snapshot");
        var tailEntry = await scenario.Cache.GetValueAsync(CacheKey.Default("tail"), cancellationToken);
        _ = await Assert.That(tailEntry.Value).IsEqualTo("from-journal");
    }

    /// <summary>A snapshot of another format version stops recovery, whether or not the journal alone could replay the state, and nothing is restored from it.</summary>
    /// <param name="hasJournal">Whether a complete journal segment exists next to the snapshot.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task UnsupportedSnapshotVersionStopsRecovery(bool hasJournal, CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-snapshot-version");
        await WriteUnsupportedSnapshotAsync(scenario, hasJournal, cancellationToken);

        var failure = await NodeAsyncAssert.ThrowsAsync<UnsupportedSnapshotVersionException>(RunRecoveryAsync(scenario, cancellationToken));

        _ = await Assert.That(failure.Message).Contains($"version: {SnapshotCodec.Version - 1}", StringComparison.Ordinal);
        _ = await Assert.That((await scenario.Cache.GetValueAsync(CacheKey.Default("base"), cancellationToken)).Found).IsFalse();
    }

    /// <summary>A snapshot of another format version in a background recovery is logged with the exception that names the version.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BackgroundRecoveryFailureLogsException(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-snapshot-version-background");
        await WriteUnsupportedSnapshotAsync(scenario, false, cancellationToken);
        var logger = new EventRecordingLogger();
        var recovery = CreateRecovery(scenario, false, logger);

        await recovery.StartAsync(cancellationToken);
        await recovery.StopAsync(cancellationToken);

        var logged = logger.Find(JournalRecoveryFailedEventId);
        _ = await Assert.That(logged?.Cause).IsTypeOf<UnsupportedSnapshotVersionException>();
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static async Task WriteUnsupportedSnapshotAsync(RecoveryScenarioBuilder scenario, bool hasJournal, CancellationToken cancellationToken)
    {
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16 };
        IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> items = [(CacheKey.Default("base"), new NodeCacheEntry<object?> { Value = "from-snapshot", Version = 1 })];
        var snapshotPath = await StoreFactory.CreateWriter(persistence).WriteAsync(1, items, [], cancellationToken);
        var snapshotBytes = await File.ReadAllBytesAsync(snapshotPath, cancellationToken);
        snapshotBytes[SnapshotVersionOffset] = SnapshotCodec.Version - 1;
        await File.WriteAllBytesAsync(snapshotPath, snapshotBytes, cancellationToken);
        if (hasJournal)
            BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "base", "from-journal"));

        await scenario.Ledger.WriteAsync(
            new State
            {
                CurrentJournal = 1,
                NextSequence = 2,
                LastSnapshot = new SnapshotRef { Index = 1, CreatedUtc = DateTime.UtcNow, LastAppliedSequence = 0UL, ReplayFromJournalSegment = 1 },
            },
            cancellationToken);
    }

    /// <summary>Runs recovery for the given scenario.</summary>
    /// <param name="scenario">The recovery scenario.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private Task RunRecoveryAsync(RecoveryScenarioBuilder scenario, CancellationToken cancellationToken) =>
        CreateRecovery(scenario, true, NullLogger<RecoveryService<object?>>.Instance).StartAsync(cancellationToken);

    private RecoveryService<object?> CreateRecovery(RecoveryScenarioBuilder scenario, bool blockOnStart, ILogger<RecoveryService<object?>> logger)
    {
        var gate = new AsyncManualResetEvent(true);
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16 };
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter), TimeProvider.System);
        var reader = StoreFactory.CreateReader();
        var dependencies = new RecoveryDependencies<object?>(persistence, scenario.Ledger, scenario.Cache, gate, store, reader, TimeProvider.System);
        var options = new RecoveryOptions { BlockOnStart = blockOnStart };
        return new RecoveryService<object?>(options, logger, dependencies);
    }
}
