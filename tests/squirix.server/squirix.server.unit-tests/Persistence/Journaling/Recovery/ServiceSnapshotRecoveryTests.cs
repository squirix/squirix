using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Recovery;

/// <summary>Recovery with binary snapshots and missing snapshot path fallbacks.</summary>
[Immutable]
public sealed class ServiceSnapshotRecoveryTests : DisposableServerUnitTestBase
{
    private readonly Meter _testMeter = new("test");

    /// <summary>Manifest pointing at a missing snapshot path falls back to journal-only recovery.</summary>
    [Fact]
    public async Task MissingSnapshotFallsBackToJournal()
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-missing-snapshot");
        var missingSnapshotPath = NodePathKit.Combine(scenario.DataDir, $"{FilePrefixes.Snapshot}{NodeInvariantIndexStrings.FormatD6(1)}{FileExtensions.Snapshot}");

        var record = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "recovered", "yes");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, record);

        var manifest = new State
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
        };
        await scenario.Ledger.WriteAsync(manifest, DefaultCancellationToken);

        await RunRecoveryAsync(scenario);

        var recovered = await scenario.Cache.GetValueAsync(CacheKey.Default("recovered"), DefaultCancellationToken);
        Assert.True(recovered.Found);
        Assert.Equal("yes", recovered.Value);
    }

    /// <summary>Loads a binary snapshot watermark and replays only journal records after it.</summary>
    [Fact]
    public async Task SnapshotRecoveryReplaysJournalTail()
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-binary-snapshot");
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16, FlushInterval = 5 };
        var writer = StoreFactory.CreateWriter(persistence);
        var e = new NodeCacheEntry<object?> { Value = "from-snapshot", Version = 1 };
        IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> readOnlyList = [(CacheKey.Default("base"), e)];
        var path = await writer.WriteAsync(1, readOnlyList, [], DefaultCancellationToken);

        const ulong snapshotSequence = 10UL;
        var baseRecord = BinaryJournalTestSegmentWriter.BuildPutRecord(snapshotSequence, "base", "ignored-by-snapshot");
        var tailRecord = BinaryJournalTestSegmentWriter.BuildPutRecord(11UL, "tail", "from-journal");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, [baseRecord, tailRecord]);

        var manifest = new State
        {
            Format = 1,
            CurrentJournal = 1,
            NextSequence = 12,
            LastSnapshot = new SnapshotRef
            {
                Index = 1,
                Path = path,
                CreatedUtc = DateTime.UtcNow,
                LastAppliedSequence = snapshotSequence,
                ReplayFromJournalSegment = 1,
            },
        };
        await scenario.Ledger.WriteAsync(manifest, DefaultCancellationToken);

        await RunRecoveryAsync(scenario);

        var entry = await scenario.Cache.GetValueAsync(CacheKey.Default("base"), DefaultCancellationToken);
        Assert.True(entry.Found);
        Assert.Equal("from-snapshot", entry.Value);

        var tailEntry = await scenario.Cache.GetValueAsync(CacheKey.Default("tail"), DefaultCancellationToken);
        Assert.True(tailEntry.Found);
        Assert.Equal("from-journal", tailEntry.Value);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private Task RunRecoveryAsync(RecoveryScenarioBuilder scenario)
    {
        var gate = new AsyncManualResetEvent(true);
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16, FlushInterval = 5 };
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        var reader = StoreFactory.CreateReader(persistence);
        var dependencies = new RecoveryDependencies<object?>(persistence, scenario.Ledger, scenario.Cache, gate, store, reader);
        var options = new RecoveryOptions { BlockOnStart = true };
        var logger = NullLogger<RecoveryService<object?>>.Instance;
        var recovery = new RecoveryService<object?>(options, logger, dependencies);
        return recovery.StartAsync(DefaultCancellationToken);
    }
}
