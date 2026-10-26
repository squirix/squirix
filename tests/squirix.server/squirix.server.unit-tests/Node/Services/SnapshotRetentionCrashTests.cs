using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Every layout a crash can leave around a snapshot publish and its retention pass recovers the last committed state: the snapshot plus the
/// journal from its replay floor when the manifest names the snapshot, the journal alone when it does not.
/// </summary>
[Immutable]
public sealed class SnapshotRetentionCrashTests : DisposableServerUnitTestBase
{
    private const string CoveredKey = "covered";
    private const string SnapshotKey = "snapshot";
    private const string TailKey = "tail";

    private readonly Meter _testMeter = new("test");

    /// <summary>A crash after the snapshot file was published and before the manifest names it recovers from the whole journal and ignores the snapshot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotFileWithoutManifestIsIgnored(CancellationToken cancellationToken)
    {
        using var scenario = CreateJournal("squirix-retention-crash-file-only");
        await WriteSnapshotFileAsync(scenario, cancellationToken);
        await scenario.Ledger.WriteAsync(new State { CurrentJournal = 2, NextSequence = 6 }, cancellationToken);

        await RunRecoveryAsync(scenario, scenario.Ledger, cancellationToken);

        _ = await Assert.That(await FoundAsync(scenario, CoveredKey, cancellationToken)).IsTrue();
        _ = await Assert.That(await FoundAsync(scenario, TailKey, cancellationToken)).IsTrue();
        _ = await Assert.That(await FoundAsync(scenario, SnapshotKey, cancellationToken)).IsFalse();
    }

    /// <summary>A crash after the manifest names the snapshot and before retention ran leaves the covered segment on disk, and recovery ignores it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CoveredSegmentLeftByCrashIsIgnored(CancellationToken cancellationToken)
    {
        using var scenario = CreateJournal("squirix-retention-crash-before-retention");
        await WriteSnapshotFileAsync(scenario, cancellationToken);

        // A ledger whose deletes fail stands in for the crash: the manifest names the snapshot and the covered segment stays.
        var fileOperations = new IStorageFileOperationsCreateExpectations();
        _ = fileOperations.Setups.TryDelete(Arg.Any<string>()).ReturnValue(false);
        using var ledger = new Ledger(new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16 }, NullLogger<Ledger>.Instance, null, null, fileOperations.Instance());
        await PublishManifestAsync(ledger, cancellationToken);

        await RunRecoveryAsync(scenario, ledger, cancellationToken);

        _ = await Assert.That(File.Exists(BinaryJournalTestSegmentWriter.SegmentPath(scenario.DataDir, 1))).IsTrue();
        _ = await Assert.That(await FoundAsync(scenario, CoveredKey, cancellationToken)).IsFalse();
        _ = await Assert.That(await FoundAsync(scenario, SnapshotKey, cancellationToken)).IsTrue();
        _ = await Assert.That(await FoundAsync(scenario, TailKey, cancellationToken)).IsTrue();
    }

    /// <summary>After retention deleted the covered segment, recovery restores the same state from the snapshot and the tail.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveryAfterRetentionMatchesLeftover(CancellationToken cancellationToken)
    {
        using var scenario = CreateJournal("squirix-retention-crash-after-retention");
        await WriteSnapshotFileAsync(scenario, cancellationToken);
        await PublishManifestAsync(scenario.Ledger, cancellationToken);
        File.Delete(BinaryJournalTestSegmentWriter.SegmentPath(scenario.DataDir, 1));

        await RunRecoveryAsync(scenario, scenario.Ledger, cancellationToken);

        _ = await Assert.That(await FoundAsync(scenario, CoveredKey, cancellationToken)).IsFalse();
        _ = await Assert.That(await FoundAsync(scenario, SnapshotKey, cancellationToken)).IsTrue();
        _ = await Assert.That(await FoundAsync(scenario, TailKey, cancellationToken)).IsTrue();
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static RecoveryScenarioBuilder CreateJournal(string prefix)
    {
        var scenario = RecoveryScenarioBuilder.Create(prefix);
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, CoveredKey, "old"));
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 2, BinaryJournalTestSegmentWriter.BuildPutRecord(5UL, TailKey, "new"));
        return scenario;
    }

    private static async Task<bool> FoundAsync(RecoveryScenarioBuilder scenario, string key, CancellationToken cancellationToken) =>
        (await scenario.Cache.GetValueAsync(CacheKey.Default(key), cancellationToken)).Found;

    private static Task PublishManifestAsync(Ledger ledger, CancellationToken cancellationToken) => ledger.WriteSnapshotAsync(
        new State
        {
            CurrentJournal = 2,
            NextSequence = 6,
            LastSnapshot = new SnapshotRef { Index = 1, CreatedUtc = DateTime.UtcNow, LastAppliedSequence = 4UL, ReplayFromJournalSegment = 2 },
        },
        cancellationToken);

    private static async Task WriteSnapshotFileAsync(RecoveryScenarioBuilder scenario, CancellationToken cancellationToken)
    {
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16 };
        var items = new List<(CacheKey Key, NodeCacheEntry<object?> Entry)>
        {
            (CacheKey.Default(SnapshotKey), new NodeCacheEntry<object?> { Value = "snap" }),
        };
        _ = await StoreFactory.CreateWriter(persistence).WriteAsync(1, items, [], cancellationToken);
    }

    private Task RunRecoveryAsync(RecoveryScenarioBuilder scenario, Ledger ledger, CancellationToken cancellationToken)
    {
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16 };
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter), TimeProvider.System);
        var dependencies = new RecoveryDependencies<object?>(persistence, ledger, scenario.Cache, new AsyncManualResetEvent(true), store, StoreFactory.CreateReader(), TimeProvider.System);
        return new RecoveryService<object?>(new RecoveryOptions { BlockOnStart = true }, NullLogger<RecoveryService<object?>>.Instance, dependencies).StartAsync(cancellationToken);
    }
}
