using System;
using System.Collections.Concurrent;
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
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Every layout a crash can leave around a snapshot publish and its retention pass recovers the last committed state: the snapshot plus the
/// journal from its replay floor when the manifest names a snapshot that loads, the whole journal when it does not.
/// </summary>
[Immutable]
public sealed class SnapshotRetentionCrashTests : DisposableServerUnitTestBase
{
    private const string CoveredKey = "covered";
    private const string SnapshotKey = "snapshot";
    private const string TailKey = "tail";

    private static readonly TimeSpan NoPassWindow = TimeSpan.FromMilliseconds(500);

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
        await PublishManifestWithoutRetentionAsync(scenario, cancellationToken);
        using var recoveryLedger = CreateRecoveryLedger(scenario, out _);

        await RunRecoveryAsync(scenario, recoveryLedger, cancellationToken);

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
        await PublishManifestWithoutRetentionAsync(scenario, cancellationToken);
        File.Delete(BinaryJournalTestSegmentWriter.SegmentPath(scenario.DataDir, 1));
        using var recoveryLedger = CreateRecoveryLedger(scenario, out _);

        await RunRecoveryAsync(scenario, recoveryLedger, cancellationToken);

        _ = await Assert.That(await FoundAsync(scenario, CoveredKey, cancellationToken)).IsFalse();
        _ = await Assert.That(await FoundAsync(scenario, SnapshotKey, cancellationToken)).IsTrue();
        _ = await Assert.That(await FoundAsync(scenario, TailKey, cancellationToken)).IsTrue();
    }

    /// <summary>A crash before retention plus a damaged snapshot recovers by full replay, and the covered segment stays: it is the only copy.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DamagedSnapshotKeepsCoveredSegment(CancellationToken cancellationToken)
    {
        using var scenario = CreateJournal("squirix-retention-crash-damaged");
        await WriteSnapshotFileAsync(scenario, cancellationToken);
        await File.WriteAllBytesAsync(SnapshotFileName.BuildPath(scenario.DataDir, 1), [1, 2, 3], cancellationToken);
        await PublishManifestWithoutRetentionAsync(scenario, cancellationToken);
        using var recoveryLedger = CreateRecoveryLedger(scenario, out var passes);

        await RunRecoveryAsync(scenario, recoveryLedger, cancellationToken);
        var passed = await WaitForPassAsync(passes, cancellationToken);

        _ = await Assert.That(passed).IsFalse();
        _ = await Assert.That(File.Exists(BinaryJournalTestSegmentWriter.SegmentPath(scenario.DataDir, 1))).IsTrue();
        _ = await Assert.That(await FoundAsync(scenario, CoveredKey, cancellationToken)).IsTrue();
        _ = await Assert.That(await FoundAsync(scenario, TailKey, cancellationToken)).IsTrue();
    }

    /// <summary>A crash before retention plus a good snapshot: once recovery loaded it, the startup pass deletes the covered segment and hands it to the journal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StartupPassFreesCoveredSegment(CancellationToken cancellationToken)
    {
        using var scenario = CreateJournal("squirix-retention-crash-startup-pass");
        await WriteSnapshotFileAsync(scenario, cancellationToken);
        await PublishManifestWithoutRetentionAsync(scenario, cancellationToken);
        var coveredPath = BinaryJournalTestSegmentWriter.SegmentPath(scenario.DataDir, 1);
        var coveredLength = new FileInfo(coveredPath).Length;
        using var recoveryLedger = CreateRecoveryLedger(scenario, out var passes);

        await RunRecoveryAsync(scenario, recoveryLedger, cancellationToken);
        await passes.WaitUntilAsync(static p => !p.IsEmpty, cancellationToken);

        _ = await Assert.That(recoveryLedger.CapacityResync.TryTake(out var segments, out var bytes)).IsTrue();
        _ = await Assert.That(segments).IsEqualTo(1);
        _ = await Assert.That(bytes).IsEqualTo(coveredLength);
        _ = await Assert.That(File.Exists(coveredPath)).IsFalse();
        _ = await Assert.That(await FoundAsync(scenario, SnapshotKey, cancellationToken)).IsTrue();
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

    /// <summary>Creates the ledger recovery runs with: it deletes for real, and reports every retention pass to <paramref name="passes" />.</summary>
    /// <param name="scenario">The scenario whose directory the ledger serves.</param>
    /// <param name="passes">Receives the outcome of each retention pass.</param>
    /// <returns>The ledger.</returns>
    private static Ledger CreateRecoveryLedger(RecoveryScenarioBuilder scenario, out ConcurrentQueue<bool> passes)
    {
        var outcomes = new ConcurrentQueue<bool>();
        var readiness = new IRetentionCleanupReadinessStatusCreateExpectations();
        _ = readiness.Setups.RecordWriteOutcome(Arg.Any<bool>()).Callback(outcomes.Enqueue);
        passes = outcomes;
        return new Ledger(OptionsOf(scenario), NullLogger<Ledger>.Instance, readiness.Instance());
    }

    private static async Task<bool> FoundAsync(RecoveryScenarioBuilder scenario, string key, CancellationToken cancellationToken) =>
        (await scenario.Cache.GetValueAsync(CacheKey.Default(key), cancellationToken)).Found;

    private static PersistenceOptions OptionsOf(RecoveryScenarioBuilder scenario) => new() { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16 };

    /// <summary>Publishes the snapshot manifest through a ledger whose deletes fail, which stands in for a crash before the retention pass.</summary>
    /// <param name="scenario">The scenario whose directory is published into.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private static async Task PublishManifestWithoutRetentionAsync(RecoveryScenarioBuilder scenario, CancellationToken cancellationToken)
    {
        var fileOperations = new IStorageFileOperationsCreateExpectations();
        _ = fileOperations.Setups.TryDelete(Arg.Any<string>()).ReturnValue(false);
        using var ledger = new Ledger(OptionsOf(scenario), NullLogger<Ledger>.Instance, null, null, fileOperations.Instance());
        await ledger.WriteSnapshotAsync(
            new State
            {
                CurrentJournal = 2,
                NextSequence = 6,
                LastSnapshot = new SnapshotRef { Index = 1, CreatedUtc = DateTime.UtcNow, LastAppliedSequence = 4UL, ReplayFromJournalSegment = 2 },
            },
            cancellationToken);
    }

    private static async Task WriteSnapshotFileAsync(RecoveryScenarioBuilder scenario, CancellationToken cancellationToken)
    {
        var items = new List<(CacheKey Key, NodeCacheEntry<object?> Entry)>
        {
            (CacheKey.Default(SnapshotKey), new NodeCacheEntry<object?> { Value = "snap" }),
        };
        _ = await StoreFactory.CreateWriter(OptionsOf(scenario)).WriteAsync(1, items, [], cancellationToken);
    }

    /// <summary>Waits a bounded real-time window for a retention pass; the window only bounds how long the test waits for a pass that must not come.</summary>
    /// <param name="passes">The passes reported by the ledger.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>Whether a pass was reported within the window.</returns>
    private static async Task<bool> WaitForPassAsync(ConcurrentQueue<bool> passes, CancellationToken cancellationToken)
    {
        try
        {
            await passes.WaitUntilAsync(static p => !p.IsEmpty, NoPassWindow, TimeProvider.System, cancellationToken);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    private Task RunRecoveryAsync(RecoveryScenarioBuilder scenario, Ledger ledger, CancellationToken cancellationToken)
    {
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter), TimeProvider.System);
        var dependencies = new RecoveryDependencies<object?>(OptionsOf(scenario), ledger, scenario.Cache, new AsyncManualResetEvent(true), store, StoreFactory.CreateReader(), TimeProvider.System);
        return new RecoveryService<object?>(new RecoveryOptions { BlockOnStart = true }, NullLogger<RecoveryService<object?>>.Instance, dependencies).StartAsync(cancellationToken);
    }
}
