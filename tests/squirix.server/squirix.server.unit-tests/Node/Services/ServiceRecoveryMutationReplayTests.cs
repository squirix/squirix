using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Recovery replay of mutation journal frames: every cache-entry frame is a put of the whole entry or a remove.</summary>
[Immutable]
public sealed class ServiceRecoveryMutationReplayTests : DisposableServerUnitTestBase
{
    private readonly Meter _testMeter = new("test");

    /// <summary>Replay must skip Put entries whose absolute expiration has already passed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredPutIsSkippedDuringReplay(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-expired-put");
        var expired = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "gone", new NodeCacheEntry<object?> { Value = "x", ExpiresUtc = DateTime.UtcNow.AddMinutes(-5) });
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, expired);
        await scenario.Ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 2 }, cancellationToken);

        await RunRecoveryAsync(scenario, cancellationToken);

        _ = await Assert.That((await scenario.Cache.GetValueAsync(CacheKey.Default("gone"), cancellationToken)).Found).IsFalse();
    }

    /// <summary>Idempotency replay with UnixMs == 0 must fall back to the recovery wall clock for CreatedUtc.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IdempotencyZeroUnixMsUsesWallClock(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-idempotency-zero");
        var bytes = IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true });
        var id = BinaryJournalTestSegmentWriter.BuildIdempotencyRecord("op-zero", "fp-zero", bytes, 0L, 1UL);
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, id);
        await scenario.Ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 2 }, cancellationToken);

        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        await RunRecoveryAsync(scenario, store, cancellationToken);

        IIdempotencySnapshotExporter exporter = store;
        var snapshot = new List<PersistedIdempotencyRecord>();
        exporter.ExportSnapshot(snapshot, DateTime.UtcNow);
        var exported = await Assert.That(snapshot).HasSingleItem();
        _ = await Assert.That(exported.OperationId).IsEqualTo("op-zero");
    }

    /// <summary>A put whose deadline passed, followed by a put of a live entry for the same key, replays as the live entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PastPutThenFuturePutKeepsFutureEntry(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-past-then-future");
        var deadline = DateTime.UtcNow.AddHours(1);
        var past = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "a", new NodeCacheEntry<object?> { Value = "old", ExpiresUtc = DateTime.UtcNow.AddMinutes(-5) });
        var future = BinaryJournalTestSegmentWriter.BuildPutRecord(2UL, "a", new NodeCacheEntry<object?> { Value = "new", ExpiresUtc = deadline });
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, [past, future]);
        await scenario.Ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 3 }, cancellationToken);

        await RunRecoveryAsync(scenario, cancellationToken);

        var entry = await scenario.Cache.GetEntryAsync(CacheKey.Default("a"), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.Value).IsEqualTo("new");
        _ = await Assert.That(entry.ExpiresUtc!.Value.Ticks / TimeSpan.TicksPerMillisecond).IsEqualTo(deadline.Ticks / TimeSpan.TicksPerMillisecond);
    }

    /// <summary>A put of a live entry, followed by a put whose deadline passed for the same key, replays as an absent key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FuturePutThenPastPutDropsKey(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-future-then-past");
        var future = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "a", new NodeCacheEntry<object?> { Value = "old", ExpiresUtc = DateTime.UtcNow.AddHours(1) });
        var past = BinaryJournalTestSegmentWriter.BuildPutRecord(2UL, "a", new NodeCacheEntry<object?> { Value = "new", ExpiresUtc = DateTime.UtcNow.AddMinutes(-5) });
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, [future, past]);
        await scenario.Ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 3 }, cancellationToken);

        await RunRecoveryAsync(scenario, cancellationToken);

        _ = await Assert.That((await scenario.Cache.GetValueAsync(CacheKey.Default("a"), cancellationToken)).Found).IsFalse();
    }

    /// <summary>Replay must apply Put and Remove in order, leaving only the keys the last frame of which is a live put.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplayAppliesMutationOpsInOrder(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-mutations");
        var put = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "a", "v");
        var extended = BinaryJournalTestSegmentWriter.BuildPutRecord(2UL, "a", new NodeCacheEntry<object?> { Value = "v", ExpiresUtc = DateTime.UtcNow.AddHours(1) });
        var persistent = BinaryJournalTestSegmentWriter.BuildPutRecord(3UL, "a", "v");
        var remove = BinaryJournalTestSegmentWriter.BuildRemoveRecord(4UL, "a");
        var putB = BinaryJournalTestSegmentWriter.BuildPutRecord(5UL, "b", "vb");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, [put, extended, persistent, remove, putB]);
        await scenario.Ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 6 }, cancellationToken);

        await RunRecoveryAsync(scenario, cancellationToken);

        _ = await Assert.That((await scenario.Cache.GetValueAsync(CacheKey.Default("a"), cancellationToken)).Found).IsFalse();
        var b = await scenario.Cache.GetValueAsync(CacheKey.Default("b"), cancellationToken);
        _ = await Assert.That(b.Found).IsTrue();
        _ = await Assert.That(b.Value).IsEqualTo("vb");
    }

    /// <summary>A frame carrying an unassigned opcode fails recovery instead of being skipped.</summary>
    /// <param name="opcodeWire">The unassigned raw opcode byte.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0)]
    [Arguments(7)]
    [Arguments(8)]
    [Arguments(255)]
    public async Task UnassignedOpcodeFailsRecovery(int opcodeWire, CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-unassigned-opcode");
        BinaryJournalTestSegmentWriter.WriteRawOpcodeSegment(scenario.DataDir, 1, Convert.ToByte(opcodeWire), "a");
        await scenario.Ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 2 }, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(RunRecoveryAsync(scenario, cancellationToken));
    }

    /// <summary>An unassigned frame followed by a valid frame fails recovery instead of being skipped.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnassignedOpcodeBeforeValidFrameFails(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-unassigned-then-valid");
        var valid = BinaryJournalTestSegmentWriter.BuildPutRecord(2UL, "b", "vb");
        BinaryJournalTestSegmentWriter.WriteRawOpcodeSegment(scenario.DataDir, 1, 8, "a", [valid]);
        await scenario.Ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 3 }, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(RunRecoveryAsync(scenario, cancellationToken));
    }

    /// <summary>An unassigned frame in a segment that is not the last one fails recovery.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnassignedOpcodeInEarlierSegmentFails(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-unassigned-earlier-segment");
        BinaryJournalTestSegmentWriter.WriteRawOpcodeSegment(scenario.DataDir, 1, 7, "a");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 2, BinaryJournalTestSegmentWriter.BuildPutRecord(2UL, "b", "vb"));
        await scenario.Ledger.WriteAsync(new State { Format = 1, CurrentJournal = 2, NextSequence = 3 }, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(RunRecoveryAsync(scenario, cancellationToken));
    }

    /// <summary>Compaction fails on an unassigned frame instead of folding it away.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionRejectsUnassignedOpcode(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-compaction-unassigned-opcode");
        BinaryJournalTestSegmentWriter.WriteRawOpcodeSegment(scenario.DataDir, 1, 255, "a");
        await scenario.Ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 2 }, cancellationToken);
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16 };

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(JournalCompactor.CompactAsync(persistence, scenario.Ledger, StoreFactory.CreateReader(), DateTime.UtcNow, DateTime.UtcNow, cancellationToken));
    }

    /// <summary>A segment written with an earlier file format version fails recovery loudly and is left untouched.</summary>
    /// <param name="version">The file format version of the segment header.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    public async Task OldFormatVersionFailsRecovery(int version, CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-old-version");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "a", "v"));
        var path = BinaryJournalTestSegmentWriter.SegmentPath(scenario.DataDir, 1);
        BinaryJournalTestSegmentWriter.SetHeaderVersion(path, Convert.ToByte(version));
        var lengthBefore = new FileInfo(path).Length;
        await scenario.Ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 2 }, cancellationToken);

        var failure = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(RunRecoveryAsync(scenario, cancellationToken));

        _ = await Assert.That(failure.Message).Contains($"version {version}", StringComparison.Ordinal);
        _ = await Assert.That(new FileInfo(path).Length).IsEqualTo(lengthBefore);
    }

    /// <summary>Startup preparation of the active segment rejects the previous file format version instead of truncating the file.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StartupScanKeepsOldFormatSegment(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-startup-old-version");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "a", "v"));
        var path = BinaryJournalTestSegmentWriter.SegmentPath(scenario.DataDir, 1);
        BinaryJournalTestSegmentWriter.SetHeaderVersion(path, 1);
        var lengthBefore = new FileInfo(path).Length;
        var manifest = new State { Format = 1, CurrentJournal = 1, NextSequence = 2 };
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16 };

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(
            (manifest, persistence),
            static state => JournalRecoveryScan.PrepareActiveSegmentForSequenceScan(state.manifest, state.persistence));

        _ = await Assert.That(new FileInfo(path).Length).IsEqualTo(lengthBefore);
        await scenario.Ledger.WriteAsync(manifest, cancellationToken);
    }

    /// <summary>Compaction fails on a segment written with the previous file format version.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionRejectsOldFormatVersion(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-compaction-old-version");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "a", "v"));
        BinaryJournalTestSegmentWriter.SetHeaderVersion(BinaryJournalTestSegmentWriter.SegmentPath(scenario.DataDir, 1), 1);
        await scenario.Ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 2 }, cancellationToken);
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16 };

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(JournalCompactor.CompactAsync(persistence, scenario.Ledger, StoreFactory.CreateReader(), DateTime.UtcNow, DateTime.UtcNow, cancellationToken));
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static RecoveryService<object?> CreateRecovery(RecoveryScenarioBuilder scenario, RpcMutationIdempotencyStore store)
    {
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16 };
        var reader = StoreFactory.CreateReader();
        var recoveryDependencies = new RecoveryDependencies<object?>(persistence, scenario.Ledger, scenario.Cache, new AsyncManualResetEvent(true), store, reader, TimeProvider.System);
        return new RecoveryService<object?>(new RecoveryOptions { BlockOnStart = true }, NullLogger<RecoveryService<object?>>.Instance, recoveryDependencies);
    }

    /// <summary>Runs recovery for the given builder and idempotency store.</summary>
    /// <param name="builder">The recovery scenario builder.</param>
    /// <param name="store">The idempotency store.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private static Task RunRecoveryAsync(RecoveryScenarioBuilder builder, RpcMutationIdempotencyStore store, CancellationToken cancellationToken) =>
        CreateRecovery(builder, store).StartAsync(cancellationToken);

    /// <summary>Runs recovery for the given builder with a fresh idempotency store.</summary>
    /// <param name="builder">The recovery scenario builder.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private Task RunRecoveryAsync(RecoveryScenarioBuilder builder, CancellationToken cancellationToken) => RunRecoveryAsync(
        builder,
        new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter)),
        cancellationToken);
}
