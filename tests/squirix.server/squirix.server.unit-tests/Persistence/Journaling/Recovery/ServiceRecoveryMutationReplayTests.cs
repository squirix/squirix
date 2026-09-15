using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
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

namespace Squirix.Server.UnitTests.Persistence.Journaling.Recovery;

/// <summary>Recovery replay of mutation journal frames (Put, Remove, RemoveExpiration, TouchExpiration).</summary>
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

    /// <summary>Replay must apply Put, TouchExpiration, RemoveExpiration, and Remove in order, leaving only untouched keys.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplayAppliesMutationOpsInOrder(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-mutations");
        var put = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "a", "v");
        var touch = BinaryJournalTestSegmentWriter.BuildTouchExpirationRecord(2UL, "a", DateTime.UtcNow.AddHours(1));
        var removeExp = BinaryJournalTestSegmentWriter.BuildRemoveExpirationRecord(3UL, "a");
        var remove = BinaryJournalTestSegmentWriter.BuildRemoveRecord(4UL, "a");
        var putB = BinaryJournalTestSegmentWriter.BuildPutRecord(5UL, "b", "vb");
        IReadOnlyList<JournalRecord> records = [put, touch, removeExp, remove, putB];
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, records);
        await scenario.Ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 6 }, cancellationToken);

        await RunRecoveryAsync(scenario, cancellationToken);

        _ = await Assert.That((await scenario.Cache.GetValueAsync(CacheKey.Default("a"), cancellationToken)).Found).IsFalse();
        var b = await scenario.Cache.GetValueAsync(CacheKey.Default("b"), cancellationToken);
        _ = await Assert.That(b.Found).IsTrue();
        _ = await Assert.That(b.Value).IsEqualTo("vb");
    }

    /// <summary>Replay must abort with InvalidOperationException when a Put payload cannot be decoded.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UndecodablePutThrowsDuringReplay(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-broken-put");
        var broken = BinaryJournalTestSegmentWriter.BuildBrokenPutRecord(1UL, "bad");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, broken);
        await scenario.Ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 2 }, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(RunRecoveryAsync(scenario, cancellationToken));
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static RecoveryService<object?> CreateRecovery(RecoveryScenarioBuilder scenario, RpcMutationIdempotencyStore store)
    {
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16, FlushInterval = 5 };
        var reader = StoreFactory.CreateReader();
        var recoveryDependencies = new RecoveryDependencies<object?>(persistence, scenario.Ledger, scenario.Cache, new AsyncManualResetEvent(true), store, reader);
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
