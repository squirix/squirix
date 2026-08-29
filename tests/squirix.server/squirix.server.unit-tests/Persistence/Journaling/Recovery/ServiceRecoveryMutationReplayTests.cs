using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
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
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Recovery;

/// <summary>Recovery replay of mutation journal frames (Put, Remove, RemoveExpiration, TouchExpiration).</summary>
[Immutable]
public sealed class ServiceRecoveryMutationReplayTests : ServerUnitTestBase
{
    /// <summary>Replay must skip Put entries whose absolute expiration has already passed.</summary>
    [Fact]
    public async Task ExpiredPutIsSkippedDuringReplay()
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-expired-put");
        var expired = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "gone", new NodeCacheEntry<object?> { Value = "x", ExpiresUtc = DateTime.UtcNow.AddMinutes(-5) });
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, expired);
        await scenario.Ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 2 }, DefaultCancellationToken);

        await RunRecoveryAsync(scenario);

        Assert.False((await scenario.Cache.GetValueAsync(CacheKey.Default("gone"), DefaultCancellationToken)).Found);
    }

    /// <summary>Idempotency replay with UnixMs == 0 must fall back to the recovery wall clock for CreatedUtc.</summary>
    [Fact]
    public async Task IdempotencyZeroUnixMsUsesWallClock()
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-idempotency-zero");
        var bytes = RpcMutationIdempotencyStore.SerializeResponseBytes(new TryAddAsyncResponse { Added = true });
        var id = BinaryJournalTestSegmentWriter.BuildIdempotencyRecord("op-zero", "fp-zero", bytes, 0L, 1UL);
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, id);
        await scenario.Ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 2 }, DefaultCancellationToken);

        var store = new RpcMutationIdempotencyStore();
        await RunRecoveryAsync(scenario, store);

        IIdempotencySnapshotExporter exporter = store;
        var snapshot = new List<PersistedIdempotencyRecord>();
        exporter.ExportSnapshot(snapshot, DateTime.UtcNow);
        var exported = Assert.Single(snapshot);
        Assert.Equal("op-zero", exported.OperationId);
    }

    /// <summary>Replay must apply Put, TouchExpiration, RemoveExpiration, and Remove in order, leaving only untouched keys.</summary>
    [Fact]
    public async Task ReplayAppliesMutationOpsInOrder()
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-mutations");
        var put = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "a", "v");
        var touch = BinaryJournalTestSegmentWriter.BuildTouchExpirationRecord(2UL, "a", DateTime.UtcNow.AddHours(1));
        var removeExp = BinaryJournalTestSegmentWriter.BuildRemoveExpirationRecord(3UL, "a");
        var remove = BinaryJournalTestSegmentWriter.BuildRemoveRecord(4UL, "a");
        var putB = BinaryJournalTestSegmentWriter.BuildPutRecord(5UL, "b", "vb");
        IReadOnlyList<JournalRecord> records = [put, touch, removeExp, remove, putB];
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, records);
        await scenario.Ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 6 }, DefaultCancellationToken);

        await RunRecoveryAsync(scenario);

        Assert.False((await scenario.Cache.GetValueAsync(CacheKey.Default("a"), DefaultCancellationToken)).Found);
        var b = await scenario.Cache.GetValueAsync(CacheKey.Default("b"), DefaultCancellationToken);
        Assert.True(b.Found);
        Assert.Equal("vb", b.Value);
    }

    /// <summary>Replay must abort with InvalidOperationException when a Put payload cannot be decoded.</summary>
    [Fact]
    public async Task UndecodablePutThrowsDuringReplay()
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-broken-put");
        var broken = BinaryJournalTestSegmentWriter.BuildBrokenPutRecord(1UL, "bad");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, broken);
        await scenario.Ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 2 }, DefaultCancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(RunRecoveryAsync(scenario));
    }

    private static RecoveryService<object?> CreateRecovery(RecoveryScenarioBuilder scenario, RpcMutationIdempotencyStore store)
    {
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16, FlushInterval = 5 };
        var reader = StoreFactory.CreateReader(persistence);
        var recoveryDependencies = new RecoveryDependencies<object?>(persistence, scenario.Ledger, scenario.Cache, new AsyncManualResetEvent(true), store, reader);
        return new RecoveryService<object?>(new RecoveryOptions { BlockOnStart = true }, NullLogger<RecoveryService<object?>>.Instance, recoveryDependencies);
    }

    private static Task RunRecoveryAsync(RecoveryScenarioBuilder builder) => RunRecoveryAsync(builder, new RpcMutationIdempotencyStore());

    private static Task RunRecoveryAsync(RecoveryScenarioBuilder builder, RpcMutationIdempotencyStore store) => CreateRecovery(builder, store).StartAsync(DefaultCancellationToken);
}
