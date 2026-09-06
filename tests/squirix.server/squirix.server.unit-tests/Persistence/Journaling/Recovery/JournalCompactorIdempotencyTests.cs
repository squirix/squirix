using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Recovery;

/// <summary>Compaction must preserve durable idempotency journal frames.</summary>
[Immutable]
public sealed class JournalCompactorIdempotencyTests : IsolatedStorageTestBase
{
    private const string Fingerprint = "try-add-entry-async|default|compact-key|abc123";
    private const string OperationId = "0123456789abcdef0123456789abcdef";

    private readonly Meter _testMeter = new("test");

    /// <summary>Recovery after compaction must restore idempotency replay from the compacted journal.</summary>
    [Fact]
    public async Task CompactedLogReplaysIdempotentOps()
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-compact-idempotency-recovery");
        var persistence = CreatePersistence(scenario.DataDir);
        await WritePutAndIdempotencyAsync(persistence, scenario.Ledger);
        await JournalCompactor.CompactAsync(persistence, scenario.Ledger, StoreFactory.CreateReader(persistence), DefaultCancellationToken);

        var idempotencyStore = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        await RunRecoveryAsync(scenario, persistence, idempotencyStore);

        var replayed = idempotencyStore.TryReplay(OperationId, Fingerprint, TryAddAsyncResponse.Parser, out var response);
        Assert.True(replayed);
        Assert.NotNull(response);
        Assert.True(response.Added);
    }

    /// <summary>Compacted journal segments must retain IdempotencyOutcome frames from the pre-compaction tail.</summary>
    [Fact]
    public async Task CompactionKeepsIdempotencyFrames()
    {
        var persistence = CreatePersistence(Dir.Path);
        using var manifestStore = new Ledger(persistence);
        await WritePutAndIdempotencyAsync(persistence, manifestStore);

        await JournalCompactor.CompactAsync(persistence, manifestStore, StoreFactory.CreateReader(persistence), DefaultCancellationToken);

        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        var found = false;
        using var records = JournalReadPath.ReadAll(persistence.DataDir, manifest.CurrentJournal, DefaultCancellationToken);
        while (records.MoveNext())
        {
            var record = records.Current;
            if (record.Operation != JournalOperationKind.IdempotencyOutcome)
                continue;

            Assert.Equal(OperationId, record.IdempotencyOperationId);
            found = true;
        }

        Assert.True(found);
    }

    /// <summary>
    /// Compaction must preserve a write-ahead started intent including its fingerprint: a snapshot
    /// started record plus a journal mutation marker compact into an IdempotencyStarted frame that
    /// still rejects operation-id reuse, and the intent survives a second compaction and recovery.
    /// </summary>
    [Fact]
    public async Task CompactionKeepsStartedFingerprint()
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-compact-idempotency-started");
        var persistence = CreatePersistence(scenario.DataDir);

        await using (var journal = JournalCoordinatorFactory.Create(
            persistence,
            await scenario.Ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            scenario.Ledger,
            new AsyncManualResetEvent(true)))
        {
            var ambientScope = new object();
            RpcMutationIdempotencyExecutionAmbient.Activate(ambientScope, OperationId);
            try
            {
                await journal.AppendPutAsync(CacheKey.Default("compact-key"), JournalEntryPayloadKit.EncodePut("v"), DefaultCancellationToken);
            }
            finally
            {
                RpcMutationIdempotencyExecutionAmbient.Deactivate(ambientScope);
            }

            await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
            await WriteStartedSnapshotAsync(scenario, persistence, journal.NextSequence);
        }

        var reader = StoreFactory.CreateReader(persistence);
        await JournalCompactor.CompactAsync(persistence, scenario.Ledger, reader, DefaultCancellationToken);
        AssertStartedFrameHasFingerprint(persistence, await scenario.Ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken));

        await JournalCompactor.CompactAsync(persistence, scenario.Ledger, reader, DefaultCancellationToken);
        AssertStartedFrameHasFingerprint(persistence, await scenario.Ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken));

        var idempotencyStore = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        await RunRecoveryAsync(scenario, persistence, idempotencyStore);

        Assert.False(idempotencyStore.TryReplay(OperationId, Fingerprint, TryAddAsyncResponse.Parser, out _));
        Assert.Equal(IdempotencyReserveResult.AlreadyStarted, idempotencyStore.ReserveIntent(OperationId, Fingerprint));
        var mismatch = NodeExceptionAssert.For<ServerOpIdMismatchException>().Throws(
            idempotencyStore,
            static value => _ = value.ReserveIntent(OperationId, "other-fingerprint"));
        Assert.Equal(ServerOpIdMismatchException.StableDetail, mismatch.Message);
    }

    /// <summary>A late started marker must never downgrade a compacted completed outcome back to start.</summary>
    [Fact]
    public async Task LateStartedMarkerKeepsCompleted()
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-compact-idempotency-order");
        var persistence = CreatePersistence(scenario.DataDir);
        await WritePutAndIdempotencyAsync(persistence, scenario.Ledger);

        await using (var journal = JournalCoordinatorFactory.Create(
            persistence,
            await scenario.Ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            scenario.Ledger,
            new AsyncManualResetEvent(true)))
        {
            var ambientScope = new object();
            RpcMutationIdempotencyExecutionAmbient.Activate(ambientScope, OperationId);
            try
            {
                await journal.AppendPutAsync(CacheKey.Default("compact-key"), JournalEntryPayloadKit.EncodePut("v2"), DefaultCancellationToken);
            }
            finally
            {
                RpcMutationIdempotencyExecutionAmbient.Deactivate(ambientScope);
            }

            await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
        }

        await JournalCompactor.CompactAsync(persistence, scenario.Ledger, StoreFactory.CreateReader(persistence), DefaultCancellationToken);

        var idempotencyStore = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        await RunRecoveryAsync(scenario, persistence, idempotencyStore);

        var replayed = idempotencyStore.TryReplay(OperationId, Fingerprint, TryAddAsyncResponse.Parser, out var response);
        Assert.True(replayed);
        Assert.NotNull(response);
        Assert.True(response.Added);
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        base.DisposeManaged();
        _testMeter.Dispose();
    }

    private static PersistenceOptions CreatePersistence(string dataDir) => new() { DataDir = dataDir, JournalMaxSegmentMb = 16, FlushInterval = 5 };

    private static Task RunRecoveryAsync(RecoveryScenarioBuilder scenario, PersistenceOptions persistence, RpcMutationIdempotencyStore idempotencyStore)
    {
        var deps = new RecoveryDependencies<object?>(
            persistence,
            scenario.Ledger,
            scenario.Cache,
            new AsyncManualResetEvent(true),
            idempotencyStore,
            StoreFactory.CreateReader(persistence));
        var recovery = new RecoveryService<object?>(new RecoveryOptions { BlockOnStart = true }, NullLogger<RecoveryService<object?>>.Instance, deps);
        return recovery.StartAsync(DefaultCancellationToken);
    }

    private static async Task WritePutAndIdempotencyAsync(PersistenceOptions persistence, Ledger manifestStore)
    {
        var readCurrentOrDefaultAsync = await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(persistence, readCurrentOrDefaultAsync, manifestStore, new AsyncManualResetEvent(true));
        await journal.AppendPutAsync(CacheKey.Default("compact-key"), JournalEntryPayloadKit.EncodePut("v"), DefaultCancellationToken);
        var bytes = IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true });
        await journal.AppendIdempotencyOutcomeAsync(OperationId, Fingerprint, bytes, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
    }

    private static void AssertStartedFrameHasFingerprint(PersistenceOptions persistence, State manifest)
    {
        var found = false;
        using var records = JournalReadPath.ReadAll(persistence.DataDir, manifest.CurrentJournal, DefaultCancellationToken);
        while (records.MoveNext())
        {
            var record = records.Current;
            if (record.Operation != JournalOperationKind.IdempotencyStarted)
                continue;

            Assert.Equal(OperationId, record.IdempotencyOperationId);
            Assert.Equal(Fingerprint, record.IdempotencyFingerprint);
            found = true;
        }

        Assert.True(found);
    }

    private async Task WriteStartedSnapshotAsync(RecoveryScenarioBuilder scenario, PersistenceOptions persistence, ulong nextSequence)
    {
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        store.RestoreStarted(OperationId, Fingerprint, DateTime.UtcNow);
        var records = new List<PersistedIdempotencyRecord>();
        IIdempotencySnapshotExporter exporter = store;
        exporter.ExportSnapshot(records, DateTime.UtcNow);

        var path = await StoreFactory.CreateWriter(persistence).WriteAsync(1, [], records, DefaultCancellationToken).ConfigureAwait(false);
        var prev = await scenario.Ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken).ConfigureAwait(false);
        await scenario.Ledger.WriteAsync(
            new State
            {
                Format = prev.Format == 0 ? 1 : prev.Format,
                CurrentJournal = prev.CurrentJournal,
                NextSequence = nextSequence,
                LastSnapshot = new SnapshotRef
                {
                    Index = 1,
                    Path = path,
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = 0,
                    ReplayFromJournalSegment = 1,
                },
            },
            DefaultCancellationToken).ConfigureAwait(false);
    }
}
