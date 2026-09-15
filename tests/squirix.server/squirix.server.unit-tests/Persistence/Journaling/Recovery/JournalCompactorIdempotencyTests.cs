using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Recovery;

/// <summary>Compaction must preserve durable idempotency journal frames.</summary>
[Immutable]
public sealed class JournalCompactorIdempotencyTests : IsolatedStorageTestBase
{
    private const string Fingerprint = "try-add-entry-async|default|compact-key|abc123";
    private const string OperationId = "0123456789abcdef0123456789abcdef";

    private readonly Meter _testMeter = new("test");

    /// <summary>Recovery after compaction must restore idempotency replay from the compacted journal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactedLogReplaysIdempotentOps(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-compact-idempotency-recovery");
        var persistence = CreatePersistence(scenario.DataDir);
        await WritePutAndIdempotencyAsync(persistence, scenario.Ledger, cancellationToken);
        await JournalCompactor.CompactAsync(persistence, scenario.Ledger, StoreFactory.CreateReader(), cancellationToken);

        var idempotencyStore = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        await RunRecoveryAsync(scenario, persistence, idempotencyStore, cancellationToken);

        var replayed = idempotencyStore.TryReplay(OperationId, Fingerprint, TryAddAsyncResponse.Parser, out var response);
        _ = await Assert.That(replayed).IsTrue();
        _ = await Assert.That(response).IsNotNull();
        _ = await Assert.That(response.Added).IsTrue();
    }

    /// <summary>Compacted journal segments must retain IdempotencyOutcome frames from the pre-compaction tail.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionKeepsIdempotencyFrames(CancellationToken cancellationToken)
    {
        var persistence = CreatePersistence(Dir.Path);
        using var manifestStore = new Ledger(persistence);
        await WritePutAndIdempotencyAsync(persistence, manifestStore, cancellationToken);

        await JournalCompactor.CompactAsync(persistence, manifestStore, StoreFactory.CreateReader(), cancellationToken);

        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken);
        var found = false;
        using var records = JournalReadPath.ReadAll(persistence.DataDir, manifest.CurrentJournal, cancellationToken);
        while (records.MoveNext())
        {
            var record = records.Current;
            if (record.Operation != JournalOperationKind.IdempotencyOutcome)
                continue;

            _ = await Assert.That(record.IdempotencyOperationId).IsEqualTo(OperationId);
            found = true;
        }

        _ = await Assert.That(found).IsTrue();
    }

    /// <summary>
    /// Compaction must preserve a write-ahead started intent including its fingerprint: a snapshot
    /// started record plus a journal mutation marker compact into an IdempotencyStarted frame that
    /// still rejects operation-id reuse, and the intent survives a second compaction and recovery.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionKeepsStartedFingerprint(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-compact-idempotency-started");
        var persistence = CreatePersistence(scenario.DataDir);

        await using (var journal = JournalCoordinatorFactory.Create(
                         persistence,
                         await scenario.Ledger.ReadCurrentOrDefaultAsync(cancellationToken),
                         scenario.Ledger,
                         new AsyncManualResetEvent(true)))
        {
            var ambientScope = new object();
            RpcMutationIdempotencyExecutionAmbient.Activate(ambientScope, OperationId);
            try
            {
                await journal.AppendPutAsync(CacheKey.Default("compact-key"), JournalEntryPayloadKit.EncodePut("v"), cancellationToken);
            }
            finally
            {
                RpcMutationIdempotencyExecutionAmbient.Deactivate(ambientScope);
            }

            await journal.AwaitDurabilityCommitAsync(cancellationToken);
            await WriteStartedSnapshotAsync(scenario, persistence, journal.NextSequence, cancellationToken);
        }

        var reader = StoreFactory.CreateReader();
        await JournalCompactor.CompactAsync(persistence, scenario.Ledger, reader, cancellationToken);
        await AssertStartedFrameHasFingerprint(persistence, await scenario.Ledger.ReadCurrentOrDefaultAsync(cancellationToken), cancellationToken);

        await JournalCompactor.CompactAsync(persistence, scenario.Ledger, reader, cancellationToken);
        await AssertStartedFrameHasFingerprint(persistence, await scenario.Ledger.ReadCurrentOrDefaultAsync(cancellationToken), cancellationToken);

        var idempotencyStore = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        await RunRecoveryAsync(scenario, persistence, idempotencyStore, cancellationToken);

        _ = await Assert.That(idempotencyStore.TryReplay(OperationId, Fingerprint, TryAddAsyncResponse.Parser, out _)).IsFalse();
        _ = await Assert.That(idempotencyStore.ReserveIntent(OperationId, Fingerprint)).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);
        var mismatch = NodeExceptionAssert.For<ServerOpIdMismatchException>().Throws(idempotencyStore, static value => _ = value.ReserveIntent(OperationId, "other-fingerprint"));
        _ = await Assert.That(mismatch.Message).IsEqualTo(ServerOpIdMismatchException.StableDetail);
    }

    /// <summary>A late started marker must never downgrade a compacted completed outcome back to start.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LateStartedMarkerKeepsCompleted(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-compact-idempotency-order");
        var persistence = CreatePersistence(scenario.DataDir);
        await WritePutAndIdempotencyAsync(persistence, scenario.Ledger, cancellationToken);

        await using (var journal = JournalCoordinatorFactory.Create(
                         persistence,
                         await scenario.Ledger.ReadCurrentOrDefaultAsync(cancellationToken),
                         scenario.Ledger,
                         new AsyncManualResetEvent(true)))
        {
            var ambientScope = new object();
            RpcMutationIdempotencyExecutionAmbient.Activate(ambientScope, OperationId);
            try
            {
                await journal.AppendPutAsync(CacheKey.Default("compact-key"), JournalEntryPayloadKit.EncodePut("v2"), cancellationToken);
            }
            finally
            {
                RpcMutationIdempotencyExecutionAmbient.Deactivate(ambientScope);
            }

            await journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        await JournalCompactor.CompactAsync(persistence, scenario.Ledger, StoreFactory.CreateReader(), cancellationToken);

        var idempotencyStore = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        await RunRecoveryAsync(scenario, persistence, idempotencyStore, cancellationToken);

        var replayed = idempotencyStore.TryReplay(OperationId, Fingerprint, TryAddAsyncResponse.Parser, out var response);
        _ = await Assert.That(replayed).IsTrue();
        _ = await Assert.That(response).IsNotNull();
        _ = await Assert.That(response.Added).IsTrue();
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        base.DisposeManaged();
        _testMeter.Dispose();
    }

    /// <summary>Asserts that the started frame carries the expected fingerprint.</summary>
    /// <param name="persistence">The persistence options.</param>
    /// <param name="manifest">The manifest state.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private static async Task AssertStartedFrameHasFingerprint(PersistenceOptions persistence, State manifest, CancellationToken cancellationToken)
    {
        var found = false;
        using var records = JournalReadPath.ReadAll(persistence.DataDir, manifest.CurrentJournal, cancellationToken);
        while (records.MoveNext())
        {
            var record = records.Current;
            if (record.Operation != JournalOperationKind.IdempotencyStarted)
                continue;

            _ = await Assert.That(record.IdempotencyOperationId).IsEqualTo(OperationId);
            _ = await Assert.That(record.IdempotencyFingerprint).IsEqualTo(Fingerprint);
            found = true;
        }

        _ = await Assert.That(found).IsTrue();
    }

    private static PersistenceOptions CreatePersistence(string dataDir) => new() { DataDir = dataDir, JournalMaxSegmentMb = 16, FlushInterval = 5 };

    /// <summary>Runs recovery for the given scenario.</summary>
    /// <param name="scenario">The recovery scenario.</param>
    /// <param name="persistence">The persistence options.</param>
    /// <param name="idempotencyStore">The idempotency store.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private static Task RunRecoveryAsync(
        RecoveryScenarioBuilder scenario,
        PersistenceOptions persistence,
        RpcMutationIdempotencyStore idempotencyStore,
        CancellationToken cancellationToken)
    {
        var deps = new RecoveryDependencies<object?>(persistence, scenario.Ledger, scenario.Cache, new AsyncManualResetEvent(true), idempotencyStore, StoreFactory.CreateReader());
        var recovery = new RecoveryService<object?>(new RecoveryOptions { BlockOnStart = true }, NullLogger<RecoveryService<object?>>.Instance, deps);
        return recovery.StartAsync(cancellationToken);
    }

    /// <summary>Writes a put and its idempotency outcome to the journal.</summary>
    /// <param name="persistence">The persistence options.</param>
    /// <param name="manifestStore">The manifest store.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private static async Task WritePutAndIdempotencyAsync(PersistenceOptions persistence, Ledger manifestStore, CancellationToken cancellationToken)
    {
        var readCurrentOrDefaultAsync = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(persistence, readCurrentOrDefaultAsync, manifestStore, new AsyncManualResetEvent(true));
        await journal.AppendPutAsync(CacheKey.Default("compact-key"), JournalEntryPayloadKit.EncodePut("v"), cancellationToken);
        var bytes = IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true });
        await journal.AppendIdempotencyOutcomeAsync(OperationId, Fingerprint, bytes, cancellationToken);
        await journal.AwaitDurabilityCommitAsync(cancellationToken);
    }

    /// <summary>Writes a started snapshot for the given scenario.</summary>
    /// <param name="scenario">The recovery scenario.</param>
    /// <param name="persistence">The persistence options.</param>
    /// <param name="nextSequence">The next sequence number.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private async Task WriteStartedSnapshotAsync(RecoveryScenarioBuilder scenario, PersistenceOptions persistence, ulong nextSequence, CancellationToken cancellationToken)
    {
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        store.RestoreStarted(OperationId, Fingerprint, DateTime.UtcNow);
        var records = new List<PersistedIdempotencyRecord>();
        IIdempotencySnapshotExporter exporter = store;
        exporter.ExportSnapshot(records, DateTime.UtcNow);

        var path = await StoreFactory.CreateWriter(persistence).WriteAsync(1, [], records, cancellationToken).ConfigureAwait(false);
        var prev = await scenario.Ledger.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
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
            cancellationToken).ConfigureAwait(false);
    }
}
