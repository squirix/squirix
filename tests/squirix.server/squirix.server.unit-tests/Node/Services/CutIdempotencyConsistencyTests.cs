using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Regression tests for idempotency export timing during snapshot cut (plan step 2).</summary>
[Immutable]
public sealed class CutIdempotencyConsistencyTests : DisposableServerUnitTestBase
{
    private const string AfterFlushOperationId = "after-flush";
    private const string AtFlushOperationId = "at-flush";
    private const string OperationId = "0123456789abcdef0123456789abcdef";
    private static readonly byte[] IdempotencyResponseBytes = [1];

    private readonly Meter _testMeter = new("test");

    /// <summary>Snapshot idempotency must match the flush watermark, not outcomes recorded after the mutation gate opens.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CutMustNotExportPostFlushRecords(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-snap-cut-idempotency");
        var persistence = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 16,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = TimeSpan.Zero,
        };
        using var manifestStore = new Ledger(persistence, NullLogger<Ledger>.Instance);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            NullLoggerFactory.Instance,
            TimeProvider.System,
            out _);
        var idempotency = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        var writer = StoreFactory.CreateWriter(persistence);

        await RecordIdempotencyAsync(journal, idempotency, AtFlushOperationId, cancellationToken);
        await journal.AwaitDurabilityCommitAsync(cancellationToken);

        var snapshotPath = await CutDuringPostFlushIdempotencyAsync(journal, manifestStore, writer, idempotency, cancellationToken);

        var loaded = await StoreFactory.CreateReader().LoadStrictAsync<object?>(snapshotPath, null, cancellationToken);
        var record = await Assert.That(loaded.IdempotencyRecords).HasSingleItem();
        _ = await Assert.That(record.OperationId).IsEqualTo(AtFlushOperationId);
    }

    /// <summary>
    /// A cut between the outcome frame and the in-memory outcome exports the outcome, not the reservation: replay skips the frame at or
    /// below the cut, so a reservation in the snapshot would answer every retry with an unknown outcome.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CutAfterOutcomeFrameExportsOutcome(CancellationToken cancellationToken)
    {
        var awaitingDurability = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var durable = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        var coordinator = new RpcMutationIdempotencyCoordinator(store, StalledDurabilityJournal(awaitingDurability, durable), NullLogger<RpcMutationIdempotencyCoordinator>.Instance);

        var write = coordinator.ExecuteAsync(OperationId, "fp", 0, static (_, _) => Task.FromResult(new TryAddAsyncResponse { Added = true }), cancellationToken);
        await awaitingDurability.Task.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        var exported = new List<PersistedIdempotencyRecord>();
        IIdempotencySnapshotExporter exporter = store;
        exporter.ExportSnapshot(exported, DateTime.UtcNow);
        var replayedBeforeDurable = store.TryReplay(OperationId, "fp", TryAddAsyncResponse.Parser, out _);
        store.HoldAppendedOutcome(OperationId, "fp", [9], new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        var foreignHoldExport = new List<PersistedIdempotencyRecord>();
        exporter.ExportSnapshot(foreignHoldExport, DateTime.UtcNow);
        durable.SetResult();
        _ = await write.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        var replayedAfterDurable = store.TryReplay(OperationId, "fp", TryAddAsyncResponse.Parser, out _);

        var record = await Assert.That(exported).HasSingleItem();
        _ = await Assert.That(record.State).IsEqualTo(IdempotencyRecordState.Completed);
        _ = await Assert.That(TryAddAsyncResponse.Parser.ParseFrom(record.ResponseBytes).Added).IsTrue();
        _ = await Assert.That(replayedBeforeDurable).IsFalse();
        _ = await Assert.That(TryAddAsyncResponse.Parser.ParseFrom((await Assert.That(foreignHoldExport).HasSingleItem()).ResponseBytes).Added).IsTrue();
        _ = await Assert.That(replayedAfterDurable).IsTrue();
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    /// <summary>A journal that enqueues outcome frames at once and holds the durability wait until <paramref name="durable" /> completes.</summary>
    /// <param name="awaitingDurability">Signaled when the durability wait starts.</param>
    /// <param name="durable">Completes the durability wait.</param>
    /// <returns>The journal.</returns>
    private static IJournalCoordinator StalledDurabilityJournal(TaskCompletionSource awaitingDurability, TaskCompletionSource durable)
    {
        var expectations = new IJournalCoordinatorCreateExpectations();
        _ = expectations.Setups.WaitForStartupAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        _ = expectations.Setups.AppendIdempotencyOutcomeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<Action?>(), Arg.Any<CancellationToken>())
                        .Callback(static (_, _, _, appended, _) =>
                         {
                             appended?.Invoke();
                             return ValueTask.CompletedTask;
                         });
        _ = expectations.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>())
                        .Callback(_ =>
                         {
                             awaitingDurability.SetResult();
                             return new ValueTask(durable.Task);
                         });
        return expectations.Instance();
    }

    private static async Task<string> CutDuringPostFlushIdempotencyAsync(
        IJournalCoordinator journal,
        Ledger manifestStore,
        ISnapshotWriter writer,
        RpcMutationIdempotencyStore idempotency,
        CancellationToken cancellationToken)
    {
        var buildStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseBuild = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cut = (buildStarted, releaseBuild, journal, manifestStore, writer, idempotency);
        var snapshotPathTask = journal.ExecuteSnapshotCutAsync(
            cut,
            static (state, _, _) =>
            {
                var records = new List<PersistedIdempotencyRecord>();
                IIdempotencySnapshotExporter exporter = state.idempotency;
                exporter.ExportSnapshot(records, DateTime.UtcNow);
                return new ValueTask<IReadOnlyList<PersistedIdempotencyRecord>>(records);
            },
            static async (state, seqAtFlush, idempotencyAtFlush, ct) =>
            {
                state.buildStarted.SetResult();
                await state.releaseBuild.Task.WaitAsync(Timeout.InfiniteTimeSpan, TimeProvider.System, ct).ConfigureAwait(false);

                var prev = await state.manifestStore.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false);
                var nextIndex = (prev.LastSnapshot?.Index ?? 0) + 1;
                var path = await state.writer.WriteAsync(nextIndex, [], idempotencyAtFlush, ct).ConfigureAwait(false);
                await state.manifestStore.WriteAsync(
                    new State
                    {
                        Format = prev.Format,
                        CurrentJournal = prev.CurrentJournal,
                        NextSequence = state.journal.NextSequence,
                        LastSnapshot = new SnapshotRef
                        {
                            Index = nextIndex,
                            Path = path,
                            CreatedUtc = DateTime.UtcNow,
                            LastAppliedSequence = seqAtFlush,
                            ReplayFromJournalSegment = state.journal.CurrentSegmentIndex,
                        },
                    },
                    ct).ConfigureAwait(false);
                return path;
            },
            cancellationToken).AsTask();

        await buildStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        await RecordIdempotencyAsync(journal, idempotency, AfterFlushOperationId, cancellationToken);
        await journal.AwaitDurabilityCommitAsync(cancellationToken);
        releaseBuild.SetResult();
        return await snapshotPathTask.WaitAsync(TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
    }

    private static async Task RecordIdempotencyAsync(IJournalCoordinator journal, RpcMutationIdempotencyStore store, string operationId, CancellationToken cancellationToken)
    {
        store.RecordSuccess(operationId, "fp", IdempotencyResponseBytes, null);
        await journal.AppendIdempotencyOutcomeAsync(operationId, "fp", IdempotencyResponseBytes, null, cancellationToken);
    }
}
