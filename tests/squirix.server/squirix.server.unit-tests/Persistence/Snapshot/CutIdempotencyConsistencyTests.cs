using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
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
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>Regression tests for idempotency export timing during snapshot cut (plan step 2).</summary>
[Immutable]
public sealed class CutIdempotencyConsistencyTests : ServerUnitTestBase
{
    private const string AfterFlushOperationId = "after-flush";
    private const string AtFlushOperationId = "at-flush";
    private static readonly byte[] IdempotencyResponseBytes = [1];

    /// <summary>Snapshot idempotency must match the flush watermark, not outcomes recorded after the mutation gate opens.</summary>
    [Fact]
    public async Task CutMustNotExportPostFlushRecords()
    {
        using var dir = new TempDirectory("squirix-snap-cut-idempotency");
        var persistence = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 16,
            FlushIntervalMs = 600_000,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = TimeSpan.Zero,
        };
        using var manifestStore = new Ledger(persistence);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        var idempotency = new RpcMutationIdempotencyStore();
        var writer = StoreFactory.CreateWriter(persistence);

        await RecordIdempotencyAsync(journal, idempotency, AtFlushOperationId, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        var snapshotPath = await CutDuringPostFlushIdempotencyAsync(journal, manifestStore, writer, idempotency, DefaultCancellationToken);

        var loaded = await StoreFactory.CreateReader(persistence).LoadStrictAsync<object?>(snapshotPath, cancellationToken: DefaultCancellationToken);
        var record = Assert.Single(loaded.IdempotencyRecords);
        Assert.Equal(AtFlushOperationId, record.OperationId);
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
        store.RecordSuccess(operationId, "fp", IdempotencyResponseBytes);
        await journal.AppendIdempotencyOutcomeAsync(operationId, "fp", IdempotencyResponseBytes, cancellationToken);
    }
}
