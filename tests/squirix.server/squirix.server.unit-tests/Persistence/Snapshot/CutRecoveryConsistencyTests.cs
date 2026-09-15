using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>Regression tests for snapshot cut recovery metadata consistency (plan step 1).</summary>
[Immutable]
public sealed class CutRecoveryConsistencyTests : DisposableServerUnitTestBase
{
    private const int FillChunkChars = 8_192;
    private const int RollOverflowChars = 16_000;
    private static readonly CacheKey BaseKey = CacheKey.Default("base");
    private static readonly CacheKey FillKey = CacheKey.Default("fill");

    private static readonly CacheKey OverflowKey = CacheKey.Default("overflow");
    private static readonly CacheKey TailKey = CacheKey.Default("tail");

    private readonly Meter _testMeter = new("test");

    /// <summary>
    /// When a segment roll happens during the slow snapshot build phase, recovery must still replay journal tail
    /// records from the closed segment. Replay-from segment and next sequence are frozen at flush time under the mutation gate.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RollDuringCutKeepsJournalTailIntact(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-snap-cut-roll-recovery");
        var persistence = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = TimeSpan.Zero,
        };
        using var manifestStore = new Ledger(persistence);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        var coordinator = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;
        var writer = StoreFactory.CreateWriter(persistence);
        var overflowPayload = JournalEntryPayloadKit.EncodePut(new string('y', RollOverflowChars));
        var overflowFrameLen = PutFrameLength(overflowPayload, OverflowKey);

        await journal.AppendPutAsync(BaseKey, JournalEntryPayloadKit.EncodePut("base"), cancellationToken);
        await journal.AwaitDurabilityCommitAsync(cancellationToken);
        await FillSegmentOneForRollAsync(coordinator, overflowFrameLen, cancellationToken);

        var snapshotRef = await CutSnapshotDuringSegmentRollAsync(coordinator, manifestStore, writer, overflowPayload, cancellationToken);
        _ = await Assert.That(snapshotRef.ReplayFromJournalSegment).IsEqualTo(1);
        _ = await Assert.That(coordinator.CurrentSegmentIndex >= 2).IsTrue();

        await AssertTailRecoveredAfterSnapshotAsync(persistence, manifestStore, cancellationToken);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static async Task<SnapshotRef> CutSnapshotDuringSegmentRollAsync(
        JournalCoordinator journal,
        Ledger manifestStore,
        ISnapshotWriter writer,
        byte[] overflowPayload,
        CancellationToken cancellationToken)
    {
        var buildStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseBuild = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cut = (buildStarted, releaseBuild, journal, manifestStore, writer);
        var snapshotTask = journal.ExecuteSnapshotCutAsync(
            cut,
            static (state, _, _) => new ValueTask<(int ReplayFromSegment, ulong NextSequence)>((state.journal.CurrentSegmentIndex, state.journal.NextSequence)),
            static async (state, seqAtFlush, flushBoundary, ct) =>
            {
                state.buildStarted.SetResult();
                await state.releaseBuild.Task.WaitAsync(Timeout.InfiniteTimeSpan, TimeProvider.System, ct).ConfigureAwait(false);

                var prev = await state.manifestStore.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false);
                var nextIndex = (prev.LastSnapshot?.Index ?? 0) + 1;
                var path = await state.writer.WriteSingleAsync(nextIndex, BaseKey, new NodeCacheEntry<object?> { Value = "base", Version = 1 }, ct).ConfigureAwait(false);
                var updated = new State
                {
                    Format = prev.Format,
                    CurrentJournal = prev.CurrentJournal,
                    NextSequence = flushBoundary.NextSequence,
                    LastSnapshot = new SnapshotRef
                    {
                        Index = nextIndex,
                        Path = path,
                        CreatedUtc = DateTime.UtcNow,
                        LastAppliedSequence = seqAtFlush,
                        ReplayFromJournalSegment = flushBoundary.ReplayFromSegment,
                    },
                };
                await state.manifestStore.WriteAsync(updated, ct).ConfigureAwait(false);
                return updated.LastSnapshot;
            },
            cancellationToken).AsTask();

        await buildStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        await journal.AppendPutAsync(TailKey, JournalEntryPayloadKit.EncodePut("tail"), cancellationToken);
        await journal.AppendPutAsync(OverflowKey, overflowPayload, cancellationToken);
        await journal.AwaitDurabilityCommitAsync(cancellationToken);
        _ = await Assert.That(journal.CurrentSegmentIndex >= 2).IsTrue();
        releaseBuild.SetResult();
        return await snapshotTask.WaitAsync(TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
    }

    private static async Task FillSegmentOneForRollAsync(JournalCoordinator journal, int overflowFrameLen, CancellationToken cancellationToken)
    {
        var fillPayload = JournalEntryPayloadKit.EncodePut(new string('x', FillChunkChars));
        var fillFrameLen = PutFrameLength(fillPayload, FillKey);
        const long maxSegmentBytes = 1024L * 1024L;

        while (journal.CurrentSegmentIndex == 1 && journal.ActiveSegmentWrittenBytes + overflowFrameLen <= maxSegmentBytes &&
               journal.ActiveSegmentWrittenBytes + fillFrameLen <= maxSegmentBytes)
        {
            await journal.AppendPutAsync(FillKey, fillPayload, cancellationToken);
            await journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        _ = await Assert.That(journal.CurrentSegmentIndex).IsEqualTo(1);
        _ = await Assert.That(journal.ActiveSegmentWrittenBytes + overflowFrameLen > maxSegmentBytes).IsTrue();
    }

    private static int PutFrameLength(ReadOnlyMemory<byte> payload, CacheKey key)
    {
        var journalRecord = new JournalRecord
        {
            Sequence = 1,
            UnixMs = 1,
            Operation = JournalOperationKind.Put,
            Key = key,
            PutEntryBytes = payload,
        };
        return JournalFraming.FrameTotalLength(BinaryJournalCodec.ComputeFrameBodyLength(journalRecord));
    }

    private async Task AssertTailRecoveredAfterSnapshotAsync(PersistenceOptions persistence, Ledger manifestStore, CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<object?>();
        var recoveryDependencies = new RecoveryDependencies<object?>(
            persistence,
            manifestStore,
            cache,
            new AsyncManualResetEvent(true),
            new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter)),
            StoreFactory.CreateReader(persistence));
        var options = new RecoveryOptions { BlockOnStart = true };
        await new RecoveryService<object?>(options, NullLogger<RecoveryService<object?>>.Instance, recoveryDependencies).StartAsync(cancellationToken);

        _ = await Assert.That((await cache.GetValueAsync(BaseKey, cancellationToken)).Value).IsEqualTo("base");
        var tailEntry = await cache.GetValueAsync(TailKey, cancellationToken);
        _ = await Assert.That(tailEntry.Found).IsTrue();
        _ = await Assert.That(tailEntry.Value).IsEqualTo("tail");
    }
}
