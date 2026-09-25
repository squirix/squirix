using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Persistence.Manifest;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Abandoned ring items (issue #569): when the journal thread can no longer complete admitted
/// appends, their uncancellable acks must fail and the queued-appends counter must return to
/// baseline instead of hanging and leaking.
/// </summary>
[Immutable]
public sealed class JournalAbandonedAppendDrainTests : IsolatedStorageTestBase
{
    private const int FillPayloadSize = 8_192;
    private const int LargePayloadSize = 16_000;
    private const int PendingAppends = 4;

    /// <summary>
    /// A failed segment roll fails the pipeline: durable appends parked behind the
    /// roll-deferred frame fault instead of hanging, and the counter returns to baseline.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedRollFailsPendingDurableAppends(CancellationToken cancellationToken)
    {
        var options = CreateOptions(Dir);
        using var ledger = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(options, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger, new AsyncManualResetEvent(true));
        await journal.WaitForStartupAsync(cancellationToken);
        var pipelined = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;

        var payload = JournalEntryPayloadKit.EncodePut("v");
        await journal.AppendPutDurablyUnderGateAsync(CacheKey.Default("seed"), payload, cancellationToken);
        var baseline = pipelined.QueuedAppendsCounter.Value;

        var overflowPayload = new byte[LargePayloadSize];
        Array.Fill(overflowPayload, Convert.ToByte('y'));
        var overflowKey = CacheKey.Default("overflow-key");
        await FillSegmentOneForOverflowAsync(pipelined, FrameLength(overflowPayload, overflowKey), cancellationToken);

        // Calibration roll like in BlockedManifestStillAppendsFrames: it advances the manifest
        // numbering so the file blocked below is the one the journal-triggered roll writes.
        // Without it the roll targets a different manifest file and succeeds instead of failing.
        await EnqueueCalibrationRollAsync(ledger);

        await BlockManifestFileAsync(Dir, 2, cancellationToken);

        // The overflow append is non-durable like in the roll tests: a durable overflow would
        // bypass the staging deferral path and fail the pipeline with a roll error instead of
        // parking. The durable followers queue behind it in ring order and never dequeue while
        // it is parked, so the drain below faults all of them.
        await journal.AppendPutUnderGateAsync(overflowKey, overflowPayload, cancellationToken);
        var pending = StartDurableAppends(journal, payload, AppendDurableAsync, cancellationToken);

        await pipelined.WaitUntilAsync(static j => j.HasFlushLoopFailure, TimeSpan.FromSeconds(15), cancellationToken);
        _ = await Assert.That(journal.HasFlushLoopFailure).IsTrue();

        var all = Task.WhenAll(pending);
        await all.WaitUntilAsync(static t => t.IsCompleted, TimeSpan.FromSeconds(30), cancellationToken);
        _ = await Assert.That(all.IsFaulted).IsTrue().Because("Append tasks completed instead of faulting.");
        _ = all.Exception;

        // Task.WhenAll faults when any constituent faults: assert every append faulted, not just one.
        for (var i = 0; i < pending.Count; i++)
        {
            _ = await Assert.That(pending[i].IsFaulted).IsTrue().Because("Append task completed instead of faulting.");
            _ = pending[i].Exception;
        }

        _ = await Assert.That(pipelined.QueuedAppendsCounter.Value).IsEqualTo(baseline);
    }

    /// <summary>
    /// A failing maintenance action ("Begin" processed, End never enqueued) drains durable appends
    /// parked behind the roll-deferred frame instead of leaving them hanging. A concurrent flush
    /// waiter is failed by the same catch branch.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailingMaintenanceDrainsPendingAppends(CancellationToken cancellationToken)
    {
        var options = CreateOptions(Dir);
        using var ledger = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(options, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger, new AsyncManualResetEvent(true));
        await journal.WaitForStartupAsync(cancellationToken);
        var pipelined = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;

        var payload = JournalEntryPayloadKit.EncodePut("v");
        await journal.AppendPutDurablyUnderGateAsync(CacheKey.Default("seed"), payload, cancellationToken);
        var baseline = pipelined.QueuedAppendsCounter.Value;

        var overflowPayload = new byte[LargePayloadSize];
        Array.Fill(overflowPayload, Convert.ToByte('y'));
        var overflowKey = CacheKey.Default("overflow-key");
        await FillSegmentOneForOverflowAsync(pipelined, FrameLength(overflowPayload, overflowKey), cancellationToken);

        // Calibration roll like in FailedRollFailsPendingDurableAppends: it advances the manifest
        // numbering so the file blocked below is the one the journal-triggered roll writes.
        // Without it the roll targets a different manifest file and succeeds instead of failing.
        await EnqueueCalibrationRollAsync(ledger);

        var gate = new BlockingMaintenanceAction();
        var maintenance = journal.ExecuteMaintenanceExclusiveAsync(gate.RunAsync, cancellationToken);
        await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        await BlockManifestFileAsync(Dir, 2, cancellationToken);

        // Non-durable overflow (see above): it parks on the roll-deferred frame while the
        // durable followers stay queued behind it for the drain. The maintenance holds the
        // mutation gate, so these appends run inside its hold on purpose, not through the gate.
        await journal.AppendPutAsync(overflowKey, overflowPayload, cancellationToken);
        var pending = StartDurableAppends(journal, payload, AppendDurableInsideHeldGateAsync, cancellationToken);
        QueueFlushWait(pending, journal, cancellationToken);

        _ = gate.Release.TrySetResult();
        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(maintenance);
        _ = await Assert.That(thrown).IsSameReferenceAs(gate.Original);

        await pipelined.WaitUntilAsync(static j => j.HasFlushLoopFailure, TimeSpan.FromSeconds(15), cancellationToken);

        var all = Task.WhenAll(pending);
        await all.WaitUntilAsync(static t => t.IsCompleted, TimeSpan.FromSeconds(30), cancellationToken);
        _ = await Assert.That(all.IsFaulted).IsTrue().Because("Append tasks completed instead of faulting.");
        _ = all.Exception;

        // Task.WhenAll faults when any constituent faults: assert every append faulted, not just one.
        for (var i = 0; i < pending.Count; i++)
        {
            _ = await Assert.That(pending[i].IsFaulted).IsTrue().Because("Append task completed instead of faulting.");
            _ = pending[i].Exception;
        }

        _ = await Assert.That(pipelined.QueuedAppendsCounter.Value).IsEqualTo(baseline);
    }

    /// <summary>
    /// Starts a durable append with only its admission under the gate, so every frame queues on the ring behind the parked one while its
    /// durability wait runs outside the gate. The roll may fail fast on the blocked file while this thread is still queueing: the pipeline
    /// may already be dead, and the refusal faults the returned task like the async drain path does; the asserts only require every append
    /// to fault.
    /// </summary>
    /// <param name="journal">Journal to append to.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="payload">Encoded cache entry.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The durable append.</returns>
    private static Task AppendDurableAsync(IJournalCoordinator journal, CacheKey key, byte[] payload, CancellationToken cancellationToken) =>
        journal.AppendAdmittedUnderGateAsync(
            (Key: key, Payload: payload),
            static (appender, s, ct) => appender.AppendPutAndAwaitDurabilityAsync(s.Key, s.Payload, ct),
            cancellationToken);

    /// <summary>Starts a durable append while another flow (the maintenance) holds the mutation gate, so it queues on the ring behind the parked frame.</summary>
    /// <param name="journal">Journal to append to.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="payload">Encoded cache entry.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The durable append.</returns>
    private static Task AppendDurableInsideHeldGateAsync(IJournalCoordinator journal, CacheKey key, byte[] payload, CancellationToken cancellationToken)
    {
        ValueTask pending;
        try
        {
            pending = journal.AppendPutAndAwaitDurabilityAsync(key, payload, cancellationToken);
        }
        catch (InvalidOperationException ex)
        {
            // The roll fails fast on the blocked file while this thread is still queueing: the pipeline
            // may already be dead and the fail-fast guard throws synchronously. Pack it into a faulted
            // task like the async drain path does; the asserts below only require every append to be faulted.
            return Task.FromException(ex);
        }

        return pending.AsTask();
    }

    private static Task AwaitFlushAsync(IJournalCoordinator journal, CancellationToken cancellationToken)
    {
        ValueTask pending;
        try
        {
            pending = journal.AwaitDurabilityCommitAsync(cancellationToken);
        }
        catch (InvalidOperationException ex)
        {
            // Same race as above: the flush waiter is expected to fault, whether synchronously or not.
            return Task.FromException(ex);
        }

        return pending.AsTask();
    }

    private static Task BlockManifestFileAsync(string dir, int index, CancellationToken cancellationToken) => File.WriteAllBytesAsync(
        NodePathKit.Combine(dir, StoreTestSupport.ManifestDataFileName(index)),
        [],
        cancellationToken);

    private static PersistenceOptions CreateOptions(string dataDir) => new()
    {
        DataDir = dataDir,
        JournalMaxSegmentMb = 1,
        FlushInterval = 600_000,
        ManifestRetentionCount = 3,
    };

    private static async Task EnqueueCalibrationRollAsync(Ledger ledger)
    {
        var rolled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Exception? rollError = null;
        ledger.EnqueueRoll(
            1,
            1,
            () => rolled.TrySetResult(),
            ex =>
            {
                rollError = ex;
                _ = rolled.TrySetResult();
            });
        await rolled.Task;
        rollError.ThrowIfFaulted();
    }

    private static async Task FillSegmentOneForOverflowAsync(JournalCoordinator journal, int overflowFrameLen, CancellationToken cancellationToken)
    {
        var fillPayload = new byte[FillPayloadSize];
        Array.Fill(fillPayload, Convert.ToByte('x'));
        var fillKey = CacheKey.Default("fill");
        var fillFrameLen = FrameLength(fillPayload, fillKey);
        const long maxBytes = 1024L * 1024L;

        for (var i = 0; i < 16_384 && journal.CurrentSegmentIndex == 1; i++)
        {
            if (journal.ActiveSegmentWrittenBytes + overflowFrameLen > maxBytes)
                break;

            if (journal.ActiveSegmentWrittenBytes + fillFrameLen > maxBytes)
                break;

            await journal.AppendPutUnderGateAsync(fillKey, fillPayload, cancellationToken);
            await journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        _ = await Assert.That(journal.CurrentSegmentIndex).IsEqualTo(1);
        _ = await Assert.That(journal.ActiveSegmentWrittenBytes + overflowFrameLen > maxBytes).IsTrue();
    }

    private static int FrameLength(ReadOnlyMemory<byte> payload, CacheKey key)
    {
        var record = new JournalRecord
        {
            Sequence = 1,
            UnixMs = 1,
            Operation = JournalOperationKind.Put,
            Key = key,
            PutEntryBytes = payload,
        };
        return JournalFraming.FrameTotalLength(BinaryJournalCodec.ComputeFrameBodyLength(record));
    }

    private static void QueueFlushWait(List<Task> pending, IJournalCoordinator journal, CancellationToken cancellationToken) =>
        pending.Add(AwaitFlushAsync(journal, cancellationToken));

    private static List<Task> StartDurableAppends(
        IJournalCoordinator journal,
        byte[] payload,
        Func<IJournalCoordinator, CacheKey, byte[], CancellationToken, Task> append,
        CancellationToken cancellationToken)
    {
        var pending = new List<Task>(PendingAppends);
        for (var i = 0; i < PendingAppends; i++)
            pending.Add(append(journal, CacheKey.Default("pending-" + NodeInvariantIndexStrings.Format(i)), payload, cancellationToken));

        return pending;
    }

    private sealed class BlockingMaintenanceAction
    {
        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal InvalidOperationException Original { get; } = new("compaction failed");

        internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal async ValueTask RunAsync(CancellationToken cancellationToken)
        {
            _ = Entered.TrySetResult();
            await Release.Task.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
            throw Original;
        }
    }
}
