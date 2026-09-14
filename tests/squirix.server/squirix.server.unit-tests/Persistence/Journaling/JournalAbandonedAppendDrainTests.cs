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
using Xunit;

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
    [Fact]
    public async Task FailedRollFailsPendingDurableAppends()
    {
        var options = CreateOptions(Dir);
        using var ledger = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            ledger,
            new AsyncManualResetEvent(true));
        await journal.WaitForStartupAsync(DefaultCancellationToken);
        var pipelined = Assert.IsType<JournalCoordinator>(journal);

        var payload = JournalEntryPayloadKit.EncodePut("v");
        await journal.AppendPutAndAwaitDurabilityAsync(CacheKey.Default("seed"), payload, DefaultCancellationToken);
        var baseline = pipelined.QueuedAppendsCounter.Value;

        var overflowPayload = new byte[LargePayloadSize];
        Array.Fill(overflowPayload, Convert.ToByte('y'));
        var overflowKey = CacheKey.Default("overflow-key");
        await FillSegmentOneForOverflowAsync(pipelined, FrameLength(overflowPayload, overflowKey), DefaultCancellationToken);

        // Calibration roll like in BlockedManifestStillAppendsFrames: it advances the manifest
        // numbering so the file blocked below is the one the journal-triggered roll writes.
        // Without it the roll targets a different manifest file and succeeds instead of failing.
        await EnqueueCalibrationRollAsync(ledger);

        await BlockManifestFileAsync(Dir, 2, DefaultCancellationToken);

        // The overflow append is non-durable like in the roll tests: a durable overflow would
        // bypass the staging deferral path and fail the pipeline with a roll error instead of
        // parking. The durable followers queue behind it in ring order and never dequeue while
        // it is parked, so the drain below faults all of them.
        await journal.AppendPutAsync(overflowKey, overflowPayload, DefaultCancellationToken);
        var pending = StartDurableAppends(journal, payload, DefaultCancellationToken);

        await pipelined.WaitUntilAsync(static j => j.HasFlushLoopFailure, TimeSpan.FromSeconds(15), DefaultCancellationToken);
        Assert.True(journal.HasFlushLoopFailure);

        var all = Task.WhenAll(pending);
        await all.WaitUntilAsync(static t => t.IsCompleted, TimeSpan.FromSeconds(30), DefaultCancellationToken);
        Assert.True(all.IsFaulted, "Append tasks completed instead of faulting.");
        _ = all.Exception;

        // Task.WhenAll faults when any constituent faults: assert every append faulted, not just one.
        for (var i = 0; i < pending.Count; i++)
        {
            Assert.True(pending[i].IsFaulted, "Append task completed instead of faulting.");
            _ = pending[i].Exception;
        }

        Assert.Equal(baseline, pipelined.QueuedAppendsCounter.Value);
    }

    /// <summary>
    /// A failing maintenance action ("Begin" processed, End never enqueued) drains durable appends
    /// parked behind the roll-deferred frame instead of leaving them hanging. A concurrent flush
    /// waiter is failed by the same catch branch.
    /// </summary>
    [Fact]
    public async Task FailingMaintenanceDrainsPendingAppends()
    {
        var options = CreateOptions(Dir);
        using var ledger = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            ledger,
            new AsyncManualResetEvent(true));
        await journal.WaitForStartupAsync(DefaultCancellationToken);
        var pipelined = Assert.IsType<JournalCoordinator>(journal);

        var payload = JournalEntryPayloadKit.EncodePut("v");
        await journal.AppendPutAndAwaitDurabilityAsync(CacheKey.Default("seed"), payload, DefaultCancellationToken);
        var baseline = pipelined.QueuedAppendsCounter.Value;

        var overflowPayload = new byte[LargePayloadSize];
        Array.Fill(overflowPayload, Convert.ToByte('y'));
        var overflowKey = CacheKey.Default("overflow-key");
        await FillSegmentOneForOverflowAsync(pipelined, FrameLength(overflowPayload, overflowKey), DefaultCancellationToken);

        var gate = new BlockingMaintenanceAction();
        var maintenance = journal.ExecuteMaintenanceExclusiveAsync(gate.RunAsync, DefaultCancellationToken);
        await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, DefaultCancellationToken);
        await BlockManifestFileAsync(Dir, 2, DefaultCancellationToken);

        // Non-durable overflow (see above): it parks on the roll-deferred frame while the
        // durable followers stay queued behind it for the drain.
        await journal.AppendPutAsync(overflowKey, overflowPayload, DefaultCancellationToken);
        var pending = StartDurableAppends(journal, payload, DefaultCancellationToken);
        QueueFlushWait(pending, journal, DefaultCancellationToken);

        _ = gate.Release.TrySetResult();
        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(maintenance);
        Assert.Same(gate.Original, thrown);

        await pipelined.WaitUntilAsync(static j => j.HasFlushLoopFailure, TimeSpan.FromSeconds(15), DefaultCancellationToken);

        var all = Task.WhenAll(pending);
        await all.WaitUntilAsync(static t => t.IsCompleted, TimeSpan.FromSeconds(30), DefaultCancellationToken);
        Assert.True(all.IsFaulted, "Append tasks completed instead of faulting.");
        _ = all.Exception;

        // Task.WhenAll faults when any constituent faults: assert every append faulted, not just one.
        for (var i = 0; i < pending.Count; i++)
        {
            Assert.True(pending[i].IsFaulted, "Append task completed instead of faulting.");
            _ = pending[i].Exception;
        }

        Assert.Equal(baseline, pipelined.QueuedAppendsCounter.Value);
    }

    private static Task AppendDurableAsync(IJournalCoordinator journal, CacheKey key, byte[] payload, CancellationToken cancellationToken) =>
        journal.AppendPutAndAwaitDurabilityAsync(key, payload, cancellationToken).AsTask();

    private static Task AwaitFlushAsync(IJournalCoordinator journal, CancellationToken cancellationToken) =>
        journal.AwaitDurabilityCommitAsync(cancellationToken).AsTask();

    private static void QueueFlushWait(List<Task> pending, IJournalCoordinator journal, CancellationToken cancellationToken) =>
        pending.Add(AwaitFlushAsync(journal, cancellationToken));

    private static Task BlockManifestFileAsync(string dir, int index, CancellationToken cancellationToken) =>
        File.WriteAllBytesAsync(NodePathKit.Combine(dir, StoreTestSupport.ManifestDataFileName(index)), [], cancellationToken);

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

    private static List<Task> StartDurableAppends(IJournalCoordinator journal, byte[] payload, CancellationToken cancellationToken)
    {
        var pending = new List<Task>(PendingAppends);
        for (var i = 0; i < PendingAppends; i++)
            pending.Add(AppendDurableAsync(journal, CacheKey.Default("pending-" + NodeInvariantIndexStrings.Format(i)), payload, cancellationToken));

        return pending;
    }

    private static PersistenceOptions CreateOptions(string dataDir) => new()
    {
        DataDir = dataDir,
        JournalMaxSegmentMb = 1,
        FlushInterval = 600_000,
        ManifestRetentionCount = 3,
    };

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

            await journal.AppendPutAsync(fillKey, fillPayload, cancellationToken);
            await journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        Assert.Equal(1, journal.CurrentSegmentIndex);
        Assert.True(journal.ActiveSegmentWrittenBytes + overflowFrameLen > maxBytes);
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
