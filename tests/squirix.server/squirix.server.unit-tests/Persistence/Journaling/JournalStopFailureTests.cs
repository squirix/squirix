using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>How the explicit journal stop reports a failure: a thread failure, a failed final flush, lost frames, or a failure the shutdown itself caused.</summary>
[Immutable]
public sealed class JournalStopFailureTests : IsolatedStorageTestBase
{
    private const int FailureSurfacedEventId = 3020;

    private const int MarkerTimedOutEventId = 3012;

    private const int ShutdownInducedEventId = 3021;

    private const int StopFailedOnDisposeEventId = 3019;

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    private static readonly byte[] Payload = JournalEntryPayloadKit.EncodePut("v");

    /// <summary>A journal thread that already exited on a failure is not sent a marker, and the failure is reported by the stop.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StopAfterThreadFailureSkipsMarker(CancellationToken cancellationToken)
    {
        var log = new EventRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, false, TimeSpan.FromSeconds(5), log, cancellationToken);
        var failure = new IOException("write failed");
        journal.Writer.Write.Arm();
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), Payload, cancellationToken);
        await journal.Writer.Write.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        journal.Writer.Write.ReleaseWithFailure(failure);
        _ = await Assert.That(await journal.Journal.DurabilityPipeline.TryJoinJournalThreadAsync(StallTimeout)).IsTrue();

        var reported = await NodeAsyncAssert.ThrowsAsync<IOException>(journal.Journal.StopAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(reported).IsSameReferenceAs(failure);
        _ = await Assert.That(log.Count(MarkerTimedOutEventId)).IsEqualTo(0);
        _ = await Assert.That(journal.Writer.DisposeCount).IsEqualTo(1);
    }

    /// <summary>A final flush that fails is reported by the stop, and logged as a data failure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedFinalFlushSurfacesOnStop(CancellationToken cancellationToken)
    {
        var log = new EventRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, false, TimeSpan.FromSeconds(10), log, cancellationToken);
        var failure = new IOException("final fsync failed");
        journal.Writer.Flush.Arm();
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), Payload, cancellationToken);

        var stop = journal.Journal.StopAsync().AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        journal.Writer.Flush.ReleaseWithFailure(failure);
        var reported = await NodeAsyncAssert.ThrowsAsync<IOException>(stop.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(reported).IsSameReferenceAs(failure);
        _ = await Assert.That(log.Find(FailureSurfacedEventId)?.Level).IsEqualTo(LogLevel.Error);
        _ = await Assert.That(journal.Writer.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Disposal over a failed final flush does not throw, and logs the failure as its cause.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeLogsFailedFinalFlush(CancellationToken cancellationToken)
    {
        var log = new EventRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, false, TimeSpan.FromSeconds(10), log, cancellationToken);
        var failure = new IOException("final fsync failed");
        journal.Writer.Flush.Arm();
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), Payload, cancellationToken);

        var dispose = journal.Journal.DisposeAsync().AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        journal.Writer.Flush.ReleaseWithFailure(failure);
        await dispose.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(log.Find(StopFailedOnDisposeEventId)?.Cause).IsSameReferenceAs(failure);
    }

    /// <summary>A failure the shutdown itself caused by refusing a maintenance step is not reported as a data failure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ShutdownInducedFailureIsNotRethrown(CancellationToken cancellationToken)
    {
        var log = new EventRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, false, TimeSpan.FromSeconds(10), log, cancellationToken);
        await journal.Journal.AppendPutDurablyUnderGateAsync(CacheKey.Default("a"), Payload, cancellationToken);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var maintenance = journal.Journal.ExecuteMaintenanceExclusiveAsync(
            async ct =>
            {
                _ = entered.TrySetResult();
                await release.Task.WaitAsync(ct);
            },
            cancellationToken).AsTask();
        await entered.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        // An in-flight apply holds the stop in its teardown, after it faulted the waiters and closed the pending registry, so the refused
        // maintenance end is latched before the stop reads the latch.
        journal.Journal.InFlightApplyGate.Enter();
        var stop = journal.Journal.StopAsync().AsTask();
        await WaitUntilAsync(() => journal.Journal.PendingAppends.Failure != null, cancellationToken);
        _ = release.TrySetResult();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<ObjectDisposedException>(maintenance.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        journal.Journal.InFlightApplyGate.Exit();
        await stop.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(log.Find(ShutdownInducedEventId)?.Level).IsEqualTo(LogLevel.Debug);
        _ = await Assert.That(log.Count(FailureSurfacedEventId)).IsEqualTo(0);
    }

    /// <summary>A refusal thrown by the producer gate during the stop is the shutdown itself, so the stop still succeeds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProducerGateRefusalIsNotRethrown(CancellationToken cancellationToken)
    {
        var log = new EventRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, false, TimeSpan.FromSeconds(10), log, cancellationToken);
        var gate = new JournalProducerGate();
        gate.InitiateShutdown();
        var refusal = NodeExceptionAssert.For<JournalShutdownRefusedException>().Throws(gate, static g => g.ThrowIfShutdownInitiated());

        _ = await StopWithLatchedFailureAsync(journal, refusal, cancellationToken);

        _ = await Assert.That(log.Find(ShutdownInducedEventId)?.Level).IsEqualTo(LogLevel.Debug);
        _ = await Assert.That(log.Count(FailureSurfacedEventId)).IsEqualTo(0);
    }

    /// <summary>A latched ObjectDisposedException from any other source is a data failure: the stop rethrows it and logs it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForeignDisposedFailureIsRethrown(CancellationToken cancellationToken)
    {
        var log = new EventRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, false, TimeSpan.FromSeconds(10), log, cancellationToken);
        var foreign = new ObjectDisposedException("SomeOtherComponent");

        var reported = await StopWithLatchedFailureAsync(journal, foreign, cancellationToken);

        _ = await Assert.That(reported).IsSameReferenceAs(foreign);
        _ = await Assert.That(log.Find(FailureSurfacedEventId)?.Level).IsEqualTo(LogLevel.Error);
        _ = await Assert.That(log.Count(ShutdownInducedEventId)).IsEqualTo(0);
    }

    /// <summary>Accepted frames that were abandoned without being written fail the stop, and every later stop reports the same loss.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LostFramesFailStopForGood(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, TimeSpan.FromSeconds(5), new EventRecordingLogger(), cancellationToken);
        TrackFrameNeverEnqueued(journal);

        var first = await NodeAsyncAssert.ThrowsAsync<IOException>(journal.Journal.StopAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        var later = await NodeAsyncAssert.ThrowsAsync<IOException>(journal.Journal.StopAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(first.Message).Contains("1 accepted journal frames", StringComparison.Ordinal);
        _ = await Assert.That(later).IsSameReferenceAs(first);
        _ = await Assert.That(journal.Writer.DisposeCount).IsEqualTo(1);
    }

    /// <summary>A disposal that stops a journal which lost accepted frames does not throw, and logs the loss.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeLogsLostFrames(CancellationToken cancellationToken)
    {
        var log = new EventRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, false, TimeSpan.FromSeconds(5), log, cancellationToken);
        TrackFrameNeverEnqueued(journal);

        await journal.Journal.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(log.Find(StopFailedOnDisposeEventId)?.Cause).IsTypeOf<IOException>();
    }

    /// <summary>Tracks an accepted append the journal thread will never see, as a frame lost between admission and the ring.</summary>
    /// <param name="journal">Journal to plant the frame in.</param>
    private static void TrackFrameNeverEnqueued(StallableJournal journal)
    {
        var frame = ArrayPool<byte>.Shared.Rent(16);
        _ = Interlocked.Increment(ref journal.Journal.QueuedAppendsCounter.Value);
        journal.Journal.PendingAppends.Track(JournalWorkItem.Append(frame, 16), frame, 16, null);
    }

    /// <summary>Latches <paramref name="failure" /> while a stop is held in its teardown, then lets the stop finish.</summary>
    /// <param name="journal">Journal to stop.</param>
    /// <param name="failure">Failure the journal pipeline latches during the stop.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The failure the stop reported, or <see langword="null" /> when it succeeded.</returns>
    private static async Task<Exception?> StopWithLatchedFailureAsync(StallableJournal journal, Exception failure, CancellationToken cancellationToken)
    {
        // An in-flight apply holds the stop in its teardown, after the journal thread exited, so the latch is set after the shutdown began.
        journal.Journal.InFlightApplyGate.Enter();
        var stop = journal.Journal.StopAsync().AsTask();
        await WaitUntilAsync(() => journal.Journal.PendingAppends.Failure != null, cancellationToken);
        journal.Journal.FailJournalPipeline(failure);
        journal.Journal.InFlightApplyGate.Exit();
        try
        {
            await stop.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            return null;
        }
        catch (ObjectDisposedException ex)
        {
            return ex;
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(StallTimeout);
        while (!condition())
            await Task.Delay(TimeSpan.FromMilliseconds(1), TimeProvider.System, timeout.Token);
    }
}
