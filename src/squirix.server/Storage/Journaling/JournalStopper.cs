using System;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Stops a <see cref="JournalCoordinator" />: refuses new work, enqueues the shutdown marker once, joins the journal thread, and releases the
/// writer, ring, and gates only when the thread is gone. A stop attempt that leaves the thread alive is not terminal; a later attempt resumes it.
/// </summary>
internal sealed class JournalStopper
{
    private static readonly TimeSpan InFlightApplyWaitFloor = TimeSpan.FromSeconds(1);

    private static readonly TimeSpan ShutdownStageFloor = TimeSpan.FromSeconds(1);

    private readonly Lock _gate = new();
    private readonly ILogger _log;
    private readonly JournalCoordinator _owner;
    private readonly IDisposable _segmentWriter;
    private bool _cancellationRequested;
    private Exception? _latchedBeforeShutdown;
    private bool _markerSettled;
    private bool _shutdownLatchCaptured;
    private Task? _stopAttempt;
    private bool _stopped;

    internal JournalStopper(JournalCoordinator owner, IDisposable segmentWriter, ILogger log)
    {
        ArgumentNullException.ThrowIfNull(owner);
        ArgumentNullException.ThrowIfNull(segmentWriter);
        ArgumentNullException.ThrowIfNull(log);
        _owner = owner;
        _segmentWriter = segmentWriter;
        _log = log;
    }

    /// <summary>Computes how long a shutdown stage may wait: the time left before the shared deadline, but never less than the stage floor.</summary>
    /// <param name="deadlineMs">The shared shutdown deadline as a tick count in milliseconds.</param>
    /// <param name="nowMs">The current tick count in milliseconds.</param>
    /// <param name="floor">The least a stage is granted once the deadline has passed, so an exhausted budget still gets a bounded, real attempt.</param>
    /// <returns>The wait granted to the stage.</returns>
    internal static TimeSpan ShutdownStageWait(long deadlineMs, long nowMs, TimeSpan floor)
    {
        var remainingMs = deadlineMs - nowMs;
        var remaining = remainingMs <= 0 ? TimeSpan.Zero : TimeSpan.FromMilliseconds(remainingMs);
        return remaining > floor ? remaining : floor;
    }

    /// <summary>Releases every caller the journal can still reach, pending or in flight, so none outlives a failed stop on a stuck disk.</summary>
    /// <returns>The number of released callers whose durable write never completed.</returns>
    internal int FaultReachableWaiters()
    {
        // The frames of these callers may or may not become durable, so they see a commit-unknown failure.
        var faulted = _owner.GroupCommit?.CancelPending(new ObjectDisposedException(nameof(JournalCoordinator))) ?? 0;
        _ = _owner.PendingAppends.FailAll(new ObjectDisposedException(nameof(JournalCoordinator)), _log, _owner.QueuedAppendsCounter);
        return faulted + _owner.DurabilityPipeline.FailPendingDurabilityAcks(new ObjectDisposedException(nameof(JournalCoordinator)));
    }

    /// <summary>Runs or joins a stop attempt.</summary>
    /// <param name="budget">Shared budget of the stop stages; each stage still gets its own floor once the budget is spent.</param>
    /// <returns>The running attempt when one is in flight, the outcome of the attempt that stopped the journal when it is stopped, otherwise a new attempt.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="budget" /> is not positive.</exception>
    internal ValueTask StopAsync(TimeSpan budget)
    {
        budget.ThrowIfNegativeOrZero(nameof(budget), "The stop budget must be greater than zero.");

        TaskCompletionSource attempt;
        lock (_gate)
        {
            if (_stopAttempt is { } current && (_stopped || !current.IsCompleted))
                return new ValueTask(current);

            attempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _stopAttempt = attempt.Task;
        }

        return new ValueTask(RunAttemptAsync(attempt, budget));
    }

    /// <summary>
    /// Makes the one bounded stop attempt a disposal owes when nobody stopped the journal; never throws, since the caller cannot act on a
    /// failure and a throw would abort the disposal of everything released after the journal.
    /// </summary>
    /// <returns>A task that completes when the journal is stopped or the attempt failed and was logged.</returns>
    internal async ValueTask StopOnDisposeAsync()
    {
        Exception? failure = null;

        // A running attempt is joined first; when it fails without stopping the journal, disposal makes one attempt of its own.
        var passes = IsAttemptRunning() ? 2 : 1;
        for (var pass = 0; pass < passes && !IsStopped(); pass++)
        {
#pragma warning disable CA1031 // Disposal must not throw; the failure is logged loudly and the journal stays open for a later stop.
            try
            {
                await StopAsync(HasAttempted() ? _owner.GraceJoinFloor : _owner.ShutdownBudget).ConfigureAwait(false);
                failure = null;
            }
            catch (Exception ex)
            {
                failure = ex;
            }
#pragma warning restore CA1031
        }

        if (failure != null)
            LogManager.JournalStopFailedOnDispose(_log, failure);
    }

    private static TimeSpan StageWait(long deadline, TimeSpan floor) => ShutdownStageWait(deadline, Environment.TickCount64, floor);

    private bool HasAttempted()
    {
        lock (_gate)
            return _stopAttempt != null;
    }

    private bool IsAttemptRunning()
    {
        lock (_gate)
            return _stopAttempt is { IsCompleted: false };
    }

    private bool IsStopped()
    {
        lock (_gate)
            return _stopped;
    }

    private ValueTask<bool> JoinJournalThreadAsync(TimeSpan wait) => _owner.JournalThread.IsAlive ? _owner.DurabilityPipeline.TryJoinJournalThreadAsync(wait) : new ValueTask<bool>(true);

    /// <summary>Surfaces the failure the journal thread latched, unless the shutdown itself caused it.</summary>
    /// <exception cref="Exception">The latched failure, when it is a data failure.</exception>
    private void ReportLatchedFailure()
    {
        if (_owner.GetJournalThreadFailure() is not { } failure)
            return;

        // A maintenance step refused because shutdown began latches an ObjectDisposedException, which says nothing about the frames.
        // Any other latch, or one that predates the shutdown, is a failure the caller must see: the final write or fsync may not have happened.
        if (_latchedBeforeShutdown == null && failure is ObjectDisposedException)
        {
            LogManager.JournalShutdownInducedFailureIgnored(_log, failure);
            return;
        }

        LogManager.JournalFailureSurfacedOnStop(_log, _latchedBeforeShutdown != null, failure);
        ExceptionDispatchInfo.Capture(failure).Throw();
    }

    private async Task RunAttemptAsync(TaskCompletionSource attempt, TimeSpan budget)
    {
        try
        {
            await RunStagesAsync(budget).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Every other caller of this attempt replays the same outcome.
            _ = attempt.TrySetException(ex);
            throw;
        }

        _ = attempt.TrySetResult();
    }

    private async Task RunStagesAsync(TimeSpan budget)
    {
        // Every stage shares one budget and gets a floor of its own, so an exhausted budget still yields a bounded, real attempt.
        var deadline = Environment.TickCount64 + Convert.ToInt64(budget.TotalMilliseconds);
        if (!_shutdownLatchCaptured)
        {
            _latchedBeforeShutdown = _owner.GetJournalThreadFailure();
            _shutdownLatchCaptured = true;
        }

        // The marker is enqueued once, and only after producers quiesced: the gate guarantees every admitted enqueue is published ahead
        // of it (ring FIFO), and work arriving later is rejected explicitly instead of being dropped or hung.
        if (!_markerSettled)
        {
            if (!await _owner.DurabilityPipeline.QuiesceProducersAsync(StageWait(deadline, ShutdownStageFloor)).ConfigureAwait(false))
            {
                LogManager.JournalProducerQuiescenceTimedOut(_log);
                _ = FaultReachableWaiters();
                throw new TimeoutException("journal producers did not quiesce within the shutdown budget.");
            }

            if (!await _owner.DurabilityPipeline.EnqueueShutdownMarkerAsync(StageWait(deadline, ShutdownStageFloor)).ConfigureAwait(false))
            {
                // Without the marker, cancelling or tearing down now would let the live thread exit with queued frames unwritten.
                LogManager.JournalShutdownMarkerTimedOut(_log);
                _ = FaultReachableWaiters();
                throw new TimeoutException("shutdown marker did not enter the journal ring within the shutdown budget.");
            }

            _markerSettled = true;
        }

        var joined = await JoinJournalThreadAsync(StageWait(deadline, TimeSpan.Zero)).ConfigureAwait(false);

        // The marker is on the ring (or the thread is gone) here: the thread dequeues FIFO, so everything ahead of the marker is drained
        // and written before it observes Shutdown. Cancelling earlier would make it spin on the empty ring or exit with frames queued.
        if (!_cancellationRequested)
        {
            await _owner.BackgroundCancellation.CancelAsync().ConfigureAwait(false);
            _cancellationRequested = true;
        }

        if (!joined)
        {
            var faultedInFlight = FaultReachableWaiters();
            LogManager.JournalThreadJoinTimedOut(_log, faultedInFlight);
            if (!await JoinJournalThreadAsync(StageWait(deadline, _owner.GraceJoinFloor)).ConfigureAwait(false))
            {
                // Tearing down the writer, ring, or gates under a live journal thread corrupts slot accounting and races in-flight
                // writes, so they stay open; a later stop can finish once the thread exits.
                LogManager.JournalThreadLeakedOnShutdownTimeout(_log, faultedInFlight);
                throw new TimeoutException("journal I/O thread is still alive after shutdown; writer, ring, and gates stay open until a later stop.");
            }
        }

        await TearDownAsync(deadline).ConfigureAwait(false);
        ReportLatchedFailure();
    }

    /// <summary>Releases the writer, ring, and gates once the journal thread is gone; runs once, since a stopped journal is terminal.</summary>
    /// <param name="deadline">The shared shutdown deadline.</param>
    /// <returns>A task that completes when the resources are released.</returns>
    private async Task TearDownAsync(long deadline)
    {
        try
        {
            // Nothing can reach the ring or the thread any more: collect what was admitted but never dequeued and return quarantined
            // buffers to the pool immediately.
            _ = FaultReachableWaiters();
            _owner.DurabilityPipeline.ReclaimAbandonedAppendsPostJoin();
            try
            {
                _segmentWriter.Dispose();
            }
            finally
            {
                _owner.Ring.Dispose();
                _owner.BackgroundCancellation.Dispose();
            }

            await AwaitInFlightAppliesAsync(StageWait(deadline, InFlightApplyWaitFloor)).ConfigureAwait(false);
        }
        finally
        {
            _owner.MutationGate.Dispose();
            lock (_gate)
                _stopped = true;
        }
    }

    /// <summary>
    /// Gives callers whose frame is already durable a bounded chance to apply it to memory before the mutation gate is disposed
    /// under them, so they get a definite success instead of a commit-unknown failure.
    /// </summary>
    /// <param name="wait">How long to wait for the in-flight applies.</param>
    /// <returns>A task that completes when the in-flight applies drained or the wait gave up.</returns>
    private async ValueTask AwaitInFlightAppliesAsync(TimeSpan wait)
    {
        // Called only after the journal thread joined and every durability waiter completed or faulted, so an applier still
        // counted here waits only for the mutation gate or its own memory apply. Stopping holds that gate at no point, so the wait cannot deadlock
        // with such an applier; a gate held elsewhere is bounded by the timeout, after which the applier fails on the disposed gate.
        using var timeout = new CancellationTokenSource(wait);
        try
        {
            await _owner.InFlightApplyGate.WaitAsync(timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            LogManager.JournalInFlightApplyWaitTimedOut(_log);
        }
    }
}
