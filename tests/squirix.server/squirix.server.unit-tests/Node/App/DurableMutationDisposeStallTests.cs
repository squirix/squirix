using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App;

/// <summary>
/// Journal disposal over an fsync that never returns: after the join budget, shutdown releases every caller past the decision point,
/// including the callers whose fsync is in flight, and reports the leaked journal thread loudly.
/// </summary>
[Immutable]
public sealed class DurableMutationDisposeStallTests : IsolatedStorageTestBase
{
    private const int ApplyWaitTimedOutEventId = 3015;

    private const int CommitUnknownEventId = 1015;

    private const int JoinTimedOutEventId = 3013;

    private const int LeakedOnShutdownEventId = 3014;

    private static readonly TimeSpan CancelObservationWindow = TimeSpan.FromMilliseconds(500);

    private static readonly TimeSpan ShutdownBudget = TimeSpan.FromMilliseconds(250);

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    private static readonly string KeyA = CacheKey.Default("a").ToString();

    /// <summary>A checkpoint waiter canceled while its fsync is in flight cannot remove it and observes the fsync outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CallerCancelDoesNotWinInFlightAck(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        await journal.Journal.AppendPutAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        journal.Writer.Flush.Arm();
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var failure = new IOException("fsync failed after the caller canceled");

        var commit = journal.Journal.AwaitDurabilityCommitAsync(caller.Token).AsTask();
        bool completedWhileStalled;
        try
        {
            await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            await caller.CancelAsync();
            completedWhileStalled = await Task.WhenAny(commit, Task.Delay(CancelObservationWindow, TimeProvider.System, cancellationToken)) == commit;
        }
        finally
        {
            journal.Writer.Flush.ReleaseWithFailure(failure);
        }

        var observed = await NodeAsyncAssert.ThrowsAsync<IOException>(commit.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(completedWhileStalled).IsFalse();
        _ = await Assert.That(observed).IsSameReferenceAs(failure);
    }

    /// <summary>Disposal over a stuck fsync faults its caller within the budget and reports the leaked journal thread as a timeout.</summary>
    /// <param name="groupCommit">Whether the stuck fsync belongs to a taken group commit batch instead of a plain checkpoint.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DisposeReleasesStuckFsyncCaller(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, ShutdownBudget, NullLogger.Instance, cancellationToken);
        var memory = new AppliedKeys();
        var put = StartStuckPutAsync(journal, memory, NullLogger.Instance, cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        var (disposeError, putError) = await DisposeOverStuckFsyncAsync(journal, put, null, cancellationToken);

        _ = await Assert.That(IsShutdownTimeout(disposeError)).IsTrue().Because(disposeError.ToString());
        _ = await Assert.That(putError).IsTypeOf<SquirixException>();
        _ = await Assert.That(memory.Snapshot).IsEmpty();
    }

    /// <summary>
    /// A caller whose frame is on the ring and whose durability wait is faulted by disposal gets the stable commit-unknown contract (gRPC
    /// Unavailable with COMMIT_OUTCOME_UNKNOWN) instead of the raw shutdown fault, which is logged as the cause at warning level.
    /// </summary>
    /// <param name="groupCommit">Whether the stuck fsync belongs to a taken group commit batch instead of a plain checkpoint.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PostRingShutdownMapsToUnknown(bool groupCommit, CancellationToken cancellationToken)
    {
        var log = new LeakRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, ShutdownBudget, NullLogger.Instance, cancellationToken);
        var memory = new AppliedKeys();
        var put = StartStuckPutAsync(journal, memory, log, cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        var (_, putError) = await DisposeOverStuckFsyncAsync(journal, put, null, cancellationToken);
        var transport = ToTransport(putError);
        var replayed = journal.Recover(string.Empty, 0, cancellationToken);

        _ = await Assert.That(putError).IsTypeOf<SquirixException>().Because(putError.ToString());
        _ = await Assert.That(transport?.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(transport?.Status.Detail).IsEqualTo(ServerOpContract.CommitOutcomeUnknownDetail);
        _ = await Assert.That(log.Find(CommitUnknownEventId)?.Level).IsEqualTo(LogLevel.Warning);
        _ = await Assert.That(log.Cause(CommitUnknownEventId)).IsTypeOf<ObjectDisposedException>();
        _ = await Assert.That(memory.Snapshot).IsEmpty();
        _ = await Assert.That(replayed).IsEqualTo(KeyA);
    }

    /// <summary>
    /// The pipeline failure latch that faults a caller whose frame is on the ring surfaces as the stable commit-unknown contract, with the
    /// latch reason logged as the cause at warning level.
    /// </summary>
    /// <param name="groupCommit">Whether the stuck fsync belongs to a taken group commit batch instead of a plain checkpoint.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PostRingLatchMapsToUnknown(bool groupCommit, CancellationToken cancellationToken)
    {
        var log = new LeakRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        var memory = new AppliedKeys();
        var reason = new IOException("journal device lost");
        var put = StartStuckPutAsync(journal, memory, log, cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        Exception error;
        try
        {
            journal.Journal.FailJournalPipeline(reason);
            error = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        }
        finally
        {
            journal.Writer.Flush.Release();
        }

        var cause = log.Cause(CommitUnknownEventId);

        _ = await Assert.That(error).IsTypeOf<SquirixException>().Because(error.ToString());
        _ = await Assert.That(ToTransport(error)?.Status.Detail).IsEqualTo(ServerOpContract.CommitOutcomeUnknownDetail);
        _ = await Assert.That(log.Find(CommitUnknownEventId)?.Level).IsEqualTo(LogLevel.Warning);
        _ = await Assert.That(ReferenceEquals(cause, reason) || ReferenceEquals(cause?.InnerException, reason)).IsTrue().Because(cause?.ToString() ?? "no cause logged");
        _ = await Assert.That(memory.Snapshot).IsEmpty();
    }

    /// <summary>
    /// A caller still parked on the mutation gate when disposal starts has no frame on the ring: it keeps the definite disposal failure, is
    /// not reported as commit-unknown, and leaves nothing to replay.
    /// </summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PreRingShutdownStaysDefinite(bool groupCommit, CancellationToken cancellationToken)
    {
        var log = new LeakRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        var memory = new AppliedKeys();

        Exception error;
        var gate = await journal.Journal.MutationGate.LockAsync(cancellationToken);
        try
        {
            // The put parks on the gate held here, before its frame is encoded or enqueued.
            var put = memory.PutAsync(new DurableMutationExecutor(journal.Journal) { Log = log }, journal.Journal, "a", cancellationToken);
            await journal.ShutdownAsync().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            error = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        }
        finally
        {
            gate.Dispose();
        }

        var replayed = journal.Recover(string.Empty, 0, cancellationToken);

        _ = await Assert.That(error).IsTypeOf<InvalidOperationException>().Because(error.ToString());
        _ = await Assert.That(error.InnerException).IsTypeOf<ObjectDisposedException>();
        _ = await Assert.That(log.Find(CommitUnknownEventId)).IsNull();
        _ = await Assert.That(memory.Snapshot).IsEmpty();
        _ = await Assert.That(replayed).IsEmpty();
    }

    /// <summary>A frame whose caller was faulted by disposal over a stuck fsync is still replayed after a restart, while memory never applied it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposedFrameStaysDurableAfterRestart(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, ShutdownBudget, NullLogger.Instance, cancellationToken);
        var memory = new AppliedKeys();
        var put = StartStuckPutAsync(journal, memory, NullLogger.Instance, cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        var (_, putError) = await DisposeOverStuckFsyncAsync(journal, put, null, cancellationToken);
        var replayed = journal.Recover(string.Empty, 0, cancellationToken);

        _ = await Assert.That(putError).IsTypeOf<SquirixException>();
        _ = await Assert.That(replayed).IsEqualTo(KeyA);
        _ = await Assert.That(memory.Snapshot).IsEmpty();
    }

    /// <summary>
    /// Disposal waits for a memory apply still counted in the in-flight apply gate only up to the wait floor, then tears down and
    /// reports the unfinished applies.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeBoundsApplyWait(CancellationToken cancellationToken)
    {
        var log = new LeakRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, true, ShutdownBudget, log, cancellationToken);

        // An applier that never leaves the gate: disposal must not wait for it beyond its bound.
        journal.Journal.InFlightApplyGate.Enter();
        await journal.ShutdownAsync().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var gateError = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, AsyncLockHolder>(journal.Journal.MutationGate.LockAsync(cancellationToken));

        _ = await Assert.That(gateError).IsNotNull();
        _ = await Assert.That(log.Find(ApplyWaitTimedOutEventId)?.Level).IsEqualTo(LogLevel.Error);
    }

    /// <summary>
    /// A group commit apply parked on the mutation gate after its durability ack is still applied when the gate frees up during disposal,
    /// so the caller of an already durable write gets a definite success.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeWaitsForInFlightApplies(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, true, cancellationToken);
        var memory = new AppliedKeys();
        var put = StartStuckPutAsync(journal, memory, NullLogger.Instance, cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        bool completedWhileGated;
        bool disposedWhileGated;
        Task shutdown;
        var gate = await journal.Journal.MutationGate.LockAsync(cancellationToken);
        try
        {
            // The durability ack completes, then the apply parks on the gate held here while disposal starts.
            journal.Writer.Flush.Release();
            completedWhileGated = await Task.WhenAny(put, Task.Delay(CancelObservationWindow, TimeProvider.System, cancellationToken)) == put;
            shutdown = journal.ShutdownAsync();
            disposedWhileGated = await Task.WhenAny(shutdown, Task.Delay(CancelObservationWindow, TimeProvider.System, cancellationToken)) == shutdown;
        }
        finally
        {
            gate.Dispose();
        }

        var applied = await put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await shutdown.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var replayed = journal.Recover(string.Empty, 0, cancellationToken);

        _ = await Assert.That(completedWhileGated).IsFalse();
        _ = await Assert.That(disposedWhileGated).IsFalse();
        _ = await Assert.That(applied).IsEqualTo(1);
        _ = await Assert.That(memory.Snapshot).IsEqualTo(KeyA);
        _ = await Assert.That(replayed).IsEqualTo(KeyA);
    }

    /// <summary>
    /// A group commit apply parked on the mutation gate after its durability ack is woken by disposal instead of hanging, once the
    /// bounded wait for in-flight applies is spent while the gate stays held; its frame is on the ring, so the caller gets commit-unknown.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeWakesParkedApplyOnGate(CancellationToken cancellationToken)
    {
        var log = new LeakRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, true, ShutdownBudget, NullLogger.Instance, cancellationToken);
        var memory = new AppliedKeys();
        var put = StartStuckPutAsync(journal, memory, log, cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        bool completedWhileGated;
        SquirixException error;
        var gate = await journal.Journal.MutationGate.LockAsync(cancellationToken);
        try
        {
            // The durability ack completes, then the apply parks on the gate held here.
            journal.Writer.Flush.Release();
            completedWhileGated = await Task.WhenAny(put, Task.Delay(CancelObservationWindow, TimeProvider.System, cancellationToken)) == put;
            await journal.ShutdownAsync().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            error = await NodeAsyncAssert.ThrowsAsync<SquirixException>(put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        }
        finally
        {
            gate.Dispose();
        }

        var replayed = journal.Recover(string.Empty, 0, cancellationToken);

        _ = await Assert.That(completedWhileGated).IsFalse();
        _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(log.Cause(CommitUnknownEventId)?.InnerException).IsTypeOf<ObjectDisposedException>();
        _ = await Assert.That(replayed).IsEqualTo(KeyA);
        _ = await Assert.That(memory.Snapshot).IsEmpty();
    }

    /// <summary>The pipeline failure latch faults a caller whose group commit batch fsync is in flight, without waiting for the fsync.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LatchFaultsInFlightBatch(CancellationToken cancellationToken)
    {
        var log = new LeakRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, true, cancellationToken);
        var memory = new AppliedKeys();
        var reason = new IOException("journal device lost");
        var put = StartStuckPutAsync(journal, memory, log, cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        try
        {
            journal.Journal.FailJournalPipeline(reason);
            _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        }
        finally
        {
            journal.Writer.Flush.Release();
        }

        var cause = log.Cause(CommitUnknownEventId);

        _ = await Assert.That(ReferenceEquals(cause, reason) || ReferenceEquals(cause?.InnerException, reason)).IsTrue().Because(cause?.ToString() ?? "no cause logged");
        _ = await Assert.That(memory.Snapshot).IsEmpty();
    }

    /// <summary>A stuck fsync that fails after disposal keeps the caller's shutdown fault; the late I/O error only latches the pipeline.</summary>
    /// <param name="groupCommit">Whether the stuck fsync belongs to a taken group commit batch instead of a plain checkpoint.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task LateFsyncFailureKeepsDisposeFault(bool groupCommit, CancellationToken cancellationToken)
    {
        var log = new LeakRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, ShutdownBudget, NullLogger.Instance, cancellationToken);
        var memory = new AppliedKeys();
        var lateFailure = new IOException("fsync failed after disposal");
        var put = StartStuckPutAsync(journal, memory, log, cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        var (_, putError) = await DisposeOverStuckFsyncAsync(journal, put, lateFailure, cancellationToken);

        _ = await Assert.That(putError).IsTypeOf<SquirixException>();
        _ = await Assert.That(log.Cause(CommitUnknownEventId)).IsTypeOf<ObjectDisposedException>();
        _ = await Assert.That(put.Exception?.InnerException).IsSameReferenceAs(putError);
        _ = await Assert.That(journal.Journal.GetJournalThreadFailure()).IsSameReferenceAs(lateFailure);
        _ = await Assert.That(memory.Snapshot).IsEmpty();
    }

    /// <summary>A stuck fsync that succeeds after disposal leaves the caller's shutdown fault in place and does not fail the pipeline.</summary>
    /// <param name="groupCommit">Whether the stuck fsync belongs to a taken group commit batch instead of a plain checkpoint.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task LateFsyncSuccessAfterDisposeIsNoOp(bool groupCommit, CancellationToken cancellationToken)
    {
        var log = new LeakRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, ShutdownBudget, NullLogger.Instance, cancellationToken);
        var memory = new AppliedKeys();
        var put = StartStuckPutAsync(journal, memory, log, cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        var (_, putError) = await DisposeOverStuckFsyncAsync(journal, put, null, cancellationToken);

        _ = await Assert.That(putError).IsTypeOf<SquirixException>();
        _ = await Assert.That(log.Cause(CommitUnknownEventId)).IsTypeOf<ObjectDisposedException>();
        _ = await Assert.That(put.Exception?.InnerException).IsSameReferenceAs(putError);
        _ = await Assert.That(journal.Journal.GetJournalThreadFailure()).IsNull();
        _ = await Assert.That(memory.Snapshot).IsEmpty();
    }

    /// <summary>Disposal over a stuck fsync logs the join timeout and the leaked journal thread as errors, with the faulted in-flight waiters.</summary>
    /// <param name="groupCommit">Whether the stuck fsync belongs to a taken group commit batch instead of a plain checkpoint.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task StuckFsyncDisposeReportsLeakLoudly(bool groupCommit, CancellationToken cancellationToken)
    {
        var log = new LeakRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, ShutdownBudget, log, cancellationToken);
        var put = StartStuckPutAsync(journal, new AppliedKeys(), NullLogger.Instance, cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await DisposeOverStuckFsyncAsync(journal, put, null, cancellationToken);

        _ = await Assert.That(log.Find(JoinTimedOutEventId)).IsEqualTo((LogLevel.Error, 1));
        _ = await Assert.That(log.Find(LeakedOnShutdownEventId)).IsEqualTo((LogLevel.Error, 1));
    }

    /// <summary>
    /// Disposes over the stuck fsync, then releases it (failing it with <paramref name="lateFailure" /> when set), joins the leaked journal
    /// thread and closes its writer, so the data directory can be replayed and deleted.
    /// </summary>
    /// <param name="journal">Journal whose fsync is stuck.</param>
    /// <param name="put">Mutation stuck in its durability wait.</param>
    /// <param name="lateFailure">Failure the stuck fsync ends with after disposal; <see langword="null" /> lets it succeed.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The disposal failure and the failure the put observed.</returns>
    private static async Task<(Exception Dispose, Exception Put)> DisposeOverStuckFsyncAsync(StallableJournal journal, Task<int> put, Exception? lateFailure, CancellationToken cancellationToken)
    {
        try
        {
            var disposeError = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(journal.DisposeStalledAsync().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            var putError = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            return (disposeError, putError);
        }
        finally
        {
            if (lateFailure != null)
                journal.Writer.Flush.ReleaseWithFailure(lateFailure);

            await journal.ReclaimLeakedAsync(StallTimeout);
        }
    }

    private static bool IsShutdownTimeout(Exception error)
    {
        if (error is TimeoutException)
            return true;

        if (error is not AggregateException aggregate)
            return false;

        foreach (var inner in aggregate.InnerExceptions)
        {
            if (inner is not TimeoutException)
                return false;
        }

        return true;
    }

    /// <summary>Arms the fsync stall and starts a put of key <c language="text">a</c>; wait for the stall to be entered before acting on it.</summary>
    /// <param name="journal">Journal under test.</param>
    /// <param name="memory">Memory model the put applies to.</param>
    /// <param name="log">Logger of the executor running the put.</param>
    /// <param name="cancellationToken">Caller cancellation token.</param>
    /// <returns>The put, stuck in its durability wait once the stall is entered.</returns>
    private static Task<int> StartStuckPutAsync(StallableJournal journal, AppliedKeys memory, ILogger log, CancellationToken cancellationToken)
    {
        journal.Writer.Flush.Arm();
        return memory.PutAsync(new DurableMutationExecutor(journal.Journal) { Log = log }, journal.Journal, "a", cancellationToken);
    }

    /// <summary>Maps a caller-visible failure to the gRPC status the transport boundary sends, or <see langword="null" /> when it is no stable contract.</summary>
    /// <param name="error">Failure the caller observed.</param>
    /// <returns>The mapped RPC failure.</returns>
    private static RpcException? ToTransport(Exception error) => error is SquirixException contract ? contract.ToRpcException() : null;

    /// <summary>Logger double recording the level and the faulted in-flight waiter count of each journal shutdown event.</summary>
    [ThreadSafe]
    private sealed class LeakRecordingLogger : ILogger
    {
        private const string FaultedWaitersKey = "FaultedInFlightWaiters";

        private readonly ConcurrentQueue<(int EventId, LogLevel Level, int? FaultedWaiters, Exception? Cause)> _events = new();

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            _events.Enqueue((eventId.Id, logLevel, FaultedWaiters(state), exception));

        internal Exception? Cause(int eventId)
        {
            foreach (var recorded in _events)
            {
                if (recorded.EventId == eventId)
                    return recorded.Cause;
            }

            return null;
        }

        internal (LogLevel Level, int? FaultedWaiters)? Find(int eventId)
        {
            foreach (var recorded in _events)
            {
                if (recorded.EventId == eventId)
                    return (recorded.Level, recorded.FaultedWaiters);
            }

            return null;
        }

        private static int? FaultedWaiters<TState>(TState state)
        {
            if (state is not IReadOnlyList<KeyValuePair<string, object?>> pairs)
                return null;

            foreach (var pair in pairs)
            {
                if (string.Equals(pair.Key, FaultedWaitersKey, StringComparison.Ordinal) && pair.Value is int count)
                    return count;
            }

            return null;
        }
    }
}
