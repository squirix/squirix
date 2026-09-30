using System;
using System.Collections.Generic;
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

/// <summary>
/// Explicit journal stop: it never throws from disposal, is bounded by its budget and stage floors, is retryable while the journal thread
/// lives, tears the journal down exactly once, and reports a failed final flush.
/// </summary>
[Immutable]
public sealed class JournalStopTests : IsolatedStorageTestBase
{
    private const int JoinTimedOutEventId = 3013;

    private const int LeakedEventId = 3014;

    private const int MarkerTimedOutEventId = 3012;

    private const int QuiescenceTimedOutEventId = 3011;

    private const int StopFailedOnDisposeEventId = 3019;

    private const int RingCapacity = 4096;

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan StopBudget = TimeSpan.FromMilliseconds(250);

    private static readonly byte[] Payload = JournalEntryPayloadKit.EncodePut("v");

    /// <summary>A shutdown stage waits for the time left before the deadline, and never less than its floor.</summary>
    [Test]
    public async Task StageWaitNeverBelowFloor()
    {
        var floor = TimeSpan.FromSeconds(1);

        _ = await Assert.That(JournalStopper.ShutdownStageWait(10_000, 1_000, floor)).IsEqualTo(TimeSpan.FromSeconds(9));
        _ = await Assert.That(JournalStopper.ShutdownStageWait(1_000, 1_000, floor)).IsEqualTo(floor);
        _ = await Assert.That(JournalStopper.ShutdownStageWait(1_000, 5_000, floor)).IsEqualTo(floor);
        _ = await Assert.That(JournalStopper.ShutdownStageWait(2_500, 1_000, floor)).IsEqualTo(TimeSpan.FromSeconds(1.5));
    }

    /// <summary>A budget that is already spent still drains the journal, including a frame that is not yet durable, and disposes its writer without an alarm.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SpentBudgetStillDrainsIdleJournal(CancellationToken cancellationToken)
    {
        var log = new EventRecordingLogger();

        // A generous stage floor: the assertion below is about draining within it, not about the machine being fast.
        await using var journal = await StallableJournal.CreateAsync(Dir, false, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(30), log, cancellationToken);
        await journal.Journal.AppendPutDurablyUnderGateAsync(CacheKey.Default("a"), Payload, cancellationToken);
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("b"), Payload, cancellationToken);

        await journal.Journal.StopAsync(TimeSpan.FromTicks(1));

        _ = await Assert.That(journal.Writer.DisposeCount).IsEqualTo(1);
        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(StallableJournal.Describe([CacheKey.Default("a").ToString(), CacheKey.Default("b").ToString()]));
        _ = await Assert.That(log.Count(JoinTimedOutEventId)).IsEqualTo(0);
        _ = await Assert.That(log.Count(LeakedEventId)).IsEqualTo(0);
    }

    /// <summary>A stop over a stalled flush fails with a timeout and keeps the writer open; a retry after the release finishes the teardown.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StalledStopFailsThenRetryCompletes(CancellationToken cancellationToken)
    {
        var log = new EventRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, false, StopBudget, log, cancellationToken);
        var key = CacheKey.Default("a");
        journal.Writer.Flush.Arm();
        await journal.Journal.AppendPutUnderGateAsync(key, Payload, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<TimeoutException>(journal.Journal.StopAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        var writerDisposedWhileStalled = journal.Writer.DisposeCount;
        journal.Writer.Flush.Release();
        await journal.Journal.StopAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(writerDisposedWhileStalled).IsEqualTo(0);
        _ = await Assert.That(journal.Writer.DisposeCount).IsEqualTo(1);
        _ = await Assert.That(log.Count(LeakedEventId)).IsEqualTo(1);
        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(key.ToString());
    }

    /// <summary>A stop that never gets the marker onto a full ring times out without touching the writer, and a retry completes once the ring drains.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FullRingStopTimesOutThenRetries(CancellationToken cancellationToken)
    {
        var log = new EventRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, false, StopBudget, log, cancellationToken);
        journal.Writer.Write.Arm();
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("first"), Payload, cancellationToken);
        await journal.Writer.Write.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        for (var i = 0; i < RingCapacity; i++)
            await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default($"k{i}"), Payload, cancellationToken);

        // The ring is full and the journal thread is parked in its write: this producer parks on the ring inside the producer gate.
        var parked = journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("parked"), Payload, cancellationToken).AsTask();
        await WaitUntilAsync(() => journal.Journal.PendingAppends.PendingCount > RingCapacity + 1, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<TimeoutException>(journal.Journal.StopAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        var writerDisposedOnFullRing = journal.Writer.DisposeCount;
        var timedOutStages = log.Count(QuiescenceTimedOutEventId) + log.Count(MarkerTimedOutEventId);
        journal.Writer.Write.Release();
        await journal.Journal.StopAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        // The parked producer settles once the ring drains: it is admitted, or refused when the shutdown began before it entered the gate.
        _ = await Task.WhenAny(parked).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var acknowledged = new List<string> { CacheKey.Default("first").ToString() };
        for (var i = 0; i < RingCapacity; i++)
            acknowledged.Add(CacheKey.Default($"k{i}").ToString());

        if (parked.IsCompletedSuccessfully)
            acknowledged.Add(CacheKey.Default("parked").ToString());

        _ = await Assert.That(writerDisposedOnFullRing).IsEqualTo(0);
        _ = await Assert.That(timedOutStages).IsEqualTo(1);
        _ = await Assert.That(journal.Writer.DisposeCount).IsEqualTo(1);
        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(StallableJournal.Describe(acknowledged));
    }

    /// <summary>An acknowledged frame whose write is stuck when the budget runs out is still written once the write returns, and the stop succeeds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StuckWriteKeepsAcknowledgedFrame(CancellationToken cancellationToken)
    {
        var log = new SignalingLogger(JoinTimedOutEventId);
        await using var journal = await StallableJournal.CreateAsync(Dir, false, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(10), log, cancellationToken);
        var key = CacheKey.Default("a");
        journal.Writer.Write.Arm();
        await journal.Journal.AppendPutUnderGateAsync(key, Payload, cancellationToken);
        await journal.Writer.Write.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        var stop = journal.Journal.StopAsync().AsTask();
        await log.Signaled.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        journal.Writer.Write.Release();
        await stop.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(key.ToString());
        _ = await Assert.That(log.Recorded.Count(LeakedEventId)).IsEqualTo(0);
        _ = await Assert.That(log.SignaledMessage).Contains("1 accepted appends still queued", StringComparison.Ordinal);
    }

    /// <summary>A disposal that joins a stop attempt which then fails makes an attempt of its own and finishes the stop quietly.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeRetriesAfterFailingStop(CancellationToken cancellationToken)
    {
        var log = new EventRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, false, StopBudget, log, cancellationToken);
        var key = CacheKey.Default("a");
        journal.Writer.Flush.Arm();
        await journal.Journal.AppendPutUnderGateAsync(key, Payload, cancellationToken);

        var stop = journal.Journal.StopAsync().AsTask();
        var dispose = journal.Journal.DisposeAsync().AsTask();
        _ = await NodeAsyncAssert.ThrowsAsync<TimeoutException>(stop.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        journal.Writer.Flush.Release();
        await dispose.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(log.Count(StopFailedOnDisposeEventId)).IsEqualTo(0);
        _ = await Assert.That(journal.Writer.DisposeCount).IsEqualTo(1);
        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(key.ToString());
    }

    /// <summary>A grace join that succeeds reports no failure and leaves nothing open.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveredGraceJoinReportsNoFailure(CancellationToken cancellationToken)
    {
        var log = new SignalingLogger(JoinTimedOutEventId);
        await using var journal = await StallableJournal.CreateAsync(Dir, false, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(10), log, cancellationToken);
        journal.Writer.Flush.Arm();
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), Payload, cancellationToken);

        var stop = journal.Journal.StopAsync().AsTask();
        await log.Signaled.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        journal.Writer.Flush.Release();
        await stop.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(log.Recorded.Count(LeakedEventId)).IsEqualTo(0);
        _ = await Assert.That(journal.Writer.DisposeCount).IsEqualTo(1);
        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(CacheKey.Default("a").ToString());
    }

    /// <summary>Disposal over a stalled flush logs the failure and does not throw; a later stop reports it, and finishes once the stall is released.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeOverStalledFlushLogsAndDefers(CancellationToken cancellationToken)
    {
        var log = new EventRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, false, StopBudget, log, cancellationToken);
        journal.Writer.Flush.Arm();
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), Payload, cancellationToken);

        await journal.Journal.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var logged = log.Find(StopFailedOnDisposeEventId);
        _ = await NodeAsyncAssert.ThrowsAsync<TimeoutException>(journal.Journal.StopAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        journal.Writer.Flush.Release();
        await journal.Journal.StopAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(logged?.Level).IsEqualTo(LogLevel.Error);
        _ = await Assert.That(logged?.Cause).IsTypeOf<TimeoutException>();
        _ = await Assert.That(journal.Writer.DisposeCount).IsEqualTo(1);
    }

    /// <summary>A stop racing a disposal tears the journal down once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentStopAndDisposeTearDownOnce(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, TimeSpan.FromSeconds(5), new EventRecordingLogger(), cancellationToken);
        await journal.Journal.AppendPutDurablyUnderGateAsync(CacheKey.Default("a"), Payload, cancellationToken);

        var stop = journal.Journal.StopAsync().AsTask();
        var dispose = journal.Journal.DisposeAsync().AsTask();
        await Task.WhenAll(stop, dispose).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(journal.Writer.DisposeCount).IsEqualTo(1);
    }

    /// <summary>A stop after the journal stopped, or was disposed, replays the outcome instead of tearing down again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StopAfterSuccessIsIdempotent(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, TimeSpan.FromSeconds(5), new EventRecordingLogger(), cancellationToken);

        await journal.Journal.StopAsync();
        await journal.Journal.StopAsync(TimeSpan.FromTicks(1));
        await journal.Journal.DisposeAsync();
        await journal.Journal.StopAsync();

        _ = await Assert.That(journal.Writer.DisposeCount).IsEqualTo(1);
    }

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(StallTimeout);
        while (!condition())
            await Task.Delay(TimeSpan.FromMilliseconds(1), TimeProvider.System, timeout.Token);
    }

    /// <summary>Records every entry and signals the first one with a watched event id.</summary>
    [ThreadSafe]
    private sealed class SignalingLogger : ILogger
    {
        private readonly TaskCompletionSource _signaled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly int _watchedEventId;
        private string _signaledMessage = string.Empty;

        internal SignalingLogger(int watchedEventId)
        {
            _watchedEventId = watchedEventId;
        }

        /// <summary>Gets the entries logged so far.</summary>
        internal EventRecordingLogger Recorded { get; } = new();

        /// <summary>Gets the formatted message of the watched event, once it was logged.</summary>
        internal string SignaledMessage => Volatile.Read(ref _signaledMessage);

        /// <summary>Gets a task that completes once the watched event was logged.</summary>
        internal Task Signaled => _signaled.Task;

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            Recorded.Log(logLevel, eventId, state, exception, formatter);
            if (eventId.Id != _watchedEventId)
                return;

            Volatile.Write(ref _signaledMessage, formatter(state, exception));
            _ = _signaled.TrySetResult();
        }
    }
}
