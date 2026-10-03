using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Drain/cancel ownership for durability and maintenance waiters: whoever wins the registry
/// removal owns the outcome, so a failure drain fault beats a racing caller cancellation.
/// </summary>
[Immutable]
public sealed class JournalDrainOwnershipTests : IsolatedStorageTestBase
{
    /// <summary>A canceled flush reports cancellation when the caller wins the ack removal.</summary>
    [Test]
    public async Task CancelledFlushCancelsWhenCallerWins()
    {
        using var fake = new FakeCoordinatorState(CreateOptions());
        var pipeline = CreatePipeline(fake);
        using var cancelled = new CancellationTokenSource();
        var pending = pipeline.EnqueueFlushAsync(cancelled.Token);
        await cancelled.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(pending);
    }

    /// <summary>A canceled flush propagates the drain failure when the drain wins the ack removal.</summary>
    [Test]
    public async Task CancelledFlushPropagatesDrainFailure()
    {
        using var fake = new FakeCoordinatorState(CreateOptions());
        var pipeline = CreatePipeline(fake);
        using var cancelled = new CancellationTokenSource();
        var pending = pipeline.EnqueueFlushAsync(cancelled.Token);

        var reason = new InvalidOperationException("pipeline failed");
        _ = pipeline.FailPendingDurabilityAcks(reason);
        await cancelled.CancelAsync();

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(pending);
        _ = await Assert.That(thrown).IsSameReferenceAs(reason);
    }

    /// <summary>A flush arriving after a durability drain fails fast with the latched reason.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FlushFailsFastAfterDrain(CancellationToken cancellationToken)
    {
        using var fake = new FakeCoordinatorState(CreateOptions());
        IJournalCoordinatorState state = fake;
        var pipeline = CreatePipeline(fake);
        var reason = new InvalidOperationException("pipeline failed");
        _ = state.DurabilityAcks.TakeAll(reason, out _);

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(pipeline.EnqueueFlushAsync(cancellationToken));
        _ = await Assert.That(thrown).IsSameReferenceAs(reason);
    }

    /// <summary>A maintenance beginning that never enters the ring detaches its ack instead of leaking it.</summary>
    [Test]
    public async Task MaintenanceBeginDetachesAckOnRingReject()
    {
        using var fake = new FakeCoordinatorState(CreateOptions(), 1);
        IJournalCoordinatorState state = fake;
        var pipeline = CreatePipeline(fake);
        await state.Ring.EnqueueAsync(JournalWorkItem.Shutdown(), CancellationToken.None);
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(pipeline.EnqueueMaintenanceAsync(static _ => ValueTask.CompletedTask, cancelled.Token));
    }

    /// <summary>A maintenance begins arriving after a drain fails fast with the latched reason.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MaintenanceBeginFailsFastAfterDrain(CancellationToken cancellationToken)
    {
        using var fake = new FakeCoordinatorState(CreateOptions());
        IJournalCoordinatorState state = fake;
        var pipeline = CreatePipeline(fake);
        var reason = new InvalidOperationException("pipeline failed");
        _ = state.PendingAppends.FailAll(reason, NullLogger.Instance, state.QueuedAppendsCounter);

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(pipeline.EnqueueMaintenanceAsync(static _ => ValueTask.CompletedTask, cancellationToken));
        _ = await Assert.That(thrown).IsSameReferenceAs(reason);
    }

    /// <summary>A canceled maintenance wait propagates the drain failure when the drain wins the ack removal.</summary>
    [Test]
    public async Task MaintenanceCancelPropagatesDrainFault()
    {
        using var fake = new FakeCoordinatorState(CreateOptions());
        IJournalCoordinatorState state = fake;
        var pipeline = CreatePipeline(fake);
        using var cancelled = new CancellationTokenSource();
        var pending = pipeline.EnqueueMaintenanceAsync(static _ => ValueTask.CompletedTask, cancelled.Token);

        var reason = new InvalidOperationException("pipeline failed");
        _ = state.PendingAppends.FailAll(reason, NullLogger.Instance, state.QueuedAppendsCounter);
        await cancelled.CancelAsync();

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(pending);
        _ = await Assert.That(thrown).IsSameReferenceAs(reason);
    }

    /// <summary>A canceled maintenance wait reports cancellation when the caller wins the ack removal.</summary>
    [Test]
    public async Task MaintenanceCancelWinsForCaller()
    {
        using var fake = new FakeCoordinatorState(CreateOptions());
        var pipeline = CreatePipeline(fake);
        using var cancelled = new CancellationTokenSource();
        var pending = pipeline.EnqueueMaintenanceAsync(static _ => ValueTask.CompletedTask, cancelled.Token);
        await cancelled.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(pending);
    }

    /// <summary>A failure drain faults a maintenance caller waiting for End: the End wait ignores cancellation, but it never outlives the pipeline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MaintenanceEndWaitFaultsOnDrain(CancellationToken cancellationToken)
    {
        using var fake = new FakeCoordinatorState(CreateOptions());
        IJournalCoordinatorState state = fake;
        var pipeline = CreatePipeline(fake);
        using var cancelled = new CancellationTokenSource();
        var maintenance = pipeline.EnqueueMaintenanceAsync(static _ => ValueTask.CompletedTask, cancelled.Token).AsTask();
        var begin = TakeNext(state, cancellationToken);
        _ = begin.Ack?.TrySetResult();
        var end = TakeNext(state, cancellationToken);
        await cancelled.CancelAsync();

        var reason = new InvalidOperationException("pipeline failed");
        _ = state.PendingAppends.FailAll(reason, NullLogger.Instance, state.QueuedAppendsCounter);

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(maintenance);
        _ = await Assert.That(begin.Kind).IsEqualTo(JournalWorkKind.MaintenanceBegin);
        _ = await Assert.That(end.Kind).IsEqualTo(JournalWorkKind.MaintenanceEnd);
        _ = await Assert.That(thrown).IsSameReferenceAs(reason);
    }

    /// <summary>
    /// Cancelling a maintenance caller once End is on the ring does not end its wait: the caller keeps the mutation gate until End is
    /// applied, so no append is admitted against the capacity counters End is about to resync.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MaintenanceEndWaitIgnoresCancel(CancellationToken cancellationToken)
    {
        using var fake = new FakeCoordinatorState(CreateOptions());
        IJournalCoordinatorState state = fake;
        var pipeline = CreatePipeline(fake);
        using var cancelled = new CancellationTokenSource();
        var maintenance = pipeline.EnqueueMaintenanceAsync(static _ => ValueTask.CompletedTask, cancelled.Token).AsTask();
        var begin = TakeNext(state, cancellationToken);
        _ = begin.Ack?.TrySetResult();
        var end = TakeNext(state, cancellationToken);

        await cancelled.CancelAsync();
        _ = end.Ack?.TrySetResult();
        await maintenance;

        _ = await Assert.That(begin.Kind).IsEqualTo(JournalWorkKind.MaintenanceBegin);
        _ = await Assert.That(end.Kind).IsEqualTo(JournalWorkKind.MaintenanceEnd);
        _ = await Assert.That(maintenance.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>A quiescence timeout is reported to the stop instead of hanging it.</summary>
    [Test]
    public async Task QuiesceTimeoutReportsFailure()
    {
        using var fake = new FakeCoordinatorState(CreateOptions());
        var gate = new JournalProducerGate();
        IJournalCoordinatorState state = fake;
        var pipeline = new JournalDurabilityCoordinator(state, new FakeSnapshotState(), NullLogger.Instance, gate);
        gate.Enter();
        try
        {
            _ = await Assert.That(await pipeline.QuiesceProducersAsync(TimeSpan.FromMilliseconds(50))).IsFalse();
        }
        finally
        {
            gate.Exit();
        }
    }

    private static JournalDurabilityCoordinator CreatePipeline(FakeCoordinatorState state) => new(state, new FakeSnapshotState(), NullLogger.Instance, new JournalProducerGate());

    /// <summary>Takes the next ring item, waiting for it when the maintenance flow publishes it asynchronously.</summary>
    /// <param name="state">Coordinator state whose ring is drained by the test instead of a journal thread.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The dequeued work item.</returns>
    /// <exception cref="TimeoutException">No item arrived within ten seconds.</exception>
    private static JournalWorkItem TakeNext(IJournalCoordinatorState state, CancellationToken cancellationToken)
    {
        var deadline = Environment.TickCount64 + 10_000;
        JournalWorkItem? item;
        while (!state.Ring.TryDequeue(out item))
        {
            var remainingMs = deadline - Environment.TickCount64;
            if (remainingMs <= 0)
                throw new TimeoutException("the maintenance flow published no ring item.");

            state.Ring.WaitForWork(Convert.ToInt32(remainingMs), cancellationToken);
        }

        return item;
    }

    private PersistenceOptions CreateOptions() => new()
    {
        DataDir = Dir,
        JournalMaxSegmentMb = 1,
        ManifestRetentionCount = 3,
    };

    private sealed class FakeCoordinatorState : IJournalCoordinatorState, IDisposable
    {
        private readonly CancellationTokenSource _backgroundCancellation = new();
        private readonly JournalEventLoop _eventLoop;
        private readonly VolatileField<Exception> _failure = new();
        private readonly Ledger _ledger;
        private readonly PersistenceOptions _options;
        private readonly PendingAppendRegistry _pendingAppends = new();
        private readonly BoundedJournalRing _ring;
        private readonly IJournalSegmentWriter _segmentWriter;
        private readonly JournalStallProbe _stallProbe;
        private int _disposed;

        internal FakeCoordinatorState(PersistenceOptions options, int ringCapacity = 4)
        {
            _options = options;
            _ring = new BoundedJournalRing(ringCapacity);
            _ledger = new Ledger(options, NullLogger<Ledger>.Instance);
            _segmentWriter = JournalSegmentWriterFactory.Create();
            _eventLoop = new JournalEventLoop(
                new FakeEventLoopHost(_pendingAppends),
                _ring,
                _segmentWriter,
                options,
                new JournalEventLoopStartup(1, 0, 0, JournalSegmentProbe.Probe(options.DataDir, 1)),
                NullLogger<JournalEventLoop>.Instance,
                _backgroundCancellation.Token);
            _stallProbe = new JournalStallProbe(NullLogger.Instance, TimeProvider.System);
        }

        CancellationTokenSource IJournalCoordinatorState.BackgroundCancellation => _backgroundCancellation;

        DurabilityAckRegistry IJournalCoordinatorState.DurabilityAcks { get; } = new();

        MutableInt32 IJournalCoordinatorState.DurabilityFlushScheduledFlag { get; } = new();

        JournalEventLoop IJournalCoordinatorState.EventLoop => _eventLoop;

        JournalDurabilityGroupCommit? IJournalCoordinatorState.GroupCommit => null;

        Thread IJournalThreadState.JournalThread => Thread.CurrentThread;

        Ledger IJournalCoordinatorState.Ledger => _ledger;

        PersistenceOptions IJournalCoordinatorState.Options => _options;

        PendingAppendRegistry IJournalCoordinatorState.PendingAppends => _pendingAppends;

        MutableInt32 IJournalCoordinatorState.QueuedAppendsCounter { get; } = new();

        BoundedJournalRing IJournalCoordinatorState.Ring => _ring;

        JournalStallProbe IJournalStallProbeSource.StallProbe => _stallProbe;

        /// <summary>Releases the ring, ledger, and background cancellation.</summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            _ring.Dispose();
            _ledger.Dispose();
            _segmentWriter.Dispose();
            _backgroundCancellation.Dispose();
        }

        Exception? IJournalThreadState.GetJournalThreadFailure() => _failure.Read();

        bool IJournalThreadState.TrySetJournalThreadFailure(Exception reason) => _failure.TryWriteIfNull(reason);
    }

    private sealed class FakeSnapshotState : IJournalCoordinatorSnapshotState
    {
        QuiescenceGate IJournalCoordinatorSnapshotState.InFlightApplyGate { get; } = new();

        AsyncLock IJournalCoordinatorSnapshotState.MutationGate { get; } = new();
    }
}
