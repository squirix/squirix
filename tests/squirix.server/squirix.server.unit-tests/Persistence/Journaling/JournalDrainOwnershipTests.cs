using System;
using System.Collections.Generic;
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
        pipeline.FailPendingDurabilityAcks(reason);
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
        _ = state.DurabilityAcks.TakeAll(reason);

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

    /// <summary>A quiescence timeout fails reachable waiters loudly instead of hanging disposal.</summary>
    [Test]
    public async Task QuiesceTimeoutFailsWaitersLoudly()
    {
        using var fake = new FakeCoordinatorState(CreateOptions());
        var gate = new JournalProducerGate();
        IJournalCoordinatorState state = fake;
        var pipeline = new JournalDurabilityCoordinator(state, new FakeSnapshotState(), NullLogger.Instance, gate);
        gate.Enter();
        try
        {
            var failures = new List<Exception>();
            var thrown = await NodeAsyncAssert.ThrowsAsync<TimeoutException>(pipeline.QuiesceProducersAsync(failures, TimeSpan.FromMilliseconds(50)));
            var singleFailure = await Assert.That(failures).HasSingleItem();
            _ = await Assert.That(thrown).IsSameReferenceAs(singleFailure);
        }
        finally
        {
            gate.Exit();
        }
    }

    private static JournalDurabilityCoordinator CreatePipeline(FakeCoordinatorState state) => new(state, new FakeSnapshotState(), NullLogger.Instance, new JournalProducerGate());

    private PersistenceOptions CreateOptions() => new()
    {
        DataDir = Dir,
        JournalMaxSegmentMb = 1,
        FlushInterval = 600_000,
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
        private int _disposed;

        internal FakeCoordinatorState(PersistenceOptions options, int ringCapacity = 4)
        {
            _options = options;
            _ring = new BoundedJournalRing(ringCapacity);
            _ledger = new Ledger(options);
            _segmentWriter = JournalSegmentWriterFactory.Create(options.JournalPlatformBackend);
            _eventLoop = new JournalEventLoop(
                new FakeEventLoopHost(_pendingAppends),
                _ring,
                _segmentWriter,
                options,
                new JournalEventLoopStartup(1, 0, 0),
                _backgroundCancellation.Token);
        }

        CancellationTokenSource IJournalCoordinatorState.BackgroundCancellation => _backgroundCancellation;

        DurabilityAckRegistry IJournalCoordinatorState.DurabilityAcks { get; } = new();

        MutableInt32 IJournalCoordinatorState.DurabilityFlushScheduledFlag { get; } = new();

        JournalEventLoop IJournalCoordinatorState.EventLoop => _eventLoop;

        JournalDurabilityGroupCommit? IJournalCoordinatorState.GroupCommit => null;

        Thread IJournalCoordinatorState.JournalThread => Thread.CurrentThread;

        Ledger IJournalCoordinatorState.Ledger => _ledger;

        PersistenceOptions IJournalCoordinatorState.Options => _options;

        PendingAppendRegistry IJournalCoordinatorState.PendingAppends => _pendingAppends;

        MutableInt32 IJournalCoordinatorState.QueuedAppendsCounter { get; } = new();

        BoundedJournalRing IJournalCoordinatorState.Ring => _ring;

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

        Exception? IJournalCoordinatorState.GetJournalThreadFailure() => _failure.Read();

        bool IJournalCoordinatorState.TrySetJournalThreadFailure(Exception reason) => _failure.TryWriteIfNull(reason);
    }

    private sealed class FakeEventLoopHost : IJournalEventLoopHost
    {
        private readonly PendingAppendRegistry _pendingAppends;

        internal FakeEventLoopHost(PendingAppendRegistry pendingAppends)
        {
            _pendingAppends = pendingAppends;
        }

        PendingAppendRegistry IJournalEventLoopHost.PendingAppends => _pendingAppends;

        void IJournalEventLoopHost.CompleteDurabilityCheckpoint(JournalWorkItem item) => _ = item.Ack?.TrySetResult();

        void IJournalEventLoopHost.DecrementQueuedAppends()
        {
        }

        void IJournalEventLoopHost.FailPipeline(Exception reason)
        {
        }

        void IJournalEventLoopHost.PublishRoll(int targetSegmentIndex)
        {
        }

        void IJournalEventLoopHost.SetNextSequence(ulong value)
        {
        }

        void IJournalEventLoopHost.ThrowIfJournalThreadFailed()
        {
        }
    }

    private sealed class FakeSnapshotState : IJournalCoordinatorSnapshotState
    {
        QuiescenceGate IJournalCoordinatorSnapshotState.InFlightApplyGate { get; } = new();

        AsyncLock IJournalCoordinatorSnapshotState.MutationGate { get; } = new();
    }
}
