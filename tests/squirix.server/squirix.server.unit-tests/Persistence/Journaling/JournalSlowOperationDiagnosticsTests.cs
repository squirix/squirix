using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Slow journal fsyncs and long mutation-gate holds emit warnings without affecting the operation outcome.</summary>
[Immutable]
public sealed class JournalSlowOperationDiagnosticsTests : IsolatedStorageTestBase
{
    private const int FsyncSlowEventId = 1012;

    private const int GateHeldLongEventId = 1013;

    private const int RingCapacity = 4096;

    private const int WaitCanceledEventId = 1014;

    private static readonly TimeSpan PastThreshold = TimeSpan.FromMilliseconds(JournalSlowOperationDiagnostics.WarningThresholdMs + 200);

    /// <summary>A fast fsync does not log a slow-fsync warning.</summary>
    [Test]
    public async Task FastFsyncDoesNotWarn()
    {
        var logger = new RecordingLogger();
        using var writer = new SleepingFsyncSegmentWriter(TimeSpan.Zero);
        using var loop = new EventLoopSetup(CreateOptions(), writer, logger);

        loop.EventLoop.SetDirty(true);
        loop.EventLoop.FlushToDisk();

        _ = await Assert.That(writer.FsyncCount).IsEqualTo(1);
        _ = await Assert.That(logger.Count(FsyncSlowEventId)).IsEqualTo(0);
    }

    /// <summary>A gate holder action that fails after a long hold still surfaces its own exception when the log sink throws.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LongGateHoldKeepsActionFailure(CancellationToken cancellationToken)
    {
        var logger = new RecordingLogger(true);
        var options = CreateOptions();
        using var manifestStore = new Ledger(options);
        using var writer = new SleepingFsyncSegmentWriter(TimeSpan.Zero);
        await using var journal = new JournalCoordinator(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            writer,
            logger);
        var failure = new InvalidOperationException("action failed");

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(journal.ExecuteUnderSnapshotBarrierAsync(
            failure,
            static async (reason, ct) =>
            {
                await Task.Delay(PastThreshold, TimeProvider.System, ct);
                throw reason;
            },
            cancellationToken));

        _ = await Assert.That(thrown).IsSameReferenceAs(failure);
        _ = await Assert.That(logger.Count(GateHeldLongEventId)).IsEqualTo(1);
    }

    /// <summary>A mutation gate held past the threshold logs a long-hold warning.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LongGateHoldWarns(CancellationToken cancellationToken)
    {
        var logger = new RecordingLogger();
        var options = CreateOptions();
        using var manifestStore = new Ledger(options);
        using var writer = new SleepingFsyncSegmentWriter(TimeSpan.Zero);
        await using var journal = new JournalCoordinator(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            writer,
            logger);

        await journal.ExecuteUnderSnapshotBarrierAsync(
            PastThreshold,
            static async (delay, ct) => await Task.Delay(delay, TimeProvider.System, ct),
            cancellationToken);

        _ = await Assert.That(logger.Count(GateHeldLongEventId)).IsEqualTo(1);
    }

    /// <summary>An fsync slower than the threshold logs a slow-fsync warning.</summary>
    [Test]
    public async Task SlowFsyncWarns()
    {
        var logger = new RecordingLogger();
        using var writer = new SleepingFsyncSegmentWriter(PastThreshold);
        using var loop = new EventLoopSetup(CreateOptions(), writer, logger);

        loop.EventLoop.SetDirty(true);
        loop.EventLoop.FlushToDisk();

        _ = await Assert.That(writer.FsyncCount).IsEqualTo(1);
        _ = await Assert.That(logger.Count(FsyncSlowEventId)).IsEqualTo(1);
    }

    /// <summary>An fsync that is slow and then fails is still reported, and its failure is preserved.</summary>
    [Test]
    public async Task SlowFailingFsyncWarns()
    {
        var logger = new RecordingLogger();
        using var writer = new SleepingFsyncSegmentWriter(PastThreshold, true);
        using var loop = new EventLoopSetup(CreateOptions(), writer, logger);
        loop.EventLoop.SetDirty(true);

        _ = NodeExceptionAssert.For<IOException>().Throws(loop, static l => l.EventLoop.FlushToDisk());

        _ = await Assert.That(logger.Count(FsyncSlowEventId)).IsEqualTo(1);
    }

    /// <summary>A maintenance action that holds the mutation gate past the threshold logs a long-hold warning.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LongMaintenanceGateHoldWarns(CancellationToken cancellationToken)
    {
        var logger = new RecordingLogger();
        var options = CreateOptions();
        using var manifestStore = new Ledger(options);
        using var writer = new SleepingFsyncSegmentWriter(TimeSpan.Zero);
        await using var journal = new JournalCoordinator(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            writer,
            logger);

        await journal.ExecuteMaintenanceExclusiveAsync(static async ct => await Task.Delay(PastThreshold, TimeProvider.System, ct), cancellationToken);

        _ = await Assert.That(logger.Count(GateHeldLongEventId)).IsEqualTo(1);
    }

    /// <summary>A durability wait canceled while an fsync is stalled reports the stall at the moment of cancellation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledDurabilityWaitReportsFsyncStall(CancellationToken cancellationToken)
    {
        var logger = new RecordingLogger();
        var options = CreateOptions();
        using var manifestStore = new Ledger(options);
        using var writer = new SleepingFsyncSegmentWriter(TimeSpan.FromMilliseconds(JournalSlowOperationDiagnostics.WarningThresholdMs + 800));
        await using var journal = new JournalCoordinator(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            writer,
            logger);
        await journal.AppendPutAsync(new CacheKey("ns", "k"), new byte[] { 1 }, cancellationToken);
        using var waitBudget = new CancellationTokenSource(TimeSpan.FromMilliseconds(JournalSlowOperationDiagnostics.WarningThresholdMs + 150));

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(journal.AwaitDurabilityCommitAsync(waitBudget.Token).AsTask());

        _ = await Assert.That(logger.Count(WaitCanceledEventId)).IsEqualTo(1);
    }

    /// <summary>A durability wait canceled while the journal thread is stuck in a segment write reports the stall.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledDurabilityWaitReportsWriteStall(CancellationToken cancellationToken)
    {
        var logger = new RecordingLogger();
        var options = CreateOptions();
        using var manifestStore = new Ledger(options);
        using var writer = new SleepingFsyncSegmentWriter(TimeSpan.Zero, false, TimeSpan.FromMilliseconds(JournalSlowOperationDiagnostics.WarningThresholdMs + 800));
        await using var journal = new JournalCoordinator(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            writer,
            logger);
        await journal.AppendPutAsync(new CacheKey("ns", "k"), new byte[] { 1 }, cancellationToken);
        using var waitBudget = new CancellationTokenSource(TimeSpan.FromMilliseconds(JournalSlowOperationDiagnostics.WarningThresholdMs + 150));

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(journal.AwaitDurabilityCommitAsync(waitBudget.Token).AsTask());

        _ = await Assert.That(logger.Count(WaitCanceledEventId)).IsEqualTo(1);
    }

    /// <summary>A group commit wait canceled while the journal thread is stuck in fsync reports the stall.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledGroupCommitWaitReportsFsyncStall(CancellationToken cancellationToken)
    {
        var logger = new RecordingLogger();
        var options = CreateOptions() with
        {
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(20),
            JournalGroupCommitMaxBatch = 1,
        };
        using var manifestStore = new Ledger(options);
        using var writer = new SleepingFsyncSegmentWriter(TimeSpan.FromMilliseconds(JournalSlowOperationDiagnostics.WarningThresholdMs + 800));
        await using var journal = new JournalCoordinator(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            writer,
            logger);
        await journal.AppendPutAsync(new CacheKey("ns", "k"), new byte[] { 1 }, cancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(200), TimeProvider.System, cancellationToken);
        using var waitBudget = new CancellationTokenSource(TimeSpan.FromMilliseconds(JournalSlowOperationDiagnostics.WarningThresholdMs + 150));

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(journal.AwaitDurabilityCommitAsync(waitBudget.Token).AsTask());

        _ = await Assert.That(logger.Count(WaitCanceledEventId)).IsEqualTo(1);
    }

    /// <summary>A durability wait parked at a full ring while the journal thread is stuck in a write reports the stall.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledRingAdmissionReportsWriteStall(CancellationToken cancellationToken)
    {
        var logger = new RecordingLogger();
        var options = CreateOptions();
        using var manifestStore = new Ledger(options);
        using var writer = new SleepingFsyncSegmentWriter(TimeSpan.Zero, false, TimeSpan.FromMilliseconds(JournalSlowOperationDiagnostics.WarningThresholdMs + 800));
        await using var journal = new JournalCoordinator(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            writer,
            logger);
        await journal.AppendPutAsync(new CacheKey("ns", "k"), new byte[] { 1 }, cancellationToken);
        await writer.WriteEntered.Task.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        for (var i = 0; i < RingCapacity; i++)
            await journal.Ring.EnqueueAsync(JournalWorkItem.DurabilityCheckpoint(new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)), cancellationToken);

        using var waitBudget = new CancellationTokenSource(TimeSpan.FromMilliseconds(JournalSlowOperationDiagnostics.WarningThresholdMs + 150));

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(journal.AwaitDurabilityCommitAsync(waitBudget.Token).AsTask());

        _ = await Assert.That(logger.Count(WaitCanceledEventId)).IsEqualTo(1);
    }

    /// <summary>A gate wait canceled while another holder keeps the gate past the threshold reports the stall.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledGateWaitReportsHolder(CancellationToken cancellationToken)
    {
        var logger = new RecordingLogger();
        var options = CreateOptions();
        using var manifestStore = new Ledger(options);
        using var writer = new SleepingFsyncSegmentWriter(TimeSpan.Zero);
        await using var journal = new JournalCoordinator(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            writer,
            logger);
        var holdFor = TimeSpan.FromMilliseconds(JournalSlowOperationDiagnostics.WarningThresholdMs + 800);
        var holder = HoldGateAsync(journal, holdFor, cancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(50), TimeProvider.System, cancellationToken);
        using var waitBudget = new CancellationTokenSource(TimeSpan.FromMilliseconds(JournalSlowOperationDiagnostics.WarningThresholdMs + 150));

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(HoldGateAsync(journal, TimeSpan.Zero, waitBudget.Token));
        await holder;

        _ = await Assert.That(logger.Count(WaitCanceledEventId)).IsEqualTo(1);
    }

    private static Task HoldGateAsync(JournalCoordinator journal, TimeSpan holdFor, CancellationToken cancellationToken) =>
        journal.ExecuteUnderSnapshotBarrierAsync(holdFor, static async (delay, ct) => await Task.Delay(delay, TimeProvider.System, ct), cancellationToken).AsTask();

    private PersistenceOptions CreateOptions() => new()
    {
        DataDir = Dir,
        JournalMaxSegmentMb = 4,
        FlushInterval = 600_000,
        ManifestRetentionCount = 1,
    };

    private sealed class EventLoopSetup : IDisposable
    {
        private readonly CancellationTokenSource _backgroundCancellation = new();
        private readonly BoundedJournalRing _ring = new(4);

        internal EventLoopSetup(PersistenceOptions options, IJournalSegmentWriter writer, ILogger logger)
        {
            EventLoop = new JournalEventLoop(
                new FakeEventLoopHost(),
                _ring,
                writer,
                options,
                new JournalEventLoopStartup(1, 0, 0),
                _backgroundCancellation.Token,
                logger);
        }

        internal JournalEventLoop EventLoop { get; }

        /// <summary>Releases the ring and background cancellation.</summary>
        public void Dispose()
        {
            _ring.Dispose();
            _backgroundCancellation.Dispose();
        }
    }

    private sealed class FakeEventLoopHost : IJournalEventLoopHost
    {
        PendingAppendRegistry IJournalEventLoopHost.PendingAppends { get; } = new();

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

    private sealed class RecordingLogger : ILogger
    {
        private readonly ConcurrentQueue<EventId> _events = new();
        private readonly bool _throwOnLog;

        internal RecordingLogger(bool throwOnLog = false)
        {
            _throwOnLog = throwOnLog;
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
                _events.Enqueue(eventId);

            if (_throwOnLog)
                throw new InvalidOperationException("log sink failed");
        }

        internal int Count(int eventId)
        {
            var count = 0;
            foreach (var recorded in _events)
            {
                if (recorded.Id == eventId)
                    count++;
            }

            return count;
        }
    }

    private sealed class SleepingFsyncSegmentWriter : IJournalSegmentWriter
    {
        private readonly TimeSpan _fsyncDelay;
        private readonly bool _failAfterDelay;
        private readonly TimeSpan _writeDelay;
        private int _fsyncCount;

        internal SleepingFsyncSegmentWriter(TimeSpan fsyncDelay, bool failAfterDelay = false, TimeSpan writeDelay = default)
        {
            _fsyncDelay = fsyncDelay;
            _failAfterDelay = failAfterDelay;
            _writeDelay = writeDelay;
        }

        long IJournalSegmentWriter.Length => 0;

        internal int FsyncCount => Volatile.Read(ref _fsyncCount);

        internal TaskCompletionSource WriteEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Releases test resources.</summary>
        public void Dispose()
        {
        }

        void IJournalSegmentWriter.FlushToDisk()
        {
            _ = Interlocked.Increment(ref _fsyncCount);
            if (_fsyncDelay > TimeSpan.Zero)
                Thread.Sleep(_fsyncDelay);

            if (_failAfterDelay)
                throw new IOException("fsync failed");
        }

        void IJournalSegmentWriter.OpenSegment(string path, bool append)
        {
        }

        void IJournalSegmentWriter.Truncate(long length)
        {
        }

        void IJournalSegmentWriter.Write(ReadOnlySpan<byte> buffer, long fileOffset)
        {
            _ = WriteEntered.TrySetResult();
            if (_writeDelay > TimeSpan.Zero)
                Thread.Sleep(_writeDelay);
        }
    }
}
