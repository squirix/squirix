using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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

    private static readonly TimeSpan EntryTimeout = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan PastThreshold = TimeSpan.FromMilliseconds(JournalSlowOperationDiagnostics.WarningThresholdMs + 200);

    private static readonly TimeSpan StallBudget = TimeSpan.FromMilliseconds(JournalSlowOperationDiagnostics.WarningThresholdMs + 150);

    /// <summary>A durability wait canceled while an fsync is stalled reports the stall at the moment of cancellation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledDurabilityWaitReportsFsyncStall(CancellationToken cancellationToken)
    {
        var logger = new RecordingLogger();
        var options = CreateOptions();
        using var manifestStore = new Ledger(options);
        using var writer = new SleepingFsyncSegmentWriter(TimeSpan.Zero, holdFsync: true);
        await using var journal = new JournalCoordinator(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            writer,
            logger);
        try
        {
            await journal.AppendPutUnderGateAsync(new CacheKey("ns", "k"), new byte[] { 1 }, cancellationToken);
            using var waitBudget = new CancellationTokenSource();
            var wait = journal.AwaitDurabilityCommitAsync(waitBudget.Token).AsTask();
            await writer.FsyncEntered.Task.WaitAsync(EntryTimeout, TimeProvider.System, cancellationToken);
            waitBudget.CancelAfter(StallBudget);

            // The checkpoint's fsync is in flight, so a caller cancel cannot win it: the canceled wait settles only once that fsync
            // returns. The stall is reported at the moment of cancellation, so release the writer once the warning is recorded.
            await logger.WaitForWarningAsync(WaitCanceledEventId, EntryTimeout, cancellationToken);
            writer.Release();

            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(wait);
        }
        finally
        {
            writer.Release();
        }

        await AssertIoStallReportedAsync(logger, "durability commit", nameof(IJournalSegmentWriter.FlushToDisk));
    }

    /// <summary>A durability wait canceled while the journal thread is stuck in a segment write reports the stall.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledDurabilityWaitReportsWriteStall(CancellationToken cancellationToken)
    {
        var logger = new RecordingLogger();
        var options = CreateOptions();
        using var manifestStore = new Ledger(options);
        using var writer = new SleepingFsyncSegmentWriter(TimeSpan.Zero, holdWrite: true);
        await using var journal = new JournalCoordinator(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            writer,
            logger);
        try
        {
            await journal.AppendPutUnderGateAsync(new CacheKey("ns", "k"), new byte[] { 1 }, cancellationToken);
            await writer.WriteEntered.Task.WaitAsync(EntryTimeout, TimeProvider.System, cancellationToken);
            using var waitBudget = new CancellationTokenSource(StallBudget);

            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(journal.AwaitDurabilityCommitAsync(waitBudget.Token).AsTask());
        }
        finally
        {
            writer.Release();
        }

        await AssertIoStallReportedAsync(logger, "durability commit", nameof(IJournalSegmentWriter.Write));
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
        using var waitBudget = new CancellationTokenSource(StallBudget);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(HoldGateAsync(journal, TimeSpan.Zero, waitBudget.Token));
        await holder;

        _ = await Assert.That(logger.Count(WaitCanceledEventId)).IsEqualTo(1);
        var warning = logger.Find(WaitCanceledEventId);
        _ = await Assert.That(warning["WaitingFor"] as string).IsEqualTo("journal barrier");
        _ = await Assert.That(warning["Holder"] as string).IsEqualTo(nameof(JournalCoordinator.ExecuteUnderSnapshotBarrierAsync));
        _ = await Assert.That(warning["HeldMs"] is long heldMs ? heldMs : -1).IsGreaterThanOrEqualTo(JournalSlowOperationDiagnostics.WarningThresholdMs);
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
        using var writer = new SleepingFsyncSegmentWriter(TimeSpan.Zero, holdFsync: true);
        await using var journal = new JournalCoordinator(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            writer,
            logger);
        try
        {
            await journal.AppendPutUnderGateAsync(new CacheKey("ns", "k"), new byte[] { 1 }, cancellationToken);
            using var waitBudget = new CancellationTokenSource();
            var wait = journal.AwaitDurabilityCommitAsync(waitBudget.Token).AsTask();
            await writer.FsyncEntered.Task.WaitAsync(EntryTimeout, TimeProvider.System, cancellationToken);
            waitBudget.CancelAfter(StallBudget);

            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(wait);
        }
        finally
        {
            writer.Release();
        }

        await AssertIoStallReportedAsync(logger, "group commit", nameof(IJournalSegmentWriter.FlushToDisk));
    }

    /// <summary>A durability wait parked at a full ring while the journal thread is stuck in a write reports the stall.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledRingAdmissionReportsWriteStall(CancellationToken cancellationToken)
    {
        var logger = new RecordingLogger();
        var options = CreateOptions();
        using var manifestStore = new Ledger(options);
        using var writer = new SleepingFsyncSegmentWriter(TimeSpan.Zero, holdWrite: true);
        await using var journal = new JournalCoordinator(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            writer,
            logger);
        try
        {
            await journal.AppendPutUnderGateAsync(new CacheKey("ns", "k"), new byte[] { 1 }, cancellationToken);
            await writer.WriteEntered.Task.WaitAsync(EntryTimeout, TimeProvider.System, cancellationToken);
            for (var i = 0; i < RingCapacity; i++)
            {
                await journal.Ring.EnqueueAsync(
                    JournalWorkItem.DurabilityCheckpoint(new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)),
                    cancellationToken);
            }

            using var waitBudget = new CancellationTokenSource(StallBudget);

            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(journal.AwaitDurabilityCommitAsync(waitBudget.Token).AsTask());
        }
        finally
        {
            writer.Release();
        }

        await AssertIoStallReportedAsync(logger, "journal ring admission", nameof(IJournalSegmentWriter.Write));
    }

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

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            journal.ExecuteUnderSnapshotBarrierAsync(
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

        await journal.ExecuteUnderSnapshotBarrierAsync(PastThreshold, static async (delay, ct) => await Task.Delay(delay, TimeProvider.System, ct), cancellationToken);

        _ = await Assert.That(logger.Count(GateHeldLongEventId)).IsEqualTo(1);
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

    /// <summary>A fsync that is slow and then fails is still reported, and its failure is preserved.</summary>
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

    private static async Task AssertIoStallReportedAsync(RecordingLogger logger, string waitingFor, string operation)
    {
        _ = await Assert.That(logger.Count(WaitCanceledEventId)).IsEqualTo(1);
        var warning = logger.Find(WaitCanceledEventId);
        _ = await Assert.That(warning["WaitingFor"] as string).IsEqualTo(waitingFor);
        _ = await Assert.That(warning["Operation"] as string).IsEqualTo(operation);
        _ = await Assert.That(warning["IoMs"] is long ioMs ? ioMs : -1).IsGreaterThanOrEqualTo(JournalSlowOperationDiagnostics.WarningThresholdMs);
    }

    private static Task HoldGateAsync(JournalCoordinator journal, TimeSpan holdFor, CancellationToken cancellationToken) => journal
       .ExecuteUnderSnapshotBarrierAsync(holdFor, static async (delay, ct) => await Task.Delay(delay, TimeProvider.System, ct), cancellationToken).AsTask();

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
            EventLoop = new JournalEventLoop(new FakeEventLoopHost(), _ring, writer, options, new JournalEventLoopStartup(1, 0, 0), _backgroundCancellation.Token, logger);
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
        private readonly ConcurrentDictionary<int, TaskCompletionSource> _signals = new();
        private readonly bool _throwOnLog;
        private readonly ConcurrentQueue<(EventId EventId, Dictionary<string, object?> Values)> _warnings = new();

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
            {
                _warnings.Enqueue((eventId, CaptureValues(state)));
                _ = SignalFor(eventId.Id).TrySetResult();
            }

            if (_throwOnLog)
                throw new InvalidOperationException("log sink failed");
        }

        internal int Count(int eventId)
        {
            var count = 0;
            foreach (var recorded in _warnings)
            {
                if (recorded.EventId.Id == eventId)
                    count++;
            }

            return count;
        }

        internal Dictionary<string, object?> Find(int eventId)
        {
            foreach (var recorded in _warnings)
            {
                if (recorded.EventId.Id == eventId)
                    return recorded.Values;
            }

            throw new InvalidOperationException($"no warning with event id {eventId} was logged.");
        }

        internal Task WaitForWarningAsync(int eventId, TimeSpan timeout, CancellationToken cancellationToken) =>
            SignalFor(eventId).Task.WaitAsync(timeout, TimeProvider.System, cancellationToken);

        private static Dictionary<string, object?> CaptureValues<TState>(TState state)
        {
            var values = new Dictionary<string, object?>(StringComparer.Ordinal);
            if (state is not IReadOnlyList<KeyValuePair<string, object?>> pairs)
                return values;

            for (var i = 0; i < pairs.Count; i++)
                values[pairs[i].Key] = pairs[i].Value;

            return values;
        }

        private TaskCompletionSource SignalFor(int eventId) => _signals.GetOrAdd(eventId, static _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
    }

    private sealed class SleepingFsyncSegmentWriter : IJournalSegmentWriter
    {
        private static readonly TimeSpan HoldLimit = TimeSpan.FromSeconds(60);

        private readonly bool _failAfterDelay;
        private readonly TimeSpan _fsyncDelay;
        private readonly bool _holdFsync;
        private readonly bool _holdWrite;
        private readonly ManualResetEventSlim _release = new();
        private int _fsyncCount;

        internal SleepingFsyncSegmentWriter(TimeSpan fsyncDelay, bool failAfterDelay = false, bool holdFsync = false, bool holdWrite = false)
        {
            _fsyncDelay = fsyncDelay;
            _failAfterDelay = failAfterDelay;
            _holdFsync = holdFsync;
            _holdWrite = holdWrite;
        }

        long IJournalSegmentWriter.Length => 0;

        internal int FsyncCount => Volatile.Read(ref _fsyncCount);

        internal TaskCompletionSource FsyncEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource WriteEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Releases test resources.</summary>
        public void Dispose() => _release.Dispose();

        void IJournalSegmentWriter.FlushToDisk()
        {
            _ = Interlocked.Increment(ref _fsyncCount);
            _ = FsyncEntered.TrySetResult();
            if (_holdFsync)
                HoldUntilReleased();
            else if (_fsyncDelay > TimeSpan.Zero)
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
            if (_holdWrite)
                HoldUntilReleased();
        }

        /// <summary>Unblocks every held fsync or write, now and from then on.</summary>
        internal void Release() => _release.Set();

        private void HoldUntilReleased()
        {
            if (!_release.IsSet)
                _ = _release.Wait(HoldLimit, CancellationToken.None);
        }
    }
}
