using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
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
        private int _fsyncCount;

        internal SleepingFsyncSegmentWriter(TimeSpan fsyncDelay)
        {
            _fsyncDelay = fsyncDelay;
        }

        long IJournalSegmentWriter.Length => 0;

        internal int FsyncCount => Volatile.Read(ref _fsyncCount);

        /// <summary>Releases test resources.</summary>
        public void Dispose()
        {
        }

        void IJournalSegmentWriter.FlushToDisk()
        {
            _ = Interlocked.Increment(ref _fsyncCount);
            if (_fsyncDelay > TimeSpan.Zero)
                Thread.Sleep(_fsyncDelay);
        }

        void IJournalSegmentWriter.OpenSegment(string path, bool append)
        {
        }

        void IJournalSegmentWriter.Truncate(long length)
        {
        }

        void IJournalSegmentWriter.Write(ReadOnlySpan<byte> buffer, long fileOffset)
        {
        }
    }
}
