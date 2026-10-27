using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Covers RetentionWorker schedule/rearm and invalid DataDir cleanup paths.</summary>
[Immutable]
public sealed class RetentionWorkerTests : ServerUnitTestBase
{
    /// <summary>Invalid DataDir causes cleanup failure reporting without crashing the worker.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CleanupWithBadDirRecordsFailure(CancellationToken cancellationToken)
    {
        var outcomes = new ConcurrentQueue<bool>();
        var readiness = new IRetentionCleanupReadinessStatusCreateExpectations();
        _ = readiness.Setups.RecordWriteOutcome(Arg.Any<bool>()).Callback(outcomes.Enqueue);
        var failures = 0;
        var metrics = new IManifestRetentionFailureMetricsCreateExpectations();
        _ = metrics.Setups.RecordDeleteFailure(Arg.Any<string>(), Arg.Any<string>()).Callback((_, _) => Interlocked.Increment(ref failures));
        var context = new RetentionContext(new RetentionSettings(".."), null, NullLogger.Instance, metrics.Instance());
        var worker = new RetentionWorker(context, readiness.Instance(), new JournalCapacityResyncSignal());

        worker.ScheduleRetentionCleanup(new State { CurrentJournal = 2, LastSnapshot = new SnapshotRef { Index = 2, CreatedUtc = DateTime.UtcNow } });

        await outcomes.WaitUntilAsync(static o => !o.IsEmpty, cancellationToken);

        _ = await Assert.That(outcomes).Contains(true);
        _ = await Assert.That(Volatile.Read(ref failures) > 0).IsTrue();
    }

    /// <summary>
    /// After a stop, the pass already running finishes but no further pass starts: work queued behind it before the stop is dropped and work
    /// scheduled after the stop is ignored.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StoppedWorkerStartsNoFurtherPass(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("retention-worker-stop");
        using var probe = new FirstPassProbe(cancellationToken);
        var readiness = new IRetentionCleanupReadinessStatusCreateExpectations();
        _ = readiness.Setups.RecordWriteOutcome(Arg.Any<bool>()).Callback(Bind.To<FirstPassProbe, Action<bool>>(probe, static pass => _ => pass.RecordPass()));
        var context = new RetentionContext(new RetentionSettings(dir), null, NullLogger.Instance, NoOpRetentionFailureMetrics.Instance);
        var worker = new RetentionWorker(context, readiness.Instance(), new JournalCapacityResyncSignal());

        worker.ScheduleRetentionCleanup(new State { CurrentJournal = 1 });
        await probe.WaitUntilEndingAsync();
        worker.ScheduleRetentionCleanup(new State { CurrentJournal = 2 });
        worker.Stop();
        worker.ScheduleRetentionCleanup(new State { CurrentJournal = 3 });
        await probe.ReleaseAsync();

        // A further pass on the empty directory would report within milliseconds; the window only bounds how long the test waits for it.
        _ = await Assert.That(await probe.WaitForSecondPassAsync(TimeSpan.FromMilliseconds(500))).IsFalse();
    }

    /// <summary>A pass deletes only the segments below the replay floor and tells the journal exactly how many segments and bytes it freed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PassRecordsFreedCapacity(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("retention-worker-capacity");
        for (var index = 1; index <= 4; index++)
            BinaryJournalTestSegmentWriter.WriteJournalSegment(dir, index, BinaryJournalTestSegmentWriter.BuildPutRecord(Convert.ToUInt64(index), "k", "v"));

        var deletedLength = new FileInfo(BinaryJournalTestSegmentWriter.SegmentPath(dir, 1)).Length + new FileInfo(BinaryJournalTestSegmentWriter.SegmentPath(dir, 2)).Length;
        var signal = new JournalCapacityResyncSignal();
        var worker = CreateWorker(dir, signal, out var passes);

        worker.ScheduleRetentionCleanup(SnapshotState(3, 3));
        await passes.WaitUntilAsync(static p => !p.IsEmpty, cancellationToken);

        _ = await Assert.That(signal.TryTake(out var segments, out var bytes)).IsTrue();
        _ = await Assert.That(segments).IsEqualTo(2);
        _ = await Assert.That(bytes).IsEqualTo(deletedLength);
        _ = await Assert.That(File.Exists(BinaryJournalTestSegmentWriter.SegmentPath(dir, 1))).IsFalse();
        _ = await Assert.That(File.Exists(BinaryJournalTestSegmentWriter.SegmentPath(dir, 2))).IsFalse();
    }

    /// <summary>A pass never deletes the replay floor segment, the active segment or a roll target above it, and records nothing when it deleted nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PassKeepsReplayFloorActiveAndTarget(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("retention-worker-keep");
        for (var index = 2; index <= 4; index++)
            BinaryJournalTestSegmentWriter.WriteJournalSegment(dir, index, BinaryJournalTestSegmentWriter.BuildPutRecord(Convert.ToUInt64(index), "k", "v"));

        var signal = new JournalCapacityResyncSignal();
        var worker = CreateWorker(dir, signal, out var passes);

        worker.ScheduleRetentionCleanup(SnapshotState(3, 2));
        await passes.WaitUntilAsync(static p => !p.IsEmpty, cancellationToken);

        _ = await Assert.That(signal.TryTake(out _, out _)).IsFalse();
        for (var index = 2; index <= 4; index++)
            _ = await Assert.That(File.Exists(BinaryJournalTestSegmentWriter.SegmentPath(dir, index))).IsTrue();
    }

    /// <summary>A manifest whose current journal lies below the replay floor deletes nothing: the layout is not trusted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PassKeepsAllBelowCurrentFloor(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("retention-worker-floor");
        for (var index = 1; index <= 2; index++)
            BinaryJournalTestSegmentWriter.WriteJournalSegment(dir, index, BinaryJournalTestSegmentWriter.BuildPutRecord(Convert.ToUInt64(index), "k", "v"));

        var signal = new JournalCapacityResyncSignal();
        var worker = CreateWorker(dir, signal, out var passes);

        worker.ScheduleRetentionCleanup(SnapshotState(1, 2));
        await passes.WaitUntilAsync(static p => !p.IsEmpty, cancellationToken);

        _ = await Assert.That(signal.TryTake(out _, out _)).IsFalse();
        _ = await Assert.That(File.Exists(BinaryJournalTestSegmentWriter.SegmentPath(dir, 1))).IsTrue();
    }

    private static State SnapshotState(int currentJournal, int replayFrom) => new()
    {
        CurrentJournal = currentJournal,
        NextSequence = 10,
        LastSnapshot = new SnapshotRef { Index = 1, CreatedUtc = DateTime.UtcNow, ReplayFromJournalSegment = replayFrom },
    };

    private static RetentionWorker CreateWorker(string dir, JournalCapacityResyncSignal signal, out ConcurrentQueue<bool> passes)
    {
        var outcomes = new ConcurrentQueue<bool>();
        var readiness = new IRetentionCleanupReadinessStatusCreateExpectations();
        _ = readiness.Setups.RecordWriteOutcome(Arg.Any<bool>()).Callback(outcomes.Enqueue);
        passes = outcomes;
        return new RetentionWorker(new RetentionContext(new RetentionSettings(dir), null, NullLogger.Instance, NoOpRetentionFailureMetrics.Instance), readiness.Instance(), signal);
    }

    /// <summary>Holds the first retention pass at its outcome report until the test releases it, and counts the passes.</summary>
    private sealed class FirstPassProbe : IDisposable
    {
        private readonly CancellationToken _cancellationToken;
        private readonly TaskCompletionSource _done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _ending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ManualResetEventSlim _release = new();
        private readonly TaskCompletionSource _second = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _passes;

        internal FirstPassProbe(CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;
        }

        public void Dispose() => _release.Dispose();

        internal void RecordPass()
        {
            if (Interlocked.Increment(ref _passes) != 1)
            {
                _ = _second.TrySetResult();
                return;
            }

            _ = _ending.TrySetResult();
            _release.Wait(_cancellationToken);
            _ = _done.TrySetResult();
        }

        internal async Task<bool> WaitForSecondPassAsync(TimeSpan window)
        {
            try
            {
                await _second.Task.WaitAsync(window, TimeProvider.System, _cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch (TimeoutException)
            {
                return false;
            }
        }

        internal Task WaitUntilEndingAsync() => _ending.Task.WaitAsync(_cancellationToken);

        internal Task ReleaseAsync()
        {
            _release.Set();
            return _done.Task.WaitAsync(_cancellationToken);
        }
    }
}
