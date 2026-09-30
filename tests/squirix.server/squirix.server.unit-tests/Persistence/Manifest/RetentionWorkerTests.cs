using System;
using System.Collections.Concurrent;
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
        var context = new RetentionContext(new RetentionSettings("..", 1, 1, "man-*.bmqx"), null, NullLogger.Instance, static _ => 1, metrics.Instance());
        var worker = new RetentionWorker(context, readiness.Instance());

        worker.ScheduleRetentionCleanup(new State { CurrentJournal = 2 });

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
        _ = readiness.Setups.RecordWriteOutcome(Arg.Any<bool>()).Callback(_ => probe.RecordPass());
        var context = new RetentionContext(new RetentionSettings(dir, 1, 1, "man-*.bmqx"), null, NullLogger.Instance, static _ => 1);
        var worker = new RetentionWorker(context, readiness.Instance());

        worker.ScheduleRetentionCleanup(new State { CurrentJournal = 1 });
        await probe.WaitUntilEndingAsync();
        worker.ScheduleRetentionCleanup(new State { CurrentJournal = 2 });
        worker.Stop();
        worker.ScheduleRetentionCleanup(new State { CurrentJournal = 3 });
        await probe.ReleaseAsync();

        // A further pass on the empty directory would report within milliseconds; the window only bounds how long the test waits for it.
        _ = await Assert.That(await probe.WaitForSecondPassAsync(TimeSpan.FromMilliseconds(500))).IsFalse();
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
