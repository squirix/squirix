using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>The journal event loop reports a missing high-resolution batch timer once, and only when group commit is on.</summary>
[Immutable]
public sealed class JournalDeadlineWaitLogTests : IsolatedStorageTestBase
{
    private const int TimerUnavailableEventId = 3028;

    /// <summary>With group commit on and the timer forced off, the line is logged once on Windows.</summary>
    [Test]
    public async Task FallbackLineIsLoggedOnce()
    {
        if (!OperatingSystem.IsWindows())
            return;

        var logger = new EventRecordingLogger();
        using var setup = CreateLoop(logger, false, true);

        setup.EventLoop.ReportDeadlineWaitFallbackOnce();
        setup.EventLoop.ReportDeadlineWaitFallbackOnce();

        _ = await Assert.That(logger.Count(TimerUnavailableEventId)).IsEqualTo(1);
        _ = await Assert.That(logger.Find(TimerUnavailableEventId)?.Level).IsEqualTo(LogLevel.Information);
    }

    /// <summary>The journal thread loop reports the fallback after its first idle wait, once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RunLoopReportsFallbackAfterIdleWait(CancellationToken cancellationToken)
    {
        if (!OperatingSystem.IsWindows())
            return;

        var logger = new EventRecordingLogger();
        using var setup = CreateLoop(logger, false, true);
        var loop = Task.Factory.StartNew(setup.EventLoop.Run, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        // A notification adds no ring item: it ends the idle wait (even if it lands before the loop parks), and the wake is followed
        // by the report. Shutdown is queued only after the report, so the loop cannot exit before its first wait.
        setup.Ring.NotifyWorkAvailable();
        var deadline = Stopwatch.GetTimestamp();
        while (logger.Count(TimerUnavailableEventId) == 0 && Stopwatch.GetElapsedTime(deadline) < TimeSpan.FromSeconds(10))
            await Task.Delay(10, cancellationToken);

        await setup.Ring.EnqueueAsync(JournalWorkItem.Shutdown(), cancellationToken);
        await loop.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);

        _ = await Assert.That(logger.Count(TimerUnavailableEventId)).IsEqualTo(1);
    }

    /// <summary>Without group commit there is no batch deadline, so nothing is logged.</summary>
    [Test]
    public async Task NoLineWithoutGroupCommit()
    {
        var logger = new EventRecordingLogger();
        using var setup = CreateLoop(logger, false, false);

        setup.EventLoop.ReportDeadlineWaitFallbackOnce();

        _ = await Assert.That(logger.Count(TimerUnavailableEventId)).IsEqualTo(0);
    }

    /// <summary>An available high-resolution timer is not reported.</summary>
    [Test]
    public async Task NoLineWhenTimerAllowed()
    {
        var logger = new EventRecordingLogger();
        using var setup = CreateLoop(logger, true, true);

        setup.EventLoop.ReportDeadlineWaitFallbackOnce();

        _ = await Assert.That(logger.Count(TimerUnavailableEventId)).IsEqualTo(0);
    }

    private LoopSetup CreateLoop(EventRecordingLogger logger, bool useHighResolutionTimer, bool groupCommit)
    {
        var options = new PersistenceOptions { DataDir = Dir, JournalGroupCommitMaxWait = TimeSpan.FromHours(1), JournalGroupCommitMaxBatch = 64 };
        var ring = new BoundedJournalRing(4, useHighResolutionTimer);
        var segmentWriter = new FlushSegmentWriter();
        var host = new FakeEventLoopHost(new PendingAppendRegistry());
        var eventLoop = new JournalEventLoop(host, ring, segmentWriter, options, new JournalEventLoopStartup(1, 1024L * 1024L, 1, JournalSegmentProbe.Probe(Dir, 1)), logger, CancellationToken.None);
        if (groupCommit)
            eventLoop.AttachGroupCommit(new JournalDurabilityGroupCommit(static () => { }, static () => { }, options));

        return new LoopSetup(eventLoop, ring, segmentWriter);
    }

    private sealed class LoopSetup : IDisposable
    {
        private readonly FlushSegmentWriter _segmentWriter;

        internal LoopSetup(JournalEventLoop eventLoop, BoundedJournalRing ring, FlushSegmentWriter segmentWriter)
        {
            EventLoop = eventLoop;
            Ring = ring;
            _segmentWriter = segmentWriter;
        }

        internal JournalEventLoop EventLoop { get; }

        internal BoundedJournalRing Ring { get; }

        public void Dispose()
        {
            Ring.Dispose();
            _segmentWriter.Dispose();
        }
    }
}
