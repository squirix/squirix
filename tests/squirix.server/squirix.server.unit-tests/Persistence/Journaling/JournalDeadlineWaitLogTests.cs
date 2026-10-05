using System;
using System.Collections.Concurrent;
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

        var logger = new RecordingLogger();
        using var setup = CreateLoop(logger, false, true);

        setup.EventLoop.ReportDeadlineWaitFallbackOnce();
        setup.EventLoop.ReportDeadlineWaitFallbackOnce();

        _ = await Assert.That(logger.Count(TimerUnavailableEventId)).IsEqualTo(1);
        _ = await Assert.That(logger.LevelOf(TimerUnavailableEventId)).IsEqualTo(LogLevel.Information);
    }

    /// <summary>Without group commit there is no batch deadline, so nothing is logged.</summary>
    [Test]
    public async Task NoLineWithoutGroupCommit()
    {
        var logger = new RecordingLogger();
        using var setup = CreateLoop(logger, false, false);

        setup.EventLoop.ReportDeadlineWaitFallbackOnce();

        _ = await Assert.That(logger.Count(TimerUnavailableEventId)).IsEqualTo(0);
    }

    /// <summary>An available high-resolution timer is not reported.</summary>
    [Test]
    public async Task NoLineWhenTimerAllowed()
    {
        var logger = new RecordingLogger();
        using var setup = CreateLoop(logger, true, true);

        setup.EventLoop.ReportDeadlineWaitFallbackOnce();

        _ = await Assert.That(logger.Count(TimerUnavailableEventId)).IsEqualTo(0);
    }

    private LoopSetup CreateLoop(RecordingLogger logger, bool useHighResolutionTimer, bool groupCommit)
    {
        var options = new PersistenceOptions { DataDir = Dir, JournalGroupCommitMaxWait = TimeSpan.FromHours(1), JournalGroupCommitMaxBatch = 64 };
        var ring = new BoundedJournalRing(4, useHighResolutionTimer);
        var segmentWriter = new NoOpSegmentWriter();
        var host = new FakeEventLoopHost(new PendingAppendRegistry());
        var eventLoop = new JournalEventLoop(host, ring, segmentWriter, options, new JournalEventLoopStartup(1, 1024L * 1024L, 1, JournalSegmentProbe.Probe(Dir, 1)), new FixedLogger(logger), CancellationToken.None);
        if (groupCommit)
            eventLoop.AttachGroupCommit(new JournalDurabilityGroupCommit(static () => { }, static () => { }, options));

        return new LoopSetup(eventLoop, ring, segmentWriter);
    }

    private sealed class FixedLogger : ILogger<JournalEventLoop>
    {
        private readonly RecordingLogger _inner;

        internal FixedLogger(RecordingLogger inner)
        {
            _inner = inner;
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            _inner.Record(logLevel, eventId);
    }

    private sealed class LoopSetup : IDisposable
    {
        private readonly BoundedJournalRing _ring;
        private readonly NoOpSegmentWriter _segmentWriter;

        internal LoopSetup(JournalEventLoop eventLoop, BoundedJournalRing ring, NoOpSegmentWriter segmentWriter)
        {
            EventLoop = eventLoop;
            _ring = ring;
            _segmentWriter = segmentWriter;
        }

        internal JournalEventLoop EventLoop { get; }

        public void Dispose()
        {
            _ring.Dispose();
            _segmentWriter.Dispose();
        }
    }

    private sealed class NoOpSegmentWriter : IJournalSegmentWriter
    {
        long IJournalSegmentWriter.Length => 0L;

        public void Dispose()
        {
        }

        void IJournalSegmentWriter.FlushToDisk()
        {
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

    private sealed class RecordingLogger
    {
        private readonly ConcurrentQueue<(LogLevel Level, int EventId)> _entries = new();

        internal int Count(int eventId)
        {
            var count = 0;
            foreach (var entry in _entries)
            {
                if (entry.EventId == eventId)
                    count++;
            }

            return count;
        }

        internal LogLevel? LevelOf(int eventId)
        {
            foreach (var entry in _entries)
            {
                if (entry.EventId == eventId)
                    return entry.Level;
            }

            return null;
        }

        internal void Record(LogLevel level, EventId eventId) => _entries.Enqueue((level, eventId.Id));
    }
}
