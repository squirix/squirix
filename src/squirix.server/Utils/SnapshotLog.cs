using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic events for snapshot scheduling.</summary>
internal static class SnapshotLog
{
    internal static void SnapshotTriggerCrashed(ILogger logger, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.SnapshotTriggerCrashedEvent.Write(logger, LogLevel.Error, exception, ValueTuple.Create());
    }

    internal static void SnapshotFailedRetrying(ILogger logger, Exception exception, int consecutiveFailures, int delayMilliseconds)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.SnapshotFailedRetryingEvent.Write(logger, LogLevel.Error, exception, (consecutiveFailures, delayMilliseconds));
    }

    internal static void SnapshotTriggerJournalAppended(ILogger logger)
    {
        if (logger.IsEnabled(LogLevel.Trace))
            Events.SnapshotTriggerJournalAppendedEvent.Write(logger, LogLevel.Trace, null, ValueTuple.Create());
    }

    internal static void SnapshotTriggerStarted(ILogger logger, int intervalSeconds)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.SnapshotTriggerStartedEvent.Write(logger, LogLevel.Information, null, ValueTuple.Create(intervalSeconds));
    }

    internal static void SnapshotTriggerStopped(ILogger logger)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.SnapshotTriggerStoppedEvent.Write(logger, LogLevel.Information, null, ValueTuple.Create());
    }

    internal static void SnapshotTriggerTick(ILogger logger)
    {
        if (logger.IsEnabled(LogLevel.Trace))
            Events.SnapshotTriggerTickEvent.Write(logger, LogLevel.Trace, null, ValueTuple.Create());
    }

    private static class Events
    {
        internal static readonly LogEvent SnapshotTriggerCrashedEvent = new(3004, nameof(SnapshotLog.SnapshotTriggerCrashed), "SnapshotTriggerService crashed.");

        internal static readonly LogEvent SnapshotFailedRetryingEvent = new(3031, nameof(SnapshotLog.SnapshotFailedRetrying), "Snapshot failed ({ConsecutiveFailures} in a row), retrying in {DelayMilliseconds} ms; the journal keeps every write until a snapshot succeeds");

        internal static readonly LogEvent SnapshotTriggerJournalAppendedEvent = new(3006, nameof(SnapshotLog.SnapshotTriggerJournalAppended), "journal appended — triggering snapshot check (ops/bytes thresholds).");

        internal static readonly LogEvent SnapshotTriggerStartedEvent = new(3001, nameof(SnapshotLog.SnapshotTriggerStarted), "SnapshotTriggerService started. Periodic interval: {IntervalSeconds}s");

        internal static readonly LogEvent SnapshotTriggerStoppedEvent = new(3005, nameof(SnapshotLog.SnapshotTriggerStopped), "Snapshot trigger stopped; this node takes no further automatic snapshots until restart");

        internal static readonly LogEvent SnapshotTriggerTickEvent = new(3002, nameof(SnapshotLog.SnapshotTriggerTick), "Timer tick — triggering snapshot check.");
    }
}
