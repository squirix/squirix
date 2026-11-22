using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic events for snapshot scheduling.</summary>
internal static class SnapshotLog
{
    internal static void SnapshotFailedRetrying(IServerLogger logger, Exception exception, int consecutiveFailures, int delayMilliseconds)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.SnapshotFailedRetryingEvent.Write(logger, ServerLogLevel.Error, exception, (consecutiveFailures, delayMilliseconds));
    }

    internal static void SnapshotTriggerCrashed(IServerLogger logger, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.SnapshotTriggerCrashedEvent.Write(logger, ServerLogLevel.Error, exception, ValueTuple.Create());
    }

    internal static void SnapshotTriggerJournalAppended(IServerLogger logger)
    {
        if (logger.IsEnabled(ServerLogLevel.Trace))
            Events.SnapshotTriggerJournalAppendedEvent.Write(logger, ServerLogLevel.Trace, null, ValueTuple.Create());
    }

    internal static void SnapshotTriggerStarted(IServerLogger logger, int intervalSeconds)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.SnapshotTriggerStartedEvent.Write(logger, ServerLogLevel.Information, null, ValueTuple.Create(intervalSeconds));
    }

    internal static void SnapshotTriggerStopped(IServerLogger logger)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.SnapshotTriggerStoppedEvent.Write(logger, ServerLogLevel.Information, null, ValueTuple.Create());
    }

    internal static void SnapshotTriggerTick(IServerLogger logger)
    {
        if (logger.IsEnabled(ServerLogLevel.Trace))
            Events.SnapshotTriggerTickEvent.Write(logger, ServerLogLevel.Trace, null, ValueTuple.Create());
    }

    private static class Events
    {
        internal static readonly LogEvent SnapshotFailedRetryingEvent = new(3031, nameof(SnapshotFailedRetrying), "Snapshot failed ({ConsecutiveFailures} in a row), retrying in {DelayMilliseconds} ms; the journal keeps every write until a snapshot succeeds");
        internal static readonly LogEvent SnapshotTriggerCrashedEvent = new(3004, nameof(SnapshotTriggerCrashed), "SnapshotTriggerService crashed.");

        internal static readonly LogEvent SnapshotTriggerJournalAppendedEvent = new(3006, nameof(SnapshotTriggerJournalAppended), "journal appended — triggering snapshot check (ops/bytes thresholds).");

        internal static readonly LogEvent SnapshotTriggerStartedEvent = new(3001, nameof(SnapshotTriggerStarted), "SnapshotTriggerService started. Periodic interval: {IntervalSeconds}s");

        internal static readonly LogEvent SnapshotTriggerStoppedEvent = new(3005, nameof(SnapshotTriggerStopped), "Snapshot trigger stopped; this node takes no further automatic snapshots until restart");

        internal static readonly LogEvent SnapshotTriggerTickEvent = new(3002, nameof(SnapshotTriggerTick), "Timer tick — triggering snapshot check.");
    }
}
