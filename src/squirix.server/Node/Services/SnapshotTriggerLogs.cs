using System;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Node.Services;

internal static class SnapshotTriggerLogs
{
    private static readonly Action<ILogger, Exception?> Cancelled;

    private static readonly Action<ILogger, Exception?> Crashed;

    private static readonly Action<ILogger, Exception?> Disabled;

    private static readonly Action<ILogger, Exception?> SnapshotAttemptFailed;

    private static readonly Action<ILogger, int, Exception?> Started;

    private static readonly Action<ILogger, Exception?> Stopped;

    private static readonly Action<ILogger, Exception?> Tick;

    private static readonly Action<ILogger, Exception?> JournalAppended;

    static SnapshotTriggerLogs()
    {
        var cancelledId = new EventId(1003, nameof(Cancelled));
        Cancelled = LoggerMessage.Define(LogLevel.Debug, cancelledId, "SnapshotTriggerService cancellation requested.");

        var crashedId = new EventId(1004, nameof(Crashed));
        Crashed = LoggerMessage.Define(LogLevel.Error, crashedId, "SnapshotTriggerService crashed.");

        var disabledId = new EventId(1000, nameof(Disabled));
        Disabled = LoggerMessage.Define(LogLevel.Information, disabledId, "SnapshotTriggerService disabled via configuration; exiting.");

        var snapFailId = new EventId(1007, nameof(SnapshotAttemptFailed));
        SnapshotAttemptFailed = LoggerMessage.Define(LogLevel.Error, snapFailId, "Snapshot attempt failed after journal append.");

        var startedId = new EventId(1001, nameof(Started));
        Started = LoggerMessage.Define<int>(LogLevel.Information, startedId, "SnapshotTriggerService started. Periodic interval: {IntervalSeconds}s");

        var stoppedId = new EventId(1005, nameof(Stopped));
        Stopped = LoggerMessage.Define(LogLevel.Information, stoppedId, "SnapshotTriggerService stopped and unsubscribed from journal metrics.");

        var tickId = new EventId(1002, nameof(Tick));
        Tick = LoggerMessage.Define(LogLevel.Trace, tickId, "Timer tick — triggering snapshot check.");

        var journalAppendId = new EventId(1006, nameof(JournalAppended));
        JournalAppended = LoggerMessage.Define(LogLevel.Trace, journalAppendId, "journal appended — triggering snapshot check (ops/bytes thresholds).");
    }

    public static void LogCancelled(ILogger l) => Cancelled(l, null);

    public static void LogCrashed(ILogger l, Exception ex) => Crashed(l, ex);

    public static void LogDisabled(ILogger l) => Disabled(l, null);

    public static void LogStarted(ILogger l, int intervalSeconds) => Started(l, intervalSeconds, null);

    public static void LogStopped(ILogger l) => Stopped(l, null);

    public static void LogTick(ILogger l) => Tick(l, null);

    public static void LogJournalAppended(ILogger l) => JournalAppended(l, null);
}
