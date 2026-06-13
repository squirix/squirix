using System;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Node.Services;

internal static class SnapshotTriggerLogs
{
    private static readonly Action<ILogger, Exception?> Cancelled = LoggerMessage.Define(
        LogLevel.Debug,
        new EventId(1003, nameof(Cancelled)),
        "SnapshotTriggerService cancellation requested.");

    private static readonly Action<ILogger, Exception?> Crashed = LoggerMessage.Define(LogLevel.Error, new EventId(1004, nameof(Crashed)), "SnapshotTriggerService crashed.");

    private static readonly Action<ILogger, Exception?> Disabled = LoggerMessage.Define(
        LogLevel.Information,
        new EventId(1000, nameof(Disabled)),
        "SnapshotTriggerService disabled via configuration; exiting.");

    private static readonly Action<ILogger, Exception?> JournalAppended = LoggerMessage.Define(
        LogLevel.Trace,
        new EventId(1006, nameof(JournalAppended)),
        "journal appended — triggering snapshot check (ops/bytes thresholds).");

    private static readonly Action<ILogger, int, Exception?> Started = LoggerMessage.Define<int>(
        LogLevel.Information,
        new EventId(1001, nameof(Started)),
        "SnapshotTriggerService started. Periodic interval: {IntervalSeconds}s");

    private static readonly Action<ILogger, Exception?> Stopped = LoggerMessage.Define(
        LogLevel.Information,
        new EventId(1005, nameof(Stopped)),
        "SnapshotTriggerService stopped and unsubscribed from journal metrics.");

    private static readonly Action<ILogger, Exception?> Tick = LoggerMessage.Define(LogLevel.Trace, new EventId(1002, nameof(Tick)), "Timer tick — triggering snapshot check.");

    public static void LogCancelled(ILogger l) => Cancelled(l, null);

    public static void LogCrashed(ILogger l, Exception ex) => Crashed(l, ex);

    public static void LogDisabled(ILogger l) => Disabled(l, null);

    public static void LogJournalAppended(ILogger l) => JournalAppended(l, null);

    public static void LogStarted(ILogger l, int intervalSeconds) => Started(l, intervalSeconds, null);

    public static void LogStopped(ILogger l) => Stopped(l, null);

    public static void LogTick(ILogger l) => Tick(l, null);
}
