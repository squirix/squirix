using System;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Logging;

/// <summary>Snapshot trigger background-service diagnostics.</summary>
internal static partial class LogManager
{
    [LoggerMessage(EventId = 3003, Level = LogLevel.Debug, Message = "SnapshotTriggerService cancellation requested.")]
    internal static partial void SnapshotTriggerCanceled(ILogger logger);

    [LoggerMessage(EventId = 3004, Level = LogLevel.Error, Message = "SnapshotTriggerService crashed.")]
    internal static partial void SnapshotTriggerCrashed(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 3006, Level = LogLevel.Trace, Message = "journal appended — triggering snapshot check (ops/bytes thresholds).")]
    internal static partial void SnapshotTriggerJournalAppended(ILogger logger);

    [LoggerMessage(EventId = 3001, Level = LogLevel.Information, Message = "SnapshotTriggerService started. Periodic interval: {IntervalSeconds}s")]
    internal static partial void SnapshotTriggerStarted(ILogger logger, int intervalSeconds);

    [LoggerMessage(EventId = 3005, Level = LogLevel.Information, Message = "SnapshotTriggerService stopped and unsubscribed from journal metrics.")]
    internal static partial void SnapshotTriggerStopped(ILogger logger);

    [LoggerMessage(EventId = 3002, Level = LogLevel.Trace, Message = "Timer tick — triggering snapshot check.")]
    internal static partial void SnapshotTriggerTick(ILogger logger);
}
