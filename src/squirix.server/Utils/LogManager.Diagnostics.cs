using System;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic logs for best-effort background, dispose, and metrics paths.</summary>
internal static partial class LogManager
{
    [LoggerMessage(EventId = 3001, Level = LogLevel.Debug, Message = "Journal recovery replay interrupted (host shutdown)")]
    internal static partial void RecoveryReplayInterrupted(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 3002, Level = LogLevel.Debug, Message = "Idempotency store background sweep canceled")]
    internal static partial void IdempotencySweepCanceled(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 3003, Level = LogLevel.Debug, Message = "Journal segment metric probe failed for {File}")]
    internal static partial void JournalMetricFileProbeFailed(ILogger logger, Exception exception, string file);

    [LoggerMessage(EventId = 3004, Level = LogLevel.Debug, Message = "Backpressure observer probe failed; skipping source")]
    internal static partial void BackpressureObservationFailed(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 3006, Level = LogLevel.Debug, Message = "Journal I/O thread exited on background cancellation")]
    internal static partial void JournalThreadExitOnCancel(ILogger logger);

    [LoggerMessage(EventId = 3007, Level = LogLevel.Debug, Message = "Symlink probe failed for {Path}; falling back to attributes")]
    internal static partial void SymlinkProbeFallback(ILogger logger, Exception exception, string path);

    [LoggerMessage(EventId = 3008, Level = LogLevel.Debug, Message = "Failed to clear read-only attribute for {File} during best-effort deletion")]
    internal static partial void ReadOnlyAttributeClearFailed(ILogger logger, Exception exception, string file);

    [LoggerMessage(EventId = 3010, Level = LogLevel.Debug, Message = "Journal compaction background loop canceled")]
    internal static partial void CompactionLoopCanceled(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 3011, Level = LogLevel.Debug, Message = "Journal producers did not quiesce within the shutdown budget")]
    internal static partial void JournalProducerQuiescenceTimedOut(ILogger logger);

    [LoggerMessage(EventId = 3012, Level = LogLevel.Debug, Message = "Shutdown marker did not enter the journal ring within the shutdown budget")]
    internal static partial void JournalShutdownMarkerTimedOut(ILogger logger);

    [LoggerMessage(EventId = 3013, Level = LogLevel.Error, Message = "Journal I/O thread join timed out during stop; {FaultedInFlightWaiters} in-flight durability waiters faulted")]
    internal static partial void JournalThreadJoinTimedOut(ILogger logger, int faultedInFlightWaiters);

    [LoggerMessage(EventId = 3014, Level = LogLevel.Error, Message = "Journal I/O thread still alive after the stop deadline; writer, ring, and gates stay open until a later stop; {FaultedInFlightWaiters} in-flight durability waiters faulted")]
    internal static partial void JournalThreadLeakedOnShutdownTimeout(ILogger logger, int faultedInFlightWaiters);

    [LoggerMessage(EventId = 3015, Level = LogLevel.Error, Message = "In-flight memory applies did not finish within the shutdown budget; their callers may fail although their journal frames may be durable")]
    internal static partial void JournalInFlightApplyWaitTimedOut(ILogger logger);

    [LoggerMessage(EventId = 3016, Level = LogLevel.Error, Message = "Journal dispose failed during host shutdown; host disposal continues with the manifest ledger and the remaining services")]
    internal static partial void JournalDisposeFailedOnHostShutdown(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 3017, Level = LogLevel.Error, Message = "Host stop failed during server disposal; the host is disposed regardless")]
    internal static partial void HostStopFailedOnDispose(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 3018, Level = LogLevel.Error, Message = "Releasing the server application after a failed startup failed; the startup failure is reported instead")]
    internal static partial void HostDisposeFailedAfterStartFailure(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 3019, Level = LogLevel.Error, Message = "Journal stop failed during dispose; the journal thread or its resources may remain open")]
    internal static partial void JournalStopFailedOnDispose(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 3020, Level = LogLevel.Error, Message = "Journal I/O failure surfaced while stopping; frames acknowledged before it may not be durable (latched before shutdown: {LatchedBeforeShutdown})")]
    internal static partial void JournalFailureSurfacedOnStop(ILogger logger, bool latchedBeforeShutdown, Exception exception);

    [LoggerMessage(EventId = 3021, Level = LogLevel.Debug, Message = "Journal pipeline was failed by the shutdown itself (a refused maintenance step); not a data failure")]
    internal static partial void JournalShutdownInducedFailureIgnored(ILogger logger, Exception exception);
}
