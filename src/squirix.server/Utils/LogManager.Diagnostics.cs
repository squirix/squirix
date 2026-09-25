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

    [LoggerMessage(EventId = 3005, Level = LogLevel.Debug, Message = "Journal durability join wait canceled during dispose")]
    internal static partial void DurabilityJoinWaitCanceledOnDispose(ILogger logger);

    [LoggerMessage(EventId = 3006, Level = LogLevel.Debug, Message = "Journal I/O thread exited on background cancellation")]
    internal static partial void JournalThreadExitOnCancel(ILogger logger);

    [LoggerMessage(EventId = 3007, Level = LogLevel.Debug, Message = "Symlink probe failed for {Path}; falling back to attributes")]
    internal static partial void SymlinkProbeFallback(ILogger logger, Exception exception, string path);

    [LoggerMessage(EventId = 3008, Level = LogLevel.Debug, Message = "Failed to clear read-only attribute for {File} during best-effort deletion")]
    internal static partial void ReadOnlyAttributeClearFailed(ILogger logger, Exception exception, string file);

    [LoggerMessage(EventId = 3009, Level = LogLevel.Debug, Message = "Journal background cancellation token was already disposed during coordinator dispose")]
    internal static partial void JournalBackgroundCancellationDisposedOnDispose(ILogger logger);

    [LoggerMessage(EventId = 3010, Level = LogLevel.Debug, Message = "Journal compaction background loop canceled")]
    internal static partial void CompactionLoopCanceled(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 3011, Level = LogLevel.Debug, Message = "Journal producers did not quiesce within the shutdown budget")]
    internal static partial void JournalProducerQuiescenceTimedOut(ILogger logger);

    [LoggerMessage(EventId = 3012, Level = LogLevel.Debug, Message = "Shutdown marker did not enter the journal ring within the shutdown budget")]
    internal static partial void JournalShutdownMarkerTimedOut(ILogger logger);

    [LoggerMessage(EventId = 3013, Level = LogLevel.Error, Message = "Journal I/O thread join timed out during dispose; {FaultedInFlightWaiters} in-flight durability waiters faulted")]
    internal static partial void JournalThreadJoinTimedOut(ILogger logger, int faultedInFlightWaiters);

    [LoggerMessage(EventId = 3014, Level = LogLevel.Error, Message = "Journal I/O thread still alive after shutdown; writer, ring, and gates are leaked; {FaultedInFlightWaiters} in-flight durability waiters faulted")]
    internal static partial void JournalThreadLeakedOnShutdownTimeout(ILogger logger, int faultedInFlightWaiters);

    [LoggerMessage(EventId = 3015, Level = LogLevel.Error, Message = "In-flight memory applies did not finish within the shutdown budget; their callers may fail although their journal frames may be durable")]
    internal static partial void JournalInFlightApplyWaitTimedOut(ILogger logger);

    [LoggerMessage(EventId = 3016, Level = LogLevel.Error, Message = "Journal dispose failed during host shutdown; host disposal continues with the manifest ledger and the remaining services")]
    internal static partial void JournalDisposeFailedOnHostShutdown(ILogger logger, Exception exception);
}
