using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic events for node lifetime and journal shutdown.</summary>
internal static class DiagnosticsLog
{
    internal static void LifetimeNotificationFailed(ILogger logger, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.LifetimeNotificationFailedEvent.Write(logger, LogLevel.Error, exception, ValueTuple.Create());
    }

    internal static void ServerWorkerFailed(ILogger logger, string service, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Critical))
            Events.ServerWorkerFailedEvent.Write(logger, LogLevel.Critical, exception, ValueTuple.Create(service));
    }

    internal static void RecoveryReplayInterrupted(ILogger logger)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.RecoveryReplayInterruptedEvent.Write(logger, LogLevel.Debug, null, ValueTuple.Create());
    }

    internal static void RecoveryReplayStopTimedOut(ILogger logger)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.RecoveryReplayStopTimedOutEvent.Write(logger, LogLevel.Warning, null, ValueTuple.Create());
    }

    internal static void IdempotencySweepStopped(ILogger logger)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.IdempotencySweepStoppedEvent.Write(logger, LogLevel.Debug, null, ValueTuple.Create());
    }

    internal static void LocalExpirationSweepStopped(ILogger logger)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.LocalExpirationSweepStoppedEvent.Write(logger, LogLevel.Debug, null, ValueTuple.Create());
    }

    internal static void JournalMetricFileProbeFailed(ILogger logger, Exception exception, string file)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.JournalMetricFileProbeFailedEvent.Write(logger, LogLevel.Debug, exception, ValueTuple.Create(file));
    }

    internal static void JournalThreadExitOnCancel(ILogger logger)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.JournalThreadExitOnCancelEvent.Write(logger, LogLevel.Debug, null, ValueTuple.Create());
    }

    internal static void JournalHighResolutionTimerUnavailable(ILogger logger)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.HighResolutionTimerUnavailableEvent.Write(logger, LogLevel.Information, null, ValueTuple.Create());
    }

    internal static void JournalProducerQuiescenceTimedOut(ILogger logger)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.JournalProducerQuiescenceTimedOutEvent.Write(logger, LogLevel.Debug, null, ValueTuple.Create());
    }

    internal static void JournalShutdownMarkerTimedOut(ILogger logger)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.JournalShutdownMarkerTimedOutEvent.Write(logger, LogLevel.Debug, null, ValueTuple.Create());
    }

    internal static void JournalThreadJoinTimedOut(ILogger logger, int faultedInFlightWaiters, int queuedAcceptedAppends)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.JournalThreadJoinTimedOutEvent.Write(logger, LogLevel.Error, null, (faultedInFlightWaiters, queuedAcceptedAppends));
    }

    internal static void JournalThreadLeakedOnShutdownTimeout(ILogger logger, int faultedInFlightWaiters, int queuedAcceptedAppends)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.ThreadLeakedOnShutdownTimeoutEvent.Write(logger, LogLevel.Error, null, (faultedInFlightWaiters, queuedAcceptedAppends));
    }

    internal static void JournalInFlightApplyWaitTimedOut(ILogger logger)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.JournalInFlightApplyWaitTimedOutEvent.Write(logger, LogLevel.Error, null, ValueTuple.Create());
    }

    internal static void JournalDisposeFailedOnHostShutdown(ILogger logger, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.JournalDisposeFailedOnHostShutdownEvent.Write(logger, LogLevel.Error, exception, ValueTuple.Create());
    }

    internal static void HostStopFailedOnDispose(ILogger logger, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.HostStopFailedOnDisposeEvent.Write(logger, LogLevel.Error, exception, ValueTuple.Create());
    }

    internal static void HostDisposeFailedAfterStartFailure(ILogger logger, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.HostDisposeFailedAfterStartFailureEvent.Write(logger, LogLevel.Error, exception, ValueTuple.Create());
    }

    internal static void JournalStopFailedOnDispose(ILogger logger, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.JournalStopFailedOnDisposeEvent.Write(logger, LogLevel.Error, exception, ValueTuple.Create());
    }

    internal static void JournalFailureSurfacedOnStop(ILogger logger, bool latchedBeforeShutdown, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.JournalFailureSurfacedOnStopEvent.Write(logger, LogLevel.Error, exception, ValueTuple.Create(latchedBeforeShutdown));
    }

    internal static void JournalShutdownInducedFailureIgnored(ILogger logger, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.ShutdownInducedFailureIgnoredEvent.Write(logger, LogLevel.Debug, exception, ValueTuple.Create());
    }

    private static class Events
    {
        internal static readonly LogEvent LifetimeNotificationFailedEvent = new(3091, nameof(DiagnosticsLog.LifetimeNotificationFailed), "A server lifetime notification callback failed");

        internal static readonly LogEvent ServerWorkerFailedEvent = new(3090, nameof(DiagnosticsLog.ServerWorkerFailed), "Background service {Service} failed; stopping the node");

        internal static readonly LogEvent RecoveryReplayInterruptedEvent = new(3025, nameof(DiagnosticsLog.RecoveryReplayInterrupted), "Journal recovery replay stopped before it finished because the host is shutting down; the next start replays the journal again");

        internal static readonly LogEvent RecoveryReplayStopTimedOutEvent = new(3029, nameof(DiagnosticsLog.RecoveryReplayStopTimedOut), "Host stop budget expired while journal recovery replay was still running; shutdown continues without waiting for it");

        internal static readonly LogEvent IdempotencySweepStoppedEvent = new(3026, nameof(DiagnosticsLog.IdempotencySweepStopped), "Idempotency store expiry sweep stopped because the host is shutting down");

        internal static readonly LogEvent LocalExpirationSweepStoppedEvent = new(3030, nameof(DiagnosticsLog.LocalExpirationSweepStopped), "Local expiration sweep stopped because the host is shutting down");

        internal static readonly LogEvent JournalMetricFileProbeFailedEvent = new(3027, nameof(DiagnosticsLog.JournalMetricFileProbeFailed), "Journal segment metric probe failed for {File}");

        internal static readonly LogEvent JournalThreadExitOnCancelEvent = new(3024, nameof(DiagnosticsLog.JournalThreadExitOnCancel), "Journal I/O thread exited on background cancellation");

        internal static readonly LogEvent HighResolutionTimerUnavailableEvent = new(3028, nameof(DiagnosticsLog.JournalHighResolutionTimerUnavailable), "Journal group commit batch deadline uses the system clock tick: the high-resolution timer is unavailable on this Windows version, so a short MaxWait may be rounded up to about 15.6 ms");

        internal static readonly LogEvent JournalProducerQuiescenceTimedOutEvent = new(3011, nameof(DiagnosticsLog.JournalProducerQuiescenceTimedOut), "Journal producers did not quiesce within the shutdown budget");

        internal static readonly LogEvent JournalShutdownMarkerTimedOutEvent = new(3012, nameof(DiagnosticsLog.JournalShutdownMarkerTimedOut), "Shutdown marker did not enter the journal ring within the shutdown budget");

        internal static readonly LogEvent JournalThreadJoinTimedOutEvent = new(3013, nameof(DiagnosticsLog.JournalThreadJoinTimedOut), "Journal I/O thread join timed out during stop; {FaultedInFlightWaiters} in-flight durability waiters faulted; {QueuedAcceptedAppends} accepted appends still queued");

        internal static readonly LogEvent ThreadLeakedOnShutdownTimeoutEvent = new(3014, nameof(DiagnosticsLog.JournalThreadLeakedOnShutdownTimeout), "Journal I/O thread still alive after the stop deadline; writer, ring, and gates stay open until a later stop; {FaultedInFlightWaiters} in-flight durability waiters faulted; {QueuedAcceptedAppends} accepted appends still queued");

        internal static readonly LogEvent JournalInFlightApplyWaitTimedOutEvent = new(3015, nameof(DiagnosticsLog.JournalInFlightApplyWaitTimedOut), "In-flight memory applies did not finish within the shutdown budget; their callers may fail although their journal frames may be durable");

        internal static readonly LogEvent JournalDisposeFailedOnHostShutdownEvent = new(3016, nameof(DiagnosticsLog.JournalDisposeFailedOnHostShutdown), "Journal dispose failed during host shutdown; host disposal continues with the manifest ledger and the remaining services");

        internal static readonly LogEvent HostStopFailedOnDisposeEvent = new(3017, nameof(DiagnosticsLog.HostStopFailedOnDispose), "Host stop failed during server disposal; the host is disposed regardless");

        internal static readonly LogEvent HostDisposeFailedAfterStartFailureEvent = new(3018, nameof(DiagnosticsLog.HostDisposeFailedAfterStartFailure), "Releasing the server application after a failed startup failed; the startup failure is reported instead");

        internal static readonly LogEvent JournalStopFailedOnDisposeEvent = new(3019, nameof(DiagnosticsLog.JournalStopFailedOnDispose), "Journal stop failed during dispose; acknowledged frames may not be durable, or the journal thread may remain open");

        internal static readonly LogEvent JournalFailureSurfacedOnStopEvent = new(3020, nameof(DiagnosticsLog.JournalFailureSurfacedOnStop), "Journal I/O failure surfaced while stopping; frames acknowledged before it may not be durable (latched before shutdown: {LatchedBeforeShutdown})");

        internal static readonly LogEvent ShutdownInducedFailureIgnoredEvent = new(3021, nameof(DiagnosticsLog.JournalShutdownInducedFailureIgnored), "Journal pipeline was failed by the shutdown itself (a refused maintenance step); not a data failure");
    }
}
