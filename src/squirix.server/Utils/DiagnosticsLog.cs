using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic events for node lifetime and journal shutdown.</summary>
internal static class DiagnosticsLog
{
    internal static void LifetimeNotificationFailed(IServerLogger logger, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.LifetimeNotificationFailedEvent.Write(logger, ServerLogLevel.Error, exception, ValueTuple.Create());
    }

    internal static void ServerWorkerFailed(IServerLogger logger, string service, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Critical))
            Events.ServerWorkerFailedEvent.Write(logger, ServerLogLevel.Critical, exception, ValueTuple.Create(service));
    }

    internal static void RecoveryReplayInterrupted(IServerLogger logger)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.RecoveryReplayInterruptedEvent.Write(logger, ServerLogLevel.Debug, null, ValueTuple.Create());
    }

    internal static void RecoveryReplayStopTimedOut(IServerLogger logger)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.RecoveryReplayStopTimedOutEvent.Write(logger, ServerLogLevel.Warning, null, ValueTuple.Create());
    }

    internal static void IdempotencySweepStopped(IServerLogger logger)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.IdempotencySweepStoppedEvent.Write(logger, ServerLogLevel.Debug, null, ValueTuple.Create());
    }

    internal static void LocalExpirationSweepStopped(IServerLogger logger)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.LocalExpirationSweepStoppedEvent.Write(logger, ServerLogLevel.Debug, null, ValueTuple.Create());
    }

    internal static void JournalMetricFileProbeFailed(IServerLogger logger, Exception exception, string file)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.JournalMetricFileProbeFailedEvent.Write(logger, ServerLogLevel.Debug, exception, ValueTuple.Create(file));
    }

    internal static void JournalThreadExitOnCancel(IServerLogger logger)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.JournalThreadExitOnCancelEvent.Write(logger, ServerLogLevel.Debug, null, ValueTuple.Create());
    }

    internal static void JournalHighResolutionTimerUnavailable(IServerLogger logger)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.HighResolutionTimerUnavailableEvent.Write(logger, ServerLogLevel.Information, null, ValueTuple.Create());
    }

    internal static void JournalProducerQuiescenceTimedOut(IServerLogger logger)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.JournalProducerQuiescenceTimedOutEvent.Write(logger, ServerLogLevel.Debug, null, ValueTuple.Create());
    }

    internal static void JournalShutdownMarkerTimedOut(IServerLogger logger)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.JournalShutdownMarkerTimedOutEvent.Write(logger, ServerLogLevel.Debug, null, ValueTuple.Create());
    }

    internal static void JournalThreadJoinTimedOut(IServerLogger logger, int faultedInFlightWaiters, int queuedAcceptedAppends)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.JournalThreadJoinTimedOutEvent.Write(logger, ServerLogLevel.Error, null, (faultedInFlightWaiters, queuedAcceptedAppends));
    }

    internal static void JournalThreadLeakedOnShutdownTimeout(IServerLogger logger, int faultedInFlightWaiters, int queuedAcceptedAppends)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.ThreadLeakedOnShutdownTimeoutEvent.Write(logger, ServerLogLevel.Error, null, (faultedInFlightWaiters, queuedAcceptedAppends));
    }

    internal static void JournalInFlightApplyWaitTimedOut(IServerLogger logger)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.JournalInFlightApplyWaitTimedOutEvent.Write(logger, ServerLogLevel.Error, null, ValueTuple.Create());
    }

    internal static void JournalDisposeFailedOnHostShutdown(IServerLogger logger, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.JournalDisposeFailedOnHostShutdownEvent.Write(logger, ServerLogLevel.Error, exception, ValueTuple.Create());
    }

    internal static void HostStopFailedOnDispose(IServerLogger logger, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.HostStopFailedOnDisposeEvent.Write(logger, ServerLogLevel.Error, exception, ValueTuple.Create());
    }

    internal static void HostDisposeFailedAfterStartFailure(IServerLogger logger, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.HostDisposeFailedAfterStartFailureEvent.Write(logger, ServerLogLevel.Error, exception, ValueTuple.Create());
    }

    internal static void JournalStopFailedOnDispose(IServerLogger logger, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.JournalStopFailedOnDisposeEvent.Write(logger, ServerLogLevel.Error, exception, ValueTuple.Create());
    }

    internal static void JournalFailureSurfacedOnStop(IServerLogger logger, bool latchedBeforeShutdown, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.JournalFailureSurfacedOnStopEvent.Write(logger, ServerLogLevel.Error, exception, ValueTuple.Create(latchedBeforeShutdown));
    }

    internal static void JournalShutdownInducedFailureIgnored(IServerLogger logger, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.ShutdownInducedFailureIgnoredEvent.Write(logger, ServerLogLevel.Debug, exception, ValueTuple.Create());
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
