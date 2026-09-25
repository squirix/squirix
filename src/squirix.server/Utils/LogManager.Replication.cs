using System;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Utils;

/// <summary>Replica group verification, committer and follower-log lifecycle logs.</summary>
internal static partial class LogManager
{
    [LoggerMessage(EventId = 4001, Level = LogLevel.Warning, Message = "Replica group verification cannot proceed: the leader log tail is not fully committed or its log is not ready")]
    internal static partial void ReplicaVerificationBlocked(ILogger logger);

    [LoggerMessage(EventId = 4002, Level = LogLevel.Information, Message = "Replica group verification is complete: every slot counts toward the write quorum")]
    internal static partial void ReplicaVerificationComplete(ILogger logger);

    [LoggerMessage(EventId = 4004, Level = LogLevel.Information, Message = "Replica group verification is pending: some followers are not yet verified against the leader log")]
    internal static partial void ReplicaVerificationPending(ILogger logger);

    [LoggerMessage(EventId = 4003, Level = LogLevel.Debug, Message = "Replica group verification attempt failed and will be retried")]
    internal static partial void ReplicaVerificationRetry(ILogger logger, Exception exception);

    [LoggerMessage(
        EventId = 4005,
        Level = LogLevel.Error,
        Message = "Replica group committer did not drain within the shutdown budget of {Budget}; the in-flight commit keeps its coordinator and gate, which are leaked")]
    internal static partial void ReplicaCommitterLeakedOnShutdownTimeout(ILogger logger, TimeSpan budget);

    [LoggerMessage(
        EventId = 4006,
        Level = LogLevel.Warning,
        Message = "Replica commit outcome is unknown after the local append; the cause is reported here and the caller gets COMMIT_OUTCOME_UNKNOWN")]
    internal static partial void ReplicaCommitOutcomeUnknown(ILogger logger, Exception exception);

    [LoggerMessage(
        EventId = 4007,
        Level = LogLevel.Warning,
        Message = "Replica group entries past their majority could not be applied to memory yet; new writes and resyncs are refused until they are")]
    internal static partial void ReplicaPendingApplyFailed(ILogger logger, Exception exception);

    [LoggerMessage(
        EventId = 4008,
        Level = LogLevel.Error,
        Message = "Replica commit coordinator did not drain within the shutdown budget of {Budget}; the in-flight commit keeps its gates and log-index sequencer, which are leaked")]
    internal static partial void ReplicaCoordinatorLeakedOnShutdown(ILogger logger, TimeSpan budget);

    [LoggerMessage(
        EventId = 4009,
        Level = LogLevel.Error,
        Message =
            "Replica group {GroupId} follower log did not drain within the shutdown budget of {Budget}; {FaultedInFlightWaiters} in-flight waiters were faulted and the log handle, its worker thread and the gate holder are leaked")]
    internal static partial void FollowerLogLeakedOnShutdownTimeout(ILogger logger, string groupId, TimeSpan budget, int faultedInFlightWaiters);
}
