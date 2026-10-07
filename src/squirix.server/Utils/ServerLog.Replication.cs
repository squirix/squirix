using System;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Utils;

/// <summary>Replica group verification, committer and follower-log lifecycle logs.</summary>
internal static partial class ServerLog
{
    [LoggerMessage(
        EventId = 4001,
        Level = LogLevel.Warning,
        Message = "Replica group verification cannot proceed: the leader log is not ready or its uncommitted tail has no entry of the current term")]
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

    [LoggerMessage(
        EventId = 4010,
        Level = LogLevel.Warning,
        Message =
            "Replica group leader tail through index {LastIndex} holds no entry of the current term {Term}; it is not committed by counting replicas, and writes stay refused until a current-term entry commits it")]
    internal static partial void ReplicaTailOfOlderTerm(ILogger logger, ulong lastIndex, ulong term);

    [LoggerMessage(
        EventId = 4011,
        Level = LogLevel.Warning,
        Message = "Replica group log maintenance failed and will be retried; the owned group log keeps its applied entries until it succeeds")]
    internal static partial void ReplicaLogMaintenanceRetry(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 4012, Level = LogLevel.Information, Message = "Replica group log compaction outcome changed to {Outcome}")]
    internal static partial void ReplicaLogCompactionChanged(ILogger logger, string outcome);

    [LoggerMessage(
        EventId = 4013,
        Level = LogLevel.Warning,
        Message = "Replica group log compaction is stalled: its snapshot would exceed the maximum snapshot size until idempotency outcomes age out")]
    internal static partial void ReplicaLogCompactionSnapshotTooLarge(ILogger logger);

    [LoggerMessage(
        EventId = 4014,
        Level = LogLevel.Error,
        Message =
            "Replica group {GroupId} refused an inconsistent log record and did not apply it; the entry stays pending, writes are refused, and the record needs operator attention")]
    internal static partial void ReplicaInconsistentRecord(ILogger logger, string groupId, Exception exception);

    [LoggerMessage(
        EventId = 4015,
        Level = LogLevel.Error,
        Message = "Replica group {GroupId} decided an inconsistent record at prepare; the write was refused and nothing was appended")]
    internal static partial void ReplicaInconsistentDecision(ILogger logger, string groupId, Exception exception);

    [LoggerMessage(
        EventId = 4016,
        Level = LogLevel.Information,
        Message = "Replica group {GroupId} rebuilt {Count} idempotency outcomes of committed log entries after the restart")]
    internal static partial void ReplicaOutcomesRestored(ILogger logger, string groupId, int count);

    [LoggerMessage(
        EventId = 4017,
        Level = LogLevel.Error,
        Message = "Replica commit coordinator work abandoned at shutdown failed after the shutdown budget expired")]
    internal static partial void ReplicaCoordinatorAbandonedWorkFaulted(ILogger logger, Exception exception);

    [LoggerMessage(
        EventId = 4018,
        Level = LogLevel.Error,
        Message = "Replica follower sender did not finish its in-flight request within the shutdown budget of {Budget}; the request ignored cancellation and is abandoned")]
    internal static partial void ReplicaFollowerSenderLeakedOnShutdown(ILogger logger, TimeSpan budget);

    [LoggerMessage(EventId = 4019, Level = LogLevel.Warning, Message = "Replica follower sender failed to close; the shutdown continues")]
    internal static partial void ReplicaFollowerSenderCloseFailed(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 4020, Level = LogLevel.Warning, Message = "Replica group durable publish of {Path} needed {Attempts} attempts; holders: {Holders}")]
    internal static partial void DurablePublishRetried(ILogger logger, string path, int attempts, string holders);

    [LoggerMessage(
        EventId = 4021,
        Level = LogLevel.Information,
        Message = "Replica group {GroupId} follower {NodeId} caught up through index {HeldThrough} with {Entries} entries and counts toward the write quorum again")]
    internal static partial void ReplicaFollowerCaughtUp(ILogger logger, string groupId, string nodeId, ulong heldThrough, int entries);

    [LoggerMessage(
        EventId = 4022,
        Level = LogLevel.Warning,
        Message =
            "Replica group {GroupId} follower {NodeId} needs entries the leader already compacted; it cannot be caught up from the log, and snapshot catch-up is not implemented yet, so it stays out of the write quorum")]
    internal static partial void ReplicaCatchUpCompacted(ILogger logger, string groupId, string nodeId);

    [LoggerMessage(
        EventId = 4023,
        Level = LogLevel.Warning,
        Message = "Replica group {GroupId} follower {NodeId} catch-up stopped: {Outcome}; the follower stays out of the write quorum")]
    internal static partial void ReplicaCatchUpStopped(ILogger logger, string groupId, string nodeId, string outcome);

    [LoggerMessage(
        EventId = 4024,
        Level = LogLevel.Error,
        Message = "Replica group {GroupId} leader log entry could not be read back intact for the catch-up of follower {NodeId}; the log needs operator attention")]
    internal static partial void ReplicaCatchUpCorrupt(ILogger logger, string groupId, string nodeId);

    [LoggerMessage(
        EventId = 4026,
        Level = LogLevel.Warning,
        Message = "Replica group {GroupId} follower {NodeId} did not acknowledge the append of index {LogIndex}; it stops counting toward the write quorum until it is caught up")]
    internal static partial void ReplicaFollowerDemoted(ILogger logger, string groupId, string nodeId, ulong logIndex);

    [LoggerMessage(EventId = 4025, Level = LogLevel.Debug, Message = "Replica group {GroupId} follower {NodeId} catch-up ended: {Outcome}; it is retried on the next verification")]
    internal static partial void ReplicaCatchUpInterrupted(ILogger logger, string groupId, string nodeId, string outcome);
}
