using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic events for follower replication and catch-up.</summary>
internal static class ReplicaFollowerLog
{
    internal static void FollowerLogLeakedOnShutdownTimeout(IServerLogger logger, string groupId, TimeSpan budget, int faultedInFlightWaiters)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.FollowerLogLeakedOnShutdownTimeoutEvent.Write(logger, ServerLogLevel.Error, null, (groupId, budget, faultedInFlightWaiters));
    }

    internal static void ReplicaFollowerSenderLeakedOnShutdown(IServerLogger logger, TimeSpan budget)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.FollowerSenderLeakedOnShutdownEvent.Write(logger, ServerLogLevel.Error, null, ValueTuple.Create(budget));
    }

    internal static void ReplicaFollowerSenderCloseFailed(IServerLogger logger, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaFollowerSenderCloseFailedEvent.Write(logger, ServerLogLevel.Warning, exception, ValueTuple.Create());
    }

    internal static void ReplicaFollowerCaughtUp(IServerLogger logger, string groupId, string nodeId, ulong heldThrough, int entries)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ReplicaFollowerCaughtUpEvent.Write(logger, ServerLogLevel.Information, null, (groupId, nodeId, heldThrough, entries));
    }

    internal static void ReplicaCatchUpCompacted(IServerLogger logger, string groupId, string nodeId)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaCatchUpCompactedEvent.Write(logger, ServerLogLevel.Warning, null, (groupId, nodeId));
    }

    internal static void ReplicaCatchUpStopped(IServerLogger logger, string groupId, string nodeId, string outcome)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaCatchUpStoppedEvent.Write(logger, ServerLogLevel.Warning, null, (groupId, nodeId, outcome));
    }

    internal static void ReplicaCatchUpCorrupt(IServerLogger logger, string groupId, string nodeId)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.ReplicaCatchUpCorruptEvent.Write(logger, ServerLogLevel.Error, null, (groupId, nodeId));
    }

    internal static void ReplicaFollowerDemoted(IServerLogger logger, string groupId, string nodeId, ulong logIndex)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaFollowerDemotedEvent.Write(logger, ServerLogLevel.Warning, null, (groupId, nodeId, logIndex));
    }

    internal static void ReplicaCatchUpInterrupted(IServerLogger logger, string groupId, string nodeId, string outcome)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.ReplicaCatchUpInterruptedEvent.Write(logger, ServerLogLevel.Debug, null, (groupId, nodeId, outcome));
    }

    internal static void ReplicaFollowerApplyStopped(IServerLogger logger, string groupId, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.ReplicaFollowerApplyStoppedEvent.Write(logger, ServerLogLevel.Error, exception, ValueTuple.Create(groupId));
    }

    internal static void ReplicaFollowerApplyRetry(IServerLogger logger, string groupId, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaFollowerApplyRetryEvent.Write(logger, ServerLogLevel.Warning, exception, ValueTuple.Create(groupId));
    }

    internal static void ReplicaFollowerApplyNoLog(IServerLogger logger, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaFollowerApplyNoLogEvent.Write(logger, ServerLogLevel.Warning, null, ValueTuple.Create(groupId));
    }

    internal static void FollowerLogAppendUnconfirmed(IServerLogger logger, Exception exception, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.FollowerLogAppendUnconfirmedEvent.Write(logger, ServerLogLevel.Error, exception, ValueTuple.Create(groupId));
    }

    internal static void FollowerLogTruncateUnconfirmed(IServerLogger logger, Exception exception, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.FollowerLogTruncateUnconfirmedEvent.Write(logger, ServerLogLevel.Error, exception, ValueTuple.Create(groupId));
    }

    internal static void FollowerLogMirrorFailed(IServerLogger logger, Exception exception, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.FollowerLogMirrorFailedEvent.Write(logger, ServerLogLevel.Error, exception, ValueTuple.Create(groupId));
    }

    internal static void FollowerLogMirrorFailedOnDispose(IServerLogger logger, Exception exception, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.FollowerLogMirrorFailedOnDisposeEvent.Write(logger, ServerLogLevel.Warning, exception, ValueTuple.Create(groupId));
    }

    internal static void FollowerLogOpenedNotComplete(IServerLogger logger, string groupId, string memberState)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.FollowerLogOpenedNotCompleteEvent.Write(logger, ServerLogLevel.Information, null, (groupId, memberState));
    }

    private static class Events
    {
        internal static readonly LogEvent FollowerLogLeakedOnShutdownTimeoutEvent = new(4009, nameof(ReplicaFollowerLog.FollowerLogLeakedOnShutdownTimeout), "Replica group {GroupId} follower log did not drain within the shutdown budget of {Budget}; {FaultedInFlightWaiters} in-flight waiters were faulted and the log handle, its worker thread and the gate holder are leaked");

        internal static readonly LogEvent FollowerSenderLeakedOnShutdownEvent = new(4018, nameof(ReplicaFollowerLog.ReplicaFollowerSenderLeakedOnShutdown), "Replica follower sender did not finish its in-flight request within the shutdown budget of {Budget}; the request ignored cancellation and is abandoned");

        internal static readonly LogEvent ReplicaFollowerSenderCloseFailedEvent = new(4019, nameof(ReplicaFollowerLog.ReplicaFollowerSenderCloseFailed), "Replica follower sender failed to close; the shutdown continues");

        internal static readonly LogEvent ReplicaFollowerCaughtUpEvent = new(4021, nameof(ReplicaFollowerLog.ReplicaFollowerCaughtUp), "Replica group {GroupId} follower {NodeId} caught up through index {HeldThrough} with {Entries} entries and counts toward the write quorum again");

        internal static readonly LogEvent ReplicaCatchUpCompactedEvent = new(4022, nameof(ReplicaFollowerLog.ReplicaCatchUpCompacted), "Replica group {GroupId} follower {NodeId} needs entries the leader already compacted; it cannot be caught up from the log, so a state transfer is attempted and it stays out of the write quorum until the transfer completes");

        internal static readonly LogEvent ReplicaCatchUpStoppedEvent = new(4023, nameof(ReplicaFollowerLog.ReplicaCatchUpStopped), "Replica group {GroupId} follower {NodeId} catch-up stopped: {Outcome}; the follower stays out of the write quorum");

        internal static readonly LogEvent ReplicaCatchUpCorruptEvent = new(4024, nameof(ReplicaFollowerLog.ReplicaCatchUpCorrupt), "Replica group {GroupId} leader log entry could not be read back intact for the catch-up of follower {NodeId}; the log needs operator attention");

        internal static readonly LogEvent ReplicaFollowerDemotedEvent = new(4026, nameof(ReplicaFollowerLog.ReplicaFollowerDemoted), "Replica group {GroupId} follower {NodeId} did not acknowledge the append of index {LogIndex}; it stops counting toward the write quorum until it is caught up");

        internal static readonly LogEvent ReplicaCatchUpInterruptedEvent = new(4025, nameof(ReplicaFollowerLog.ReplicaCatchUpInterrupted), "Replica group {GroupId} follower {NodeId} catch-up ended: {Outcome}; it is retried on the next verification");

        internal static readonly LogEvent ReplicaFollowerApplyStoppedEvent = new(4028, nameof(ReplicaFollowerLog.ReplicaFollowerApplyStopped), "Replica group {GroupId} stopped applying committed entries on this follower: a committed record cannot be applied, and the later entries stay unapplied until the record gets operator attention or a state transfer replaces the copy");

        internal static readonly LogEvent ReplicaFollowerApplyRetryEvent = new(4029, nameof(ReplicaFollowerLog.ReplicaFollowerApplyRetry), "Replica group {GroupId} could not apply committed entries on this follower and will retry");

        internal static readonly LogEvent ReplicaFollowerApplyNoLogEvent = new(4031, nameof(ReplicaFollowerLog.ReplicaFollowerApplyNoLog), "Replica group {GroupId} has no open log on this node, so this follower applies none of its committed entries");

        internal static readonly LogEvent FollowerLogAppendUnconfirmedEvent = new(4077, nameof(ReplicaFollowerLog.FollowerLogAppendUnconfirmed), "Replica group {GroupId} log failed after its frames were handed to the storage; they may be on disk and a restart can recover them as an uncommitted tail, so the log refuses every append until the node restarts");

        internal static readonly LogEvent FollowerLogTruncateUnconfirmedEvent = new(4078, nameof(ReplicaFollowerLog.FollowerLogTruncateUnconfirmed), "Replica group {GroupId} log truncate failed before its durable point; the file may or may not be cut, so the log refuses every append until the node restarts and re-reads it");

        internal static readonly LogEvent FollowerLogMirrorFailedEvent = new(4079, nameof(ReplicaFollowerLog.FollowerLogMirrorFailed), "Replica group {GroupId} log failed because the mirror copy of its metadata did not complete; the first copy is durable, and the log refuses every append until the node restarts and re-reads it");

        internal static readonly LogEvent FollowerLogMirrorFailedOnDisposeEvent = new(4080, nameof(ReplicaFollowerLog.FollowerLogMirrorFailedOnDispose), "Replica group {GroupId} mirror copy of its metadata failed before the log was disposed; the first copy is durable, and the next open re-reads it");

        internal static readonly LogEvent FollowerLogOpenedNotCompleteEvent = new(4062, nameof(ReplicaFollowerLog.FollowerLogOpenedNotComplete), "Replica group {GroupId} log opened with a copy that is {MemberState}: it takes no entries until the group is started or the copy is repaired");
    }
}
