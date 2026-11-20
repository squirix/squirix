using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic events for replica bootstrap and verification.</summary>
internal static class ReplicaBootstrapLog
{
    internal static void ReplicaVerificationBlocked(ILogger logger, string groupId)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.ReplicaVerificationBlockedEvent.Write(logger, LogLevel.Warning, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaVerificationComplete(ILogger logger, string groupId)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.ReplicaVerificationCompleteEvent.Write(logger, LogLevel.Information, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaVerificationPending(ILogger logger, string groupId)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.ReplicaVerificationPendingEvent.Write(logger, LogLevel.Information, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaVerificationRetry(ILogger logger, string groupId, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.ReplicaVerificationRetryEvent.Write(logger, LogLevel.Debug, exception, ValueTuple.Create(groupId));
    }

    internal static void ReplicaVerificationRefused(ILogger logger, string groupId, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.ReplicaVerificationRefusedEvent.Write(logger, LogLevel.Warning, exception, ValueTuple.Create(groupId));
    }

    internal static void ReplicaBootstrapWaiting(ILogger logger, string groupId, string nodeId)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.ReplicaBootstrapWaitingEvent.Write(logger, LogLevel.Information, null, (groupId, nodeId));
    }

    internal static void ReplicaBootstrapMemberComplete(ILogger logger, string groupId, string nodeId)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.ReplicaBootstrapMemberCompleteEvent.Write(logger, LogLevel.Warning, null, (groupId, nodeId));
    }

    internal static void ReplicaBootstrapOfferRefused(ILogger logger, string groupId, string nodeId, string memberState, string refusalCode)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.ReplicaBootstrapOfferRefusedEvent.Write(logger, LogLevel.Information, null, (groupId, nodeId, memberState, refusalCode));
    }

    internal static void ReplicaBootstrapCompleted(ILogger logger, string groupId)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.ReplicaBootstrapCompletedEvent.Write(logger, LogLevel.Information, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaBootstrapLogNotReady(ILogger logger, string groupId)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.ReplicaBootstrapLogNotReadyEvent.Write(logger, LogLevel.Information, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaBootstrapIncomplete(ILogger logger, string groupId)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.ReplicaBootstrapIncompleteEvent.Write(logger, LogLevel.Warning, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaBootstrapAlreadyComplete(ILogger logger, string groupId)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.ReplicaBootstrapAlreadyCompleteEvent.Write(logger, LogLevel.Debug, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaBootstrapRetry(ILogger logger, string groupId, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.ReplicaBootstrapRetryEvent.Write(logger, LogLevel.Debug, exception, ValueTuple.Create(groupId));
    }

    internal static void ReplicaBootstrapStopped(ILogger logger, string groupId)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.ReplicaBootstrapStoppedEvent.Write(logger, LogLevel.Information, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaVerificationUnseeded(ILogger logger, string groupId)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.ReplicaVerificationUnseededEvent.Write(logger, LogLevel.Information, null, ValueTuple.Create(groupId));
    }

    private static class Events
    {
        internal static readonly LogEvent ReplicaVerificationBlockedEvent = new(4001, nameof(ReplicaBootstrapLog.ReplicaVerificationBlocked), "Replica group {GroupId} verification cannot proceed: the leader log is not ready or its uncommitted tail has no entry of the current term");

        internal static readonly LogEvent ReplicaVerificationCompleteEvent = new(4002, nameof(ReplicaBootstrapLog.ReplicaVerificationComplete), "Replica group {GroupId} verification is complete: every slot counts toward the write quorum");

        internal static readonly LogEvent ReplicaVerificationPendingEvent = new(4004, nameof(ReplicaBootstrapLog.ReplicaVerificationPending), "Replica group {GroupId} verification is pending: some followers are not yet verified against the leader log");

        internal static readonly LogEvent ReplicaVerificationRetryEvent = new(4003, nameof(ReplicaBootstrapLog.ReplicaVerificationRetry), "Replica group {GroupId} verification attempt failed and will be retried");

        internal static readonly LogEvent ReplicaVerificationRefusedEvent = new(4051, nameof(ReplicaBootstrapLog.ReplicaVerificationRefused), "Replica group {GroupId} cannot be verified because the journal or memory refused the write; retried until it has room");

        internal static readonly LogEvent ReplicaBootstrapWaitingEvent = new(4053, nameof(ReplicaBootstrapLog.ReplicaBootstrapWaiting), "Replica group {GroupId} first start waits for node {NodeId}: it did not answer, is not ready, or is not an unseeded copy of the same topology");

        internal static readonly LogEvent ReplicaBootstrapMemberCompleteEvent = new(4054, nameof(ReplicaBootstrapLog.ReplicaBootstrapMemberComplete), "Replica group {GroupId} first start found a complete copy on node {NodeId}, so the empty copy of this node needs repair; it is not seeded");

        internal static readonly LogEvent ReplicaBootstrapOfferRefusedEvent = new(4055, nameof(ReplicaBootstrapLog.ReplicaBootstrapOfferRefused), "Replica group {GroupId} first start: node {NodeId} did not accept the offer (member state {MemberState}, refusal {RefusalCode}); it is retried");

        internal static readonly LogEvent ReplicaBootstrapCompletedEvent = new(4056, nameof(ReplicaBootstrapLog.ReplicaBootstrapCompleted), "Replica group {GroupId} is seeded: every member accepted the offer and the copy of this node is complete");

        internal static readonly LogEvent ReplicaBootstrapLogNotReadyEvent = new(4057, nameof(ReplicaBootstrapLog.ReplicaBootstrapLogNotReady), "Replica group {GroupId} first start waits for the log of this node to be ready");

        internal static readonly LogEvent ReplicaBootstrapIncompleteEvent = new(4058, nameof(ReplicaBootstrapLog.ReplicaBootstrapIncomplete), "Replica group {GroupId} copy of this node is incomplete, so the first start has nothing to do for it");

        internal static readonly LogEvent ReplicaBootstrapAlreadyCompleteEvent = new(4059, nameof(ReplicaBootstrapLog.ReplicaBootstrapAlreadyComplete), "Replica group {GroupId} copy of this node is complete; the first start has nothing to do");

        internal static readonly LogEvent ReplicaBootstrapRetryEvent = new(4060, nameof(ReplicaBootstrapLog.ReplicaBootstrapRetry), "Replica group {GroupId} first start attempt failed and will be retried");

        internal static readonly LogEvent ReplicaBootstrapStoppedEvent = new(4061, nameof(ReplicaBootstrapLog.ReplicaBootstrapStopped), "Replica group {GroupId} first start stopped because the host is shutting down; the copy of this node stays unseeded");

        internal static readonly LogEvent ReplicaVerificationUnseededEvent = new(4063, nameof(ReplicaBootstrapLog.ReplicaVerificationUnseeded), "Replica group {GroupId} verification waits for the first start of the group: the copy of this node is not seeded yet");
    }
}
