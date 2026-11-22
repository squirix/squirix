using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic events for replica bootstrap and verification.</summary>
internal static class ReplicaBootstrapLog
{
    internal static void ReplicaBootstrapAlreadyComplete(IServerLogger logger, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.ReplicaBootstrapAlreadyCompleteEvent.Write(logger, ServerLogLevel.Debug, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaBootstrapCompleted(IServerLogger logger, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ReplicaBootstrapCompletedEvent.Write(logger, ServerLogLevel.Information, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaBootstrapIncomplete(IServerLogger logger, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaBootstrapIncompleteEvent.Write(logger, ServerLogLevel.Warning, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaBootstrapLogNotReady(IServerLogger logger, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ReplicaBootstrapLogNotReadyEvent.Write(logger, ServerLogLevel.Information, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaBootstrapMemberComplete(IServerLogger logger, string groupId, string nodeId)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaBootstrapMemberCompleteEvent.Write(logger, ServerLogLevel.Warning, null, (groupId, nodeId));
    }

    internal static void ReplicaBootstrapOfferRefused(IServerLogger logger, string groupId, string nodeId, string memberState, string refusalCode)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ReplicaBootstrapOfferRefusedEvent.Write(logger, ServerLogLevel.Information, null, (groupId, nodeId, memberState, refusalCode));
    }

    internal static void ReplicaBootstrapRetry(IServerLogger logger, string groupId, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.ReplicaBootstrapRetryEvent.Write(logger, ServerLogLevel.Debug, exception, ValueTuple.Create(groupId));
    }

    internal static void ReplicaBootstrapStopped(IServerLogger logger, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ReplicaBootstrapStoppedEvent.Write(logger, ServerLogLevel.Information, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaBootstrapWaiting(IServerLogger logger, string groupId, string nodeId)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ReplicaBootstrapWaitingEvent.Write(logger, ServerLogLevel.Information, null, (groupId, nodeId));
    }

    internal static void ReplicaVerificationBlocked(IServerLogger logger, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaVerificationBlockedEvent.Write(logger, ServerLogLevel.Warning, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaVerificationComplete(IServerLogger logger, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ReplicaVerificationCompleteEvent.Write(logger, ServerLogLevel.Information, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaVerificationPending(IServerLogger logger, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ReplicaVerificationPendingEvent.Write(logger, ServerLogLevel.Information, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaVerificationRefused(IServerLogger logger, string groupId, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaVerificationRefusedEvent.Write(logger, ServerLogLevel.Warning, exception, ValueTuple.Create(groupId));
    }

    internal static void ReplicaVerificationRetry(IServerLogger logger, string groupId, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.ReplicaVerificationRetryEvent.Write(logger, ServerLogLevel.Debug, exception, ValueTuple.Create(groupId));
    }

    internal static void ReplicaVerificationUnseeded(IServerLogger logger, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ReplicaVerificationUnseededEvent.Write(logger, ServerLogLevel.Information, null, ValueTuple.Create(groupId));
    }

    private static class Events
    {
        internal static readonly LogEvent ReplicaBootstrapAlreadyCompleteEvent = new(4059, nameof(ReplicaBootstrapAlreadyComplete), "Replica group {GroupId} copy of this node is complete; the first start has nothing to do");

        internal static readonly LogEvent ReplicaBootstrapCompletedEvent = new(4056, nameof(ReplicaBootstrapCompleted), "Replica group {GroupId} is seeded: every member accepted the offer and the copy of this node is complete");

        internal static readonly LogEvent ReplicaBootstrapIncompleteEvent = new(4058, nameof(ReplicaBootstrapIncomplete), "Replica group {GroupId} copy of this node is incomplete, so the first start has nothing to do for it");

        internal static readonly LogEvent ReplicaBootstrapLogNotReadyEvent = new(4057, nameof(ReplicaBootstrapLogNotReady), "Replica group {GroupId} first start waits for the log of this node to be ready");

        internal static readonly LogEvent ReplicaBootstrapMemberCompleteEvent = new(4054, nameof(ReplicaBootstrapMemberComplete), "Replica group {GroupId} first start found a complete copy on node {NodeId}, so the empty copy of this node needs repair; it is not seeded");

        internal static readonly LogEvent ReplicaBootstrapOfferRefusedEvent = new(4055, nameof(ReplicaBootstrapOfferRefused), "Replica group {GroupId} first start: node {NodeId} did not accept the offer (member state {MemberState}, refusal {RefusalCode}); it is retried");

        internal static readonly LogEvent ReplicaBootstrapRetryEvent = new(4060, nameof(ReplicaBootstrapRetry), "Replica group {GroupId} first start attempt failed and will be retried");

        internal static readonly LogEvent ReplicaBootstrapStoppedEvent = new(4061, nameof(ReplicaBootstrapStopped), "Replica group {GroupId} first start stopped because the host is shutting down; the copy of this node stays unseeded");

        internal static readonly LogEvent ReplicaBootstrapWaitingEvent = new(4053, nameof(ReplicaBootstrapWaiting), "Replica group {GroupId} first start waits for node {NodeId}: it did not answer, is not ready, or is not an unseeded copy of the same topology");
        internal static readonly LogEvent ReplicaVerificationBlockedEvent = new(4001, nameof(ReplicaVerificationBlocked), "Replica group {GroupId} verification cannot proceed: the leader log is not ready or its uncommitted tail has no entry of the current term");

        internal static readonly LogEvent ReplicaVerificationCompleteEvent = new(4002, nameof(ReplicaVerificationComplete), "Replica group {GroupId} verification is complete: every slot counts toward the write quorum");

        internal static readonly LogEvent ReplicaVerificationPendingEvent = new(4004, nameof(ReplicaVerificationPending), "Replica group {GroupId} verification is pending: some followers are not yet verified against the leader log");

        internal static readonly LogEvent ReplicaVerificationRefusedEvent = new(4051, nameof(ReplicaVerificationRefused), "Replica group {GroupId} cannot be verified because the journal or memory refused the write; retried until it has room");

        internal static readonly LogEvent ReplicaVerificationRetryEvent = new(4003, nameof(ReplicaVerificationRetry), "Replica group {GroupId} verification attempt failed and will be retried");

        internal static readonly LogEvent ReplicaVerificationUnseededEvent = new(4063, nameof(ReplicaVerificationUnseeded), "Replica group {GroupId} verification waits for the first start of the group: the copy of this node is not seeded yet");
    }
}
