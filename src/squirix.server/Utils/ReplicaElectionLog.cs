using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic events for replica elections.</summary>
internal static class ReplicaElectionLog
{
    internal static void ReplicaElectionDenied(IServerLogger logger, string groupId, ulong term, string denial)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.ReplicaElectionDeniedEvent.Write(logger, ServerLogLevel.Debug, null, (groupId, term, denial));
    }

    internal static void ReplicaElectionPreVoteLost(IServerLogger logger, string groupId, ulong term)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.ReplicaElectionPreVoteLostEvent.Write(logger, ServerLogLevel.Debug, null, (groupId, term));
    }

    internal static void ReplicaElectionVoteLost(IServerLogger logger, string groupId, ulong term)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ReplicaElectionVoteLostEvent.Write(logger, ServerLogLevel.Information, null, (groupId, term));
    }

    internal static void ReplicaElectionElected(IServerLogger logger, string groupId, ulong term)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ReplicaElectionElectedEvent.Write(logger, ServerLogLevel.Information, null, (groupId, term));
    }

    internal static void ReplicaElectionAuthorized(IServerLogger logger, string groupId, ulong term)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ReplicaElectionAuthorizedEvent.Write(logger, ServerLogLevel.Information, null, (groupId, term));
    }

    internal static void ReplicaElectionPromotionPending(IServerLogger logger, string groupId, ulong term)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.ReplicaElectionPromotionPendingEvent.Write(logger, ServerLogLevel.Debug, null, (groupId, term));
    }

    internal static void ReplicaElectionSteppedDown(IServerLogger logger, string groupId, ulong term)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ReplicaElectionSteppedDownEvent.Write(logger, ServerLogLevel.Information, null, (groupId, term));
    }

    internal static void ReplicaElectionRetirePending(IServerLogger logger, string groupId, ulong term)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaElectionRetirePendingEvent.Write(logger, ServerLogLevel.Warning, null, (groupId, term));
    }

    internal static void ReplicaElectionTermObserved(IServerLogger logger, string groupId, ulong term)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ReplicaElectionTermObservedEvent.Write(logger, ServerLogLevel.Information, null, (groupId, term));
    }

    internal static void ReplicaElectionTermNotDurable(IServerLogger logger, string groupId, ulong term)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaElectionTermNotDurableEvent.Write(logger, ServerLogLevel.Warning, null, (groupId, term));
    }

    internal static void ReplicaElectionAuthorityTimedOut(IServerLogger logger, string groupId, ulong term, TimeSpan waited)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaElectionAuthorityTimedOutEvent.Write(logger, ServerLogLevel.Warning, null, (groupId, term, waited));
    }

    internal static void ReplicaElectionApplyTimedOut(IServerLogger logger, string groupId, ulong term, TimeSpan waited)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaElectionApplyTimedOutEvent.Write(logger, ServerLogLevel.Warning, null, (groupId, term, waited));
    }

    internal static void ReplicaElectionAppendTimedOut(IServerLogger logger, string groupId, ulong term, TimeSpan waited)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaElectionAppendTimedOutEvent.Write(logger, ServerLogLevel.Warning, null, (groupId, term, waited));
    }

    internal static void ReplicaElectionApplyBlocked(IServerLogger logger, string groupId, ulong term)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaElectionApplyBlockedEvent.Write(logger, ServerLogLevel.Warning, null, (groupId, term));
    }

    internal static void ReplicaElectionStopped(IServerLogger logger)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ReplicaElectionStoppedEvent.Write(logger, ServerLogLevel.Information, null, ValueTuple.Create());
    }

    internal static void ReplicaElectionNoLog(IServerLogger logger, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaElectionNoLogEvent.Write(logger, ServerLogLevel.Warning, null, ValueTuple.Create(groupId));
    }

    private static class Events
    {
        internal static readonly LogEvent ReplicaElectionDeniedEvent = new(4036, nameof(ReplicaElectionLog.ReplicaElectionDenied), "Replica group {GroupId} may not campaign after term {Term}: {Denial}");

        internal static readonly LogEvent ReplicaElectionPreVoteLostEvent = new(4037, nameof(ReplicaElectionLog.ReplicaElectionPreVoteLost), "Replica group {GroupId} pre-vote found no majority; the term stays {Term}");

        internal static readonly LogEvent ReplicaElectionVoteLostEvent = new(4038, nameof(ReplicaElectionLog.ReplicaElectionVoteLost), "Replica group {GroupId} vote round found no majority in term {Term}");

        internal static readonly LogEvent ReplicaElectionElectedEvent = new(4039, nameof(ReplicaElectionLog.ReplicaElectionElected), "Replica group {GroupId} is led by this node in term {Term}; writes wait for its leader-term entry");

        internal static readonly LogEvent ReplicaElectionAuthorizedEvent = new(4040, nameof(ReplicaElectionLog.ReplicaElectionAuthorized), "Replica group {GroupId} leader-term entry of term {Term} is committed: this node serves its writes");

        internal static readonly LogEvent ReplicaElectionPromotionPendingEvent = new(4041, nameof(ReplicaElectionLog.ReplicaElectionPromotionPending), "Replica group {GroupId} leader-term entry of term {Term} is not committed yet and is retried");

        internal static readonly LogEvent ReplicaElectionSteppedDownEvent = new(4042, nameof(ReplicaElectionLog.ReplicaElectionSteppedDown), "Replica group {GroupId} leader stepped down and follows term {Term}");

        internal static readonly LogEvent ReplicaElectionRetirePendingEvent = new(4043, nameof(ReplicaElectionLog.ReplicaElectionRetirePending), "Replica group {GroupId} stepped down in term {Term} but cannot retire while committed entries stay unapplied; retried");

        internal static readonly LogEvent ReplicaElectionTermObservedEvent = new(4044, nameof(ReplicaElectionLog.ReplicaElectionTermObserved), "Replica group {GroupId} follows the higher term {Term}");

        internal static readonly LogEvent ReplicaElectionTermNotDurableEvent = new(4045, nameof(ReplicaElectionLog.ReplicaElectionTermNotDurable), "Replica group {GroupId} could not make term {Term} durable; the step is retried");

        internal static readonly LogEvent ReplicaElectionAuthorityTimedOutEvent = new(4052, nameof(ReplicaElectionLog.ReplicaElectionAuthorityTimedOut), "Replica group {GroupId} leader of term {Term} got no authority within {Waited}; it gives the group up and waits before it campaigns again");

        internal static readonly LogEvent ReplicaElectionApplyTimedOutEvent = new(4073, nameof(ReplicaElectionLog.ReplicaElectionApplyTimedOut), "Replica group {GroupId} leader of term {Term} applied no committed entry for {Waited}; it gives the group up and waits before it campaigns again");

        internal static readonly LogEvent ReplicaElectionAppendTimedOutEvent = new(4074, nameof(ReplicaElectionLog.ReplicaElectionAppendTimedOut), "Replica group {GroupId} leader of term {Term} could not append to its own log for {Waited}; it gives the group up and waits before it campaigns again");

        internal static readonly LogEvent ReplicaElectionApplyBlockedEvent = new(4076, nameof(ReplicaElectionLog.ReplicaElectionApplyBlocked), "Replica group {GroupId} does not campaign after term {Term} because this node cannot apply the committed entries it holds; it still votes and accepts appends, and campaigns again only after its apply recovers");

        internal static readonly LogEvent ReplicaElectionStoppedEvent = new(4046, nameof(ReplicaElectionLog.ReplicaElectionStopped), "Replica group elections stopped because the host is shutting down; every group is left a follower without authority");

        internal static readonly LogEvent ReplicaElectionNoLogEvent = new(4049, nameof(ReplicaElectionLog.ReplicaElectionNoLog), "Replica group {GroupId} has no open log on this node, so no election runs for it");
    }
}
