using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Logs of the receiver and of the sender of a state transfer.</summary>
internal static class ReplicaTransferLog
{
    internal static void Began(IServerLogger logger, string groupId, string leaderNodeId, ulong index, ulong term)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.BeganEvent.Write(logger, ServerLogLevel.Information, null, (groupId, leaderNodeId, index, term));
    }

    internal static void CeilingRead(IServerLogger logger, string groupId, string nodeId, ulong term, int answers, int needed, ulong highest)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.CeilingReadEvent.Write(logger, ServerLogLevel.Information, null, (groupId, nodeId, term, answers, needed, highest));
    }

    internal static void Completed(IServerLogger logger, string groupId, string leaderNodeId, ulong index, ulong entries, long durationMs)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.CompletedEvent.Write(logger, ServerLogLevel.Information, null, (groupId, leaderNodeId, index, entries, durationMs));
    }

    internal static void CoolingDown(IServerLogger logger, string groupId, string nodeId, string reason, TimeSpan delay)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.CoolingDownEvent.Write(logger, ServerLogLevel.Warning, null, (groupId, nodeId, reason, delay));
    }

    internal static void Declined(IServerLogger logger, string groupId, string nodeId, string outcome)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.DeclinedEvent.Write(logger, ServerLogLevel.Debug, null, (groupId, nodeId, outcome));
    }

    internal static void Dropped(IServerLogger logger, string groupId, string leaderNodeId, ReplicaTransferEnd reason)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.DroppedEvent.Write(logger, ServerLogLevel.Warning, null, (groupId, leaderNodeId, reason));
    }

    internal static void Invalid(IServerLogger logger, string nodeId, in ReplicaTransferHeader header, string step, string reason)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.InvalidEvent.Write(logger, ServerLogLevel.Warning, null, (nodeId, header, step, reason));
    }

    internal static void JournalBusy(IServerLogger logger, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.JournalBusyEvent.Write(logger, ServerLogLevel.Debug, null, ValueTuple.Create(groupId));
    }

    internal static void Paused(IServerLogger logger, string groupId, string nodeId)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.PausedEvent.Write(logger, ServerLogLevel.Information, null, (groupId, nodeId));
    }

    internal static void Refused(IServerLogger logger, string groupId, string refusalCode)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.RefusedEvent.Write(logger, ServerLogLevel.Debug, null, (groupId, refusalCode));
    }

    internal static void Sending(IServerLogger logger, string groupId, string nodeId, ulong index, ulong term)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.SendingEvent.Write(logger, ServerLogLevel.Information, null, (groupId, nodeId, index, term));
    }

    internal static void Sent(IServerLogger logger, string groupId, string nodeId, ulong index, string outcome, TimeSpan duration, (ulong Entries, long Bytes) volume)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.SentEvent.Write(logger, ServerLogLevel.Information, null, (groupId, nodeId, index, outcome, duration, volume));
    }

    internal static void TotalsMismatch(IServerLogger logger, string nodeId, in ReplicaTransferHeader header, ReplicaStateTotals expected, ReplicaStateTotals actual)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.TotalsMismatchEvent.Write(logger, ServerLogLevel.Warning, null, (nodeId, header, expected, actual));
    }

    private static class Events
    {
        internal static readonly LogEvent BeganEvent = new(4064, nameof(Began), "Replica group {GroupId} began receiving a state transfer from {LeaderNodeId} at index {Index} in term {Term}");

        internal static readonly LogEvent CeilingReadEvent = new(4072, nameof(CeilingRead), "Replica group {GroupId} read the term ceiling before repairing {NodeId}, which lost its disk, in term {Term}: {Answers} of {Needed} members answered, highest term {Highest}");

        internal static readonly LogEvent CompletedEvent = new(4065, nameof(Completed), "Replica group {GroupId} completed the state transfer from {LeaderNodeId} at index {Index}: {Entries} entries in {DurationMs} ms");

        internal static readonly LogEvent CoolingDownEvent = new(4087, nameof(CoolingDown), "Replica group {GroupId} pauses state transfers to {NodeId} after {Reason}; next attempt in {Delay}");

        internal static readonly LogEvent DeclinedEvent = new(4070, nameof(Declined), "Replica group {GroupId} did not start a state transfer to {NodeId}: {Outcome}; it is retried on the next pass");

        internal static readonly LogEvent DroppedEvent = new(4066, nameof(Dropped), "Replica group {GroupId} dropped the state transfer from {LeaderNodeId}: {Reason}; the copy stays as it is");

        internal static readonly LogEvent InvalidEvent = new(4085, nameof(Invalid), "Node {NodeId} refused state transfer {Header} at {Step}: {Reason}");

        internal static readonly LogEvent JournalBusyEvent = new(4084, nameof(JournalBusy), "Replica group {GroupId} refused a state transfer step because the node journal is at the high-water mark");

        internal static readonly LogEvent PausedEvent = new(4083, nameof(Paused), "Replica group {GroupId} paused the state transfer to {NodeId}: its journal is at the high-water mark; the same step is sent again until the journal frees space");

        internal static readonly LogEvent RefusedEvent = new(4067, nameof(Refused), "Replica group {GroupId} refused a state transfer message: {RefusalCode}");

        internal static readonly LogEvent SendingEvent = new(4068, nameof(Sending), "Replica group {GroupId} starts a state transfer to {NodeId} at index {Index} in term {Term}");

        internal static readonly LogEvent SentEvent = new(4069, nameof(Sent), "Replica group {GroupId} ended the state transfer to {NodeId} at index {Index} as {Outcome} after {Duration}: (entries, bytes) {Volume}");

        internal static readonly LogEvent TotalsMismatchEvent = new(4086, nameof(TotalsMismatch), "Node {NodeId} refused state transfer {Header} at End: expected {Expected}, actual {Actual}");
    }
}
