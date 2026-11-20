using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Logs of the receiver and of the sender of a state transfer.</summary>
internal static class ReplicaTransferLog
{
    internal static void Began(ILogger logger, string groupId, string leaderNodeId, ulong index, ulong term)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.BeganEvent.Write(logger, LogLevel.Information, null, (groupId, leaderNodeId, index, term));
    }

    internal static void Completed(ILogger logger, string groupId, string leaderNodeId, ulong index, ulong entries, long durationMs)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.CompletedEvent.Write(logger, LogLevel.Information, null, (groupId, leaderNodeId, index, entries, durationMs));
    }

    internal static void Dropped(ILogger logger, string groupId, string leaderNodeId, ReplicaTransferEnd reason)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.DroppedEvent.Write(logger, LogLevel.Warning, null, (groupId, leaderNodeId, reason));
    }

    internal static void Refused(ILogger logger, string groupId, string refusalCode)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.RefusedEvent.Write(logger, LogLevel.Debug, null, (groupId, refusalCode));
    }

    internal static void Sending(ILogger logger, string groupId, string nodeId, ulong index, ulong term)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.SendingEvent.Write(logger, LogLevel.Information, null, (groupId, nodeId, index, term));
    }

    internal static void Sent(ILogger logger, string groupId, string nodeId, ulong index, string outcome, TimeSpan duration, (ulong Entries, long Bytes) volume)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.SentEvent.Write(logger, LogLevel.Information, null, (groupId, nodeId, index, outcome, duration, volume));
    }

    internal static void Declined(ILogger logger, string groupId, string nodeId, string outcome)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.DeclinedEvent.Write(logger, LogLevel.Debug, null, (groupId, nodeId, outcome));
    }

    internal static void CeilingRead(ILogger logger, string groupId, string nodeId, ulong term, int answers, int needed, ulong highest)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.CeilingReadEvent.Write(logger, LogLevel.Information, null, (groupId, nodeId, term, answers, needed, highest));
    }

    internal static void Paused(ILogger logger, string groupId, string nodeId)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.PausedEvent.Write(logger, LogLevel.Information, null, (groupId, nodeId));
    }

    internal static void JournalBusy(ILogger logger, string groupId)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.JournalBusyEvent.Write(logger, LogLevel.Debug, null, ValueTuple.Create(groupId));
    }

    internal static void Invalid(ILogger logger, string nodeId, in ReplicaTransferHeader header, string step, string reason)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.InvalidEvent.Write(logger, LogLevel.Warning, null, (nodeId, header, step, reason));
    }

    internal static void TotalsMismatch(ILogger logger, string nodeId, in ReplicaTransferHeader header, ReplicaStateTotals expected, ReplicaStateTotals actual)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.TotalsMismatchEvent.Write(logger, LogLevel.Warning, null, (nodeId, header, expected, actual));
    }

    internal static void CoolingDown(ILogger logger, string groupId, string nodeId, string reason, TimeSpan delay)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.CoolingDownEvent.Write(logger, LogLevel.Warning, null, (groupId, nodeId, reason, delay));
    }

    private static class Events
    {
        internal static readonly LogEvent BeganEvent = new(4064, nameof(ReplicaTransferLog.Began), "Replica group {GroupId} began receiving a state transfer from {LeaderNodeId} at index {Index} in term {Term}");

        internal static readonly LogEvent CompletedEvent = new(4065, nameof(ReplicaTransferLog.Completed), "Replica group {GroupId} completed the state transfer from {LeaderNodeId} at index {Index}: {Entries} entries in {DurationMs} ms");

        internal static readonly LogEvent DroppedEvent = new(4066, nameof(ReplicaTransferLog.Dropped), "Replica group {GroupId} dropped the state transfer from {LeaderNodeId}: {Reason}; the copy stays as it is");

        internal static readonly LogEvent RefusedEvent = new(4067, nameof(ReplicaTransferLog.Refused), "Replica group {GroupId} refused a state transfer message: {RefusalCode}");

        internal static readonly LogEvent SendingEvent = new(4068, nameof(ReplicaTransferLog.Sending), "Replica group {GroupId} starts a state transfer to {NodeId} at index {Index} in term {Term}");

        internal static readonly LogEvent SentEvent = new(4069, nameof(ReplicaTransferLog.Sent), "Replica group {GroupId} ended the state transfer to {NodeId} at index {Index} as {Outcome} after {Duration}: (entries, bytes) {Volume}");

        internal static readonly LogEvent DeclinedEvent = new(4070, nameof(ReplicaTransferLog.Declined), "Replica group {GroupId} did not start a state transfer to {NodeId}: {Outcome}; it is retried on the next pass");

        internal static readonly LogEvent CeilingReadEvent = new(4072, nameof(ReplicaTransferLog.CeilingRead), "Replica group {GroupId} read the term ceiling before repairing {NodeId}, which lost its disk, in term {Term}: {Answers} of {Needed} members answered, highest term {Highest}");

        internal static readonly LogEvent PausedEvent = new(4083, nameof(ReplicaTransferLog.Paused), "Replica group {GroupId} paused the state transfer to {NodeId}: its journal is at the high-water mark; the same step is sent again until the journal frees space");

        internal static readonly LogEvent JournalBusyEvent = new(4084, nameof(ReplicaTransferLog.JournalBusy), "Replica group {GroupId} refused a state transfer step because the node journal is at the high-water mark");

        internal static readonly LogEvent InvalidEvent = new(4085, nameof(ReplicaTransferLog.Invalid), "Node {NodeId} refused state transfer {Header} at {Step}: {Reason}");

        internal static readonly LogEvent TotalsMismatchEvent = new(4086, nameof(ReplicaTransferLog.TotalsMismatch), "Node {NodeId} refused state transfer {Header} at End: expected {Expected}, actual {Actual}");

        internal static readonly LogEvent CoolingDownEvent = new(4087, nameof(ReplicaTransferLog.CoolingDown), "Replica group {GroupId} pauses state transfers to {NodeId} after {Reason}; next attempt in {Delay}");
    }
}
