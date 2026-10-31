using System;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Logs of the receiver and of the sender of a state transfer.</summary>
internal static partial class ReplicaTransferLog
{
    [LoggerMessage(
        EventId = 4064,
        Level = LogLevel.Information,
        Message = "Replica group {GroupId} began receiving a state transfer from {LeaderNodeId} at index {Index} in term {Term}")]
    internal static partial void Began(ILogger logger, string groupId, string leaderNodeId, ulong index, ulong term);

    [LoggerMessage(
        EventId = 4065,
        Level = LogLevel.Information,
        Message = "Replica group {GroupId} completed the state transfer from {LeaderNodeId} at index {Index}: {Entries} entries in {DurationMs} ms")]
    internal static partial void Completed(ILogger logger, string groupId, string leaderNodeId, ulong index, ulong entries, long durationMs);

    [LoggerMessage(
        EventId = 4066,
        Level = LogLevel.Warning,
        Message = "Replica group {GroupId} dropped the state transfer from {LeaderNodeId}: {Reason}; the copy stays as it is")]
    internal static partial void Dropped(ILogger logger, string groupId, string leaderNodeId, ReplicaTransferEnd reason);

    [LoggerMessage(
        EventId = 4067,
        Level = LogLevel.Debug,
        Message = "Replica group {GroupId} refused a state transfer message: {RefusalCode}")]
    internal static partial void Refused(ILogger logger, string groupId, string refusalCode);

    [LoggerMessage(
        EventId = 4068,
        Level = LogLevel.Information,
        Message = "Replica group {GroupId} starts a state transfer to {NodeId} at index {Index} in term {Term}")]
    internal static partial void Sending(ILogger logger, string groupId, string nodeId, ulong index, ulong term);

    [LoggerMessage(
        EventId = 4069,
        Level = LogLevel.Information,
        Message = "Replica group {GroupId} ended the state transfer to {NodeId} at index {Index} as {Outcome} after {Duration}: (entries, bytes) {Volume}")]
    internal static partial void Sent(ILogger logger, string groupId, string nodeId, ulong index, string outcome, TimeSpan duration, (ulong Entries, long Bytes) volume);

    [LoggerMessage(
        EventId = 4070,
        Level = LogLevel.Debug,
        Message = "Replica group {GroupId} did not start a state transfer to {NodeId}: {Outcome}; it is retried on the next pass")]
    internal static partial void Declined(ILogger logger, string groupId, string nodeId, string outcome);

    [LoggerMessage(
        EventId = 4072,
        Level = LogLevel.Information,
        Message = "Replica group {GroupId} read the term ceiling before repairing {NodeId}, which lost its disk, in term {Term}: {Answers} of {Needed} members answered, highest term {Highest}")]
    internal static partial void CeilingRead(ILogger logger, string groupId, string nodeId, ulong term, int answers, int needed, ulong highest);

    [LoggerMessage(
        EventId = 4083,
        Level = LogLevel.Information,
        Message = "Replica group {GroupId} paused the state transfer to {NodeId}: its journal is at the high-water mark; the same step is sent again until the journal frees space")]
    internal static partial void Paused(ILogger logger, string groupId, string nodeId);

    [LoggerMessage(
        EventId = 4084,
        Level = LogLevel.Debug,
        Message = "Replica group {GroupId} refused a state transfer step because the node journal is at the high-water mark")]
    internal static partial void JournalBusy(ILogger logger, string groupId);

    [LoggerMessage(
        EventId = 4085,
        Level = LogLevel.Warning,
        Message = "Node {NodeId} refused state transfer {Header} at {Step}: {Reason}")]
    internal static partial void Invalid(ILogger logger, string nodeId, ReplicaTransferHeader header, string step, string reason);

    [LoggerMessage(
        EventId = 4086,
        Level = LogLevel.Warning,
        Message = "Node {NodeId} refused state transfer {Header} at End: expected {Expected}, actual {Actual}")]
    internal static partial void TotalsMismatch(ILogger logger, string nodeId, ReplicaTransferHeader header, ReplicaStateTotals expected, ReplicaStateTotals actual);

    [LoggerMessage(
        EventId = 4087,
        Level = LogLevel.Warning,
        Message = "Replica group {GroupId} pauses state transfers to {NodeId} after {Reason}; next attempt in {Delay}")]
    internal static partial void CoolingDown(ILogger logger, string groupId, string nodeId, string reason, TimeSpan delay);
}
