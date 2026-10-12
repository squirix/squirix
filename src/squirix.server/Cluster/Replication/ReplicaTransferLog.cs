using Microsoft.Extensions.Logging;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Logs of the receiver of a state transfer.</summary>
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
}
