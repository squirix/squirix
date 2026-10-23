using Microsoft.Extensions.Logging;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Logs of the replica group registry.</summary>
internal static partial class ReplicaGroupRegistryLog
{
    [LoggerMessage(
        EventId = 4030,
        Level = LogLevel.Warning,
        Message = "Replica group {GroupId} recovered uncommitted log entry {LogIndex} is not pinned for idempotent retries: {Reason}")]
    internal static partial void TailPinSkipped(ILogger logger, string groupId, ulong logIndex, string reason);
}
