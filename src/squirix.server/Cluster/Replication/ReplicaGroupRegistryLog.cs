using Squirix.Server.Logging;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Logs of the replica group registry.</summary>
internal static class ReplicaGroupRegistryLog
{
    internal static void TailPinSkipped(IServerLogger logger, string groupId, ulong logIndex, string reason)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.TailPinSkippedEvent.Write(logger, ServerLogLevel.Warning, null, (groupId, logIndex, reason));
    }

    private static class Events
    {
        internal static readonly LogEvent TailPinSkippedEvent = new(4030, nameof(ReplicaGroupRegistryLog.TailPinSkipped), "Replica group {GroupId} recovered uncommitted log entry {LogIndex} is not pinned for idempotent retries: {Reason}");
    }
}
