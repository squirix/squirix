using Squirix.Server.Logging;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Logs of the member memory pressure a leader tracks.</summary>
internal static class ReplicaMemberPressureLog
{
    internal static void Cleared(IServerLogger logger, string groupId, int slot)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ClearedEvent.Write(logger, ServerLogLevel.Information, null, (groupId, slot));
    }

    internal static void Flagged(IServerLogger logger, string groupId, int slot)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.FlaggedEvent.Write(logger, ServerLogLevel.Warning, null, (groupId, slot));
    }

    private static class Events
    {
        internal static readonly LogEvent ClearedEvent = new(4082, nameof(Cleared), "Replica group {GroupId} member in slot {Slot} no longer reports critical memory pressure");
        internal static readonly LogEvent FlaggedEvent = new(4081, nameof(Flagged), "Replica group {GroupId} member in slot {Slot} reported critical memory pressure: the group refuses memory-growing writes until it reports otherwise or its report lapses");
    }
}
