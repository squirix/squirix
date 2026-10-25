using Microsoft.Extensions.Logging;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Logs of the member memory pressure a leader tracks.</summary>
internal static partial class ReplicaMemberPressureLog
{
    [LoggerMessage(
        EventId = 4081,
        Level = LogLevel.Warning,
        Message = "Replica group {GroupId} member in slot {Slot} reported critical memory pressure: the group refuses memory-growing writes until it reports otherwise or its report lapses")]
    internal static partial void Flagged(ILogger logger, string groupId, int slot);

    [LoggerMessage(
        EventId = 4082,
        Level = LogLevel.Information,
        Message = "Replica group {GroupId} member in slot {Slot} no longer reports critical memory pressure")]
    internal static partial void Cleared(ILogger logger, string groupId, int slot);
}
