using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

/// <summary>Counts the entry catch-up sessions a replica group owner runs for its followers, on the host-scoped <see cref="Meter" />.</summary>
/// <remarks>Labels stay bounded: the node and group identifiers and a closed outcome name.</remarks>
[ThreadSafe]
internal sealed class ReplicaCatchUpMetrics
{
    private readonly Counter<long> _sessionsTotal;

    internal ReplicaCatchUpMetrics(Meter meter)
    {
        ArgumentNullException.ThrowIfNull(meter);
        _sessionsTotal = meter.CreateCounter<long>(
            "squirix_replication_catch_up_sessions_total",
            "{session}",
            "Entry catch-up sessions the replica group owner ran for its followers, by outcome");
    }

    /// <summary>Counts one entry catch-up session of a follower of the replica group this node owns.</summary>
    /// <param name="nodeId">The observing node identifier.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="outcome">The closed session outcome name.</param>
    internal void ReportSession(string nodeId, string groupId, string outcome)
    {
        var tags = new TagList
        {
            { "node", nodeId },
            { "group", groupId },
            { "outcome", outcome },
        };
        _sessionsTotal.Add(1, in tags);
    }
}
