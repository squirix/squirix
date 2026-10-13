using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

/// <summary>Counts the entry catch-up sessions and the state transfers a replica group owner runs for its followers, on the host-scoped <see cref="Meter" />.</summary>
/// <remarks>Labels stay bounded: the node and group identifiers, a closed outcome name and, for a refusal of a follower, its closed refusal code.</remarks>
[ThreadSafe]
internal sealed class ReplicaCatchUpMetrics
{
    private readonly Counter<long> _sessionsTotal;
    private readonly Counter<long> _transfersTotal;

    internal ReplicaCatchUpMetrics(Meter meter)
    {
        ArgumentNullException.ThrowIfNull(meter);
        _sessionsTotal = meter.CreateCounter<long>(
            "squirix_replication_catch_up_sessions_total",
            "{session}",
            "Entry catch-up sessions the replica group owner ran for its followers, by outcome");
        _transfersTotal = meter.CreateCounter<long>(
            "squirix_replication_state_transfers_total",
            "{transfer}",
            "State transfers the replica group owner attempted for followers it cannot catch up from its log, by outcome and refusal code");
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

    /// <summary>Counts one state transfer attempt of the replica group this node leads.</summary>
    /// <param name="nodeId">The observing node identifier.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="outcome">The closed transfer outcome name.</param>
    /// <param name="reason">The closed refusal code of the follower; empty when it did not refuse.</param>
    internal void ReportTransfer(string nodeId, string groupId, string outcome, string reason)
    {
        var tags = new TagList
        {
            { "node", nodeId },
            { "group", groupId },
            { "outcome", outcome },
            { "reason", reason },
        };
        _transfersTotal.Add(1, in tags);
    }
}
