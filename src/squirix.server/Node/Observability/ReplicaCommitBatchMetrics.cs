using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

/// <summary>The size of the commit rounds of the replica groups a node leads, on the host-scoped <see cref="Meter" />.</summary>
[ThreadSafe]
internal sealed class ReplicaCommitBatchMetrics
{
    private readonly Histogram<long> _size;

    internal ReplicaCommitBatchMetrics(Meter meter)
    {
        ArgumentNullException.ThrowIfNull(meter);
        _size = meter.CreateHistogram<long>(
            "squirix_replication_commit_batch_size",
            "{write}",
            "Writes one commit round of a led replica group carried: the writes that queued while the previous commit ran, submitted with one append and one majority; a round refused before its append is counted too");
    }

    /// <summary>Records the size of one commit round of a replica group this node leads.</summary>
    /// <param name="nodeId">The observing node identifier.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="writes">The number of writes the round carried; one for a write that committed alone.</param>
    internal void Record(string nodeId, string groupId, int writes)
    {
        var tags = new TagList
        {
            { "node", nodeId },
            { "group", groupId },
        };
        _size.Record(writes, in tags);
    }
}
