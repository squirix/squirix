using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

/// <summary>Holds per-node inputs for idempotency observable gauges.</summary>
[Immutable]
internal sealed class IdempotencyMetricRegistration
{
    internal IdempotencyMetricRegistration(string nodeId, Func<int> recordCount)
    {
        ArgumentNullException.ThrowIfNull(nodeId);
        ArgumentNullException.ThrowIfNull(recordCount);
        NodeId = nodeId;
        RecordCount = recordCount;
    }

    internal string NodeId { get; }

    internal Func<int> RecordCount { get; }
}
