using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

/// <summary>Holds per-node inputs for idempotency observable gauges.</summary>
[Immutable]
internal sealed class IdempotencyMetricRegistration
{
    internal IdempotencyMetricRegistration(string nodeId, Func<int> recordCount, Func<long> recordBytes)
    {
        ArgumentNullException.ThrowIfNull(nodeId);
        ArgumentNullException.ThrowIfNull(recordCount);
        ArgumentNullException.ThrowIfNull(recordBytes);
        NodeId = nodeId;
        RecordCount = recordCount;
        RecordBytes = recordBytes;
    }

    internal string NodeId { get; }

    internal Func<long> RecordBytes { get; }

    internal Func<int> RecordCount { get; }
}
