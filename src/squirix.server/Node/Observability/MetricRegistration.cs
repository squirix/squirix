using System;
using Squirix.Server.Attributes;
using Squirix.Server.Node.MemoryPressure;

namespace Squirix.Server.Node.Observability;

/// <summary>Holds per-node inputs for memory pressure observable gauges.</summary>
[Immutable]
internal sealed class MetricRegistration
{
    internal MetricRegistration(string nodeId, IMemoryUsageAccounting accounting, IMemoryPressureStateEvaluator evaluator)
    {
        ArgumentNullException.ThrowIfNull(nodeId);
        ArgumentNullException.ThrowIfNull(accounting);
        ArgumentNullException.ThrowIfNull(evaluator);
        NodeId = nodeId;
        Accounting = accounting;
        Evaluator = evaluator;
    }

    internal IMemoryUsageAccounting Accounting { get; }

    internal IMemoryPressureStateEvaluator Evaluator { get; }

    internal string NodeId { get; }
}
