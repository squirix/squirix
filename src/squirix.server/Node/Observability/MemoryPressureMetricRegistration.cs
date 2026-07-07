using System;
using Squirix.Server.Node.MemoryPressure;

namespace Squirix.Server.Node.Observability;

/// <summary>Holds per-node inputs for memory pressure observable gauges.</summary>
internal sealed class MemoryPressureMetricRegistration
{
    public MemoryPressureMetricRegistration(string nodeId, IMemoryUsageAccounting accounting, IMemoryPressureStateEvaluator evaluator)
    {
        NodeId = nodeId ?? throw new ArgumentNullException(nameof(nodeId));
        Accounting = accounting ?? throw new ArgumentNullException(nameof(accounting));
        Evaluator = evaluator ?? throw new ArgumentNullException(nameof(evaluator));
    }

    internal IMemoryUsageAccounting Accounting { get; }

    internal IMemoryPressureStateEvaluator Evaluator { get; }

    internal string NodeId { get; }
}
