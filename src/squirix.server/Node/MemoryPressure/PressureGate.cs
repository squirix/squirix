using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Default <see cref="IMemoryPressureGate" /> using pressure state evaluation and approximate accounting.
/// </summary>
[Immutable]
internal sealed class PressureGate : IMemoryPressureGate
{
    private readonly IMemoryUsageAccounting _accounting;
    private readonly IMemoryPressureStateEvaluator _evaluator;
    private readonly string _nodeId;
    private readonly Counter<long> _rejectionsTotal;

    /// <summary>
    /// Initializes a new instance of the <see cref="PressureGate" /> class.
    /// </summary>
    /// <param name="evaluator">Pressure state evaluator.</param>
    /// <param name="accounting">Approximate global accounting snapshot input.</param>
    /// <param name="nodeId">This node's id for low-cardinality metrics only.</param>
    /// <param name="meter">Server-wide meter used to create the rejection counter.</param>
    internal PressureGate(IMemoryPressureStateEvaluator evaluator, IMemoryUsageAccounting accounting, string nodeId, Meter meter)
    {
        ArgumentNullException.ThrowIfNull(evaluator);
        ArgumentNullException.ThrowIfNull(accounting);
        ArgumentNullException.ThrowIfNull(nodeId);
        _evaluator = evaluator;
        _accounting = accounting;
        _nodeId = nodeId;
        ArgumentNullException.ThrowIfNull(meter);
        _rejectionsTotal = meter.CreateCounter<long>("squirix_memory_rejections_total", "{rejection}", "Memory admission rejections by operation and reason");
    }

    /// <inheritdoc />
    public void ThrowIfMemoryGrowingWriteRejected(long estimatedNetGrowthBytes, bool magnitudeUnknown, string operation)
    {
        var boundedGrowth = estimatedNetGrowthBytes < 0 ? 0 : estimatedNetGrowthBytes;
        var currentBytes = _accounting.ReadEstimatedBytes();
        var notCritical = _evaluator.Evaluate(currentBytes) != PressureLevel.Critical;
        if (notCritical && (magnitudeUnknown || boundedGrowth <= 0 || _evaluator.Evaluate(AddSaturating(currentBytes, boundedGrowth)) != PressureLevel.Critical))
            return;

        if (!magnitudeUnknown && boundedGrowth <= 0)
            return;

        _accounting.RecordAdmissionRejection();
        var unknown = string.IsNullOrEmpty(operation) ? AdmissionOperations.Unknown : operation;
        var tags = new TagList
        {
            { "node", _nodeId },
            { "operation", unknown },
            { "reason", ClassifyRejectionReason(magnitudeUnknown, boundedGrowth) },
        };
        _rejectionsTotal.Add(1, in tags);
        throw new ResourceExhaustedException();
    }

    private static long AddSaturating(long left, long right)
    {
        var addSaturating = left > long.MaxValue - right ? long.MaxValue : left + right;
        return right <= 0 ? left : addSaturating;
    }

    private static string ClassifyRejectionReason(bool magnitudeUnknown, long boundedGrowth)
    {
        var classifyRejectionReason = boundedGrowth > 0 ? "estimated_limit" : "critical_pressure";
        return magnitudeUnknown ? "unknown_size" : classifyRejectionReason;
    }
}
