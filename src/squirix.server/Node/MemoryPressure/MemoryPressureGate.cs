using System;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Default <see cref="IMemoryPressureGate" /> using pressure state evaluation and approximate accounting.
/// </summary>
internal sealed class MemoryPressureGate : IMemoryPressureGate
{
    private readonly IMemoryUsageAccounting _accounting;
    private readonly IMemoryPressureStateEvaluator _evaluator;
    private readonly string _nodeId;

    /// <summary>
    /// Initializes a new instance of the <see cref="MemoryPressureGate" /> class.
    /// </summary>
    /// <param name="evaluator">Pressure state evaluator.</param>
    /// <param name="accounting">Approximate global accounting snapshot input.</param>
    /// <param name="nodeId">This node's id for low-cardinality metrics only.</param>
    public MemoryPressureGate(IMemoryPressureStateEvaluator evaluator, IMemoryUsageAccounting accounting, string nodeId)
    {
        _evaluator = evaluator ?? throw new ArgumentNullException(nameof(evaluator));
        _accounting = accounting ?? throw new ArgumentNullException(nameof(accounting));
        _nodeId = nodeId ?? throw new ArgumentNullException(nameof(nodeId));
    }

    /// <inheritdoc />
    public void ThrowIfMemoryGrowingWriteRejected(long estimatedNetGrowthBytes, bool magnitudeUnknown, string operation)
    {
        var boundedGrowth = estimatedNetGrowthBytes < 0 ? 0 : estimatedNetGrowthBytes;
        var currentBytes = _accounting.EstimatedBytes;
        if (_evaluator.Evaluate(currentBytes) != MemoryPressureState.Critical && (magnitudeUnknown || boundedGrowth <= 0 ||
                                                                                  _evaluator.Evaluate(AddSaturating(currentBytes, boundedGrowth)) != MemoryPressureState.Critical))
        {
            return;
        }

        if (!magnitudeUnknown && boundedGrowth <= 0)
            return;

        _accounting.RecordAdmissionRejection();
        var unknown = string.IsNullOrEmpty(operation) ? MemoryPressureAdmissionOperations.Unknown : operation;
        MemoryPressureMetrics.RecordRejection(_nodeId, unknown, ClassifyRejectionReason(magnitudeUnknown, boundedGrowth));
        throw new ResourceExhaustedException();
    }

    private static long AddSaturating(long left, long right) => right <= 0 ? left : left > long.MaxValue - right ? long.MaxValue : left + right;

    private static string ClassifyRejectionReason(bool magnitudeUnknown, long boundedGrowth) =>
        magnitudeUnknown ? "unknown_size" : boundedGrowth > 0 ? "estimated_limit" : "critical_pressure";
}
