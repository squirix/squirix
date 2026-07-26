using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Squirix.Server.Errors;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Default <see cref="IMemoryPressureGate" /> using pressure state evaluation and approximate accounting.
/// </summary>
internal sealed class PressureGate : IMemoryPressureGate
{
    private readonly IMemoryUsageAccounting _accounting;
    private readonly IMemoryPressureStateEvaluator _evaluator;
    private readonly string _nodeId;

    /// <summary>
    /// Initializes a new instance of the <see cref="PressureGate" /> class.
    /// </summary>
    /// <param name="evaluator">Pressure state evaluator.</param>
    /// <param name="accounting">Approximate global accounting snapshot input.</param>
    /// <param name="nodeId">This node's id for low-cardinality metrics only.</param>
    internal PressureGate(IMemoryPressureStateEvaluator evaluator, IMemoryUsageAccounting accounting, string nodeId)
    {
        _evaluator = evaluator ?? throw new ArgumentNullException(nameof(evaluator));
        _accounting = accounting ?? throw new ArgumentNullException(nameof(accounting));
        _nodeId = nodeId ?? throw new ArgumentNullException(nameof(nodeId));
    }

    /// <inheritdoc />
    public void ThrowIfMemoryGrowingWriteRejected(long estimatedNetGrowthBytes, bool magnitudeUnknown, string operation)
    {
        var boundedGrowth = estimatedNetGrowthBytes < 0 ? 0 : estimatedNetGrowthBytes;
        var currentBytes = _accounting.ReadEstimatedBytes();
        if (_evaluator.Evaluate(currentBytes) is not PressureLevel.Critical && (magnitudeUnknown || boundedGrowth <= 0 ||
                                                                                _evaluator.Evaluate(AddSaturating(currentBytes, boundedGrowth)) is not PressureLevel.Critical))
        {
            return;
        }

        if (!magnitudeUnknown && boundedGrowth <= 0)
            return;

        _accounting.RecordAdmissionRejection();
        var unknown = string.IsNullOrEmpty(operation) ? AdmissionOperations.Unknown : operation;
        RejectionCounter.Record(_nodeId, unknown, ClassifyRejectionReason(magnitudeUnknown, boundedGrowth));
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

    /// <summary>Low-cardinality memory admission rejection counter (hot path).</summary>
    private static class RejectionCounter
    {
        private static readonly Counter<long> RejectionsTotal = new Meter("Squirix").CreateCounter<long>(
            "squirix_memory_rejections_total",
            "{rejection}",
            "Memory admission rejections by operation and reason");

        internal static void Record(string nodeId, string operation, string reason)
        {
            var tags = new TagList
            {
                { "node", nodeId },
                { "operation", operation },
                { "reason", reason },
            };
            RejectionsTotal.Add(1, in tags);
        }
    }
}
