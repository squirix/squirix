using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Node.MemoryPressure;

namespace Squirix.Server.Node.App;

/// <summary>Composition-layer admission gate for memory-growing cache writes under critical pressure.</summary>
[Immutable]
internal sealed class PressureGate
{
    /// <summary>The rejection reason of a write of known growth refused because this node is already under critical memory pressure.</summary>
    internal const string CriticalPressureReason = "critical_pressure";

    /// <summary>The rejection reason of a write whose own growth would bring this node to critical memory pressure.</summary>
    internal const string EstimatedLimitReason = "estimated_limit";

    /// <summary>The rejection reason of a write refused because a replica of its group is under critical memory pressure.</summary>
    internal const string ReplicaPressureReason = "replica_pressure";

    /// <summary>The rejection reason of a write of unbounded growth refused because this node is under critical memory pressure.</summary>
    internal const string UnknownSizeReason = "unknown_size";

    private readonly IMemoryUsageAccounting _accounting;
    private readonly IMemoryPressureStateEvaluator _evaluator;
    private readonly string _nodeId;
    private readonly Counter<long> _rejectionsTotal;

    /// <summary>Initializes a new instance of the <see cref="PressureGate" /> class.</summary>
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

    /// <summary>Tells, without counting a rejection, whether memory pressure policy would refuse a memory-growing write now.</summary>
    /// <param name="estimatedNetGrowthBytes">Best-effort net growth in estimated resident bytes; a negative value counts as no growth.</param>
    /// <param name="magnitudeUnknown">When <see langword="true" />, the net growth cannot be bounded cheaply.</param>
    /// <returns><see langword="true" /> when <see cref="ThrowIfMemoryGrowingWriteRejected" /> would throw.</returns>
    public bool RejectsMemoryGrowingWrite(long estimatedNetGrowthBytes, bool magnitudeUnknown) =>
        AdmissionPolicy.Evaluate(_evaluator, _accounting, estimatedNetGrowthBytes, magnitudeUnknown) != AdmissionRejection.None;

    /// <summary>
    /// Throws an internal memory-pressure admission exception when the operation must not proceed because
    /// memory pressure policy rejects a growing mutation at critical pressure.
    /// </summary>
    /// <param name="estimatedNetGrowthBytes">Best-effort net growth in estimated resident bytes; a negative value counts as no growth.</param>
    /// <param name="magnitudeUnknown">When <see langword="true" />, the net growth cannot be bounded cheaply; policy treats this conservatively at critical pressure.</param>
    /// <param name="operation">Bounded admission operation label for metrics (never a cache name or key).</param>
    /// <exception cref="ResourceExhaustedException">The write is refused.</exception>
    public void ThrowIfMemoryGrowingWriteRejected(long estimatedNetGrowthBytes, bool magnitudeUnknown, string operation)
    {
        var reason = AdmissionPolicy.Evaluate(_evaluator, _accounting, estimatedNetGrowthBytes, magnitudeUnknown);
        if (reason == AdmissionRejection.None)
            return;

        throw Reject(operation, ReasonOf(reason));
    }

    /// <summary>
    /// Throws an internal memory-pressure admission exception because a member of the replica group that owns the key is under critical
    /// memory pressure, so the group must not grow.
    /// </summary>
    /// <param name="operation">Bounded admission operation label for metrics (never a cache name or key).</param>
    /// <exception cref="ResourceExhaustedException">Always.</exception>
    public void ThrowReplicaPressureRejection(string operation) => throw Reject(operation, ReplicaPressureReason);

    private static string ReasonOf(AdmissionRejection reason) => reason switch
    {
        AdmissionRejection.CriticalPressure => CriticalPressureReason,
        AdmissionRejection.EstimatedLimit => EstimatedLimitReason,
        AdmissionRejection.UnknownSize => UnknownSizeReason,
        _ => throw new ArgumentOutOfRangeException(nameof(reason), reason, "Unsupported memory admission rejection."),
    };

    private ResourceExhaustedException Reject(string operation, string reason)
    {
        _accounting.RecordAdmissionRejection();
        var unknown = string.IsNullOrEmpty(operation) ? AdmissionOperations.Unknown : operation;
        var tags = new TagList
        {
            { "node", _nodeId },
            { "operation", unknown },
            { "reason", reason },
        };
        _rejectionsTotal.Add(1, in tags);
        return new ResourceExhaustedException();
    }
}
