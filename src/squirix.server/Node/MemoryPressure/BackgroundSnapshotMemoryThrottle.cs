using System;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Suppresses background snapshots while estimated cache memory is in the critical pressure band.</summary>
[Immutable]
internal sealed class BackgroundSnapshotMemoryThrottle : IBackgroundSnapshotMemoryThrottle
{
    private readonly IMemoryUsageAccounting _accounting;
    private readonly IMemoryPressureStateEvaluator _evaluator;

    internal BackgroundSnapshotMemoryThrottle(IMemoryPressureStateEvaluator evaluator, IMemoryUsageAccounting accounting)
    {
        ArgumentNullException.ThrowIfNull(evaluator);
        ArgumentNullException.ThrowIfNull(accounting);
        _evaluator = evaluator;
        _accounting = accounting;
    }

    public bool ShouldSuppressBackgroundSnapshot() => _evaluator.Evaluate(_accounting.ReadEstimatedBytes()) is PressureLevel.Critical;
}
