using System;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Suppresses background snapshots while estimated cache memory is in the critical pressure band.</summary>
internal sealed class BackgroundSnapshotMemoryThrottle : IBackgroundSnapshotMemoryThrottle
{
    private readonly IMemoryUsageAccounting _accounting;
    private readonly IMemoryPressureStateEvaluator _evaluator;

    internal BackgroundSnapshotMemoryThrottle(IMemoryPressureStateEvaluator evaluator, IMemoryUsageAccounting accounting)
    {
        _evaluator = evaluator ?? throw new ArgumentNullException(nameof(evaluator));
        _accounting = accounting ?? throw new ArgumentNullException(nameof(accounting));
    }

    public bool ShouldSuppressBackgroundSnapshot() => _evaluator.Evaluate(_accounting.ReadEstimatedBytes()) is PressureLevel.Critical;
}
