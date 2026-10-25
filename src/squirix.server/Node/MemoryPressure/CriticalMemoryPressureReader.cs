using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Reports critical pressure while estimated cache memory is in the critical band.</summary>
[Immutable]
internal sealed class CriticalMemoryPressureReader : ICriticalMemoryPressureReader
{
    private readonly IMemoryUsageAccounting _accounting;
    private readonly IMemoryPressureStateEvaluator _evaluator;

    internal CriticalMemoryPressureReader(IMemoryPressureStateEvaluator evaluator, IMemoryUsageAccounting accounting)
    {
        ArgumentNullException.ThrowIfNull(evaluator);
        ArgumentNullException.ThrowIfNull(accounting);
        _evaluator = evaluator;
        _accounting = accounting;
    }

    public bool IsCritical() => _evaluator.Evaluate(_accounting.ReadEstimatedBytes()) is PressureLevel.Critical;
}
