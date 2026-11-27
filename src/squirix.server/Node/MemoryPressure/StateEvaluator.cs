using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Evaluates pressure from resolved thresholds and limits.</summary>
[Immutable]
internal sealed class StateEvaluator : IMemoryPressureStateEvaluator
{
    private readonly long _highBytes;
    private readonly long _criticalBytes;

    internal StateEvaluator(PressureOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _highBytes = options.HighBytes;
        _criticalBytes = options.CriticalBytes;
    }

    /// <inheritdoc />
    public PressureLevel Evaluate(long bytes)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(bytes, 0);

        return (bytes == 0 || bytes < _highBytes, bytes < _criticalBytes) switch
        {
            (true, _) => PressureLevel.Normal,
            (false, true) => PressureLevel.High,
            (false, false) => PressureLevel.Critical,
        };
    }
}
