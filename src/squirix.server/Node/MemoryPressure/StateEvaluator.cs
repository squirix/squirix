using System;
using Microsoft.Extensions.Options;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Default evaluator using <see cref="IOptions{TOptions}" /> thresholds and limits.
/// </summary>
internal sealed class StateEvaluator : IMemoryPressureStateEvaluator
{
    private readonly PressureOptions _options;

    public StateEvaluator(IOptions<PressureOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options.Value;
    }

    /// <inheritdoc />
    public PressureLevel Evaluate(long estimatedCacheBytes)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(estimatedCacheBytes, 0);

        if (estimatedCacheBytes == 0)
            return PressureLevel.Normal;

        var limit = _options.MaxEstimatedCacheBytes;
        var usedPercent = 1.0 * estimatedCacheBytes / limit * 100.0;
        if (usedPercent < _options.HighPressureThresholdPercent)
            return PressureLevel.Normal;

        return usedPercent < _options.CriticalPressureThresholdPercent ? PressureLevel.High : PressureLevel.Critical;
    }
}
