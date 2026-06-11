using System;
using Microsoft.Extensions.Options;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Default evaluator using <see cref="IOptions{TOptions}" /> thresholds and limits.
/// </summary>
internal sealed class MemoryPressureStateEvaluator : IMemoryPressureStateEvaluator
{
    private readonly MemoryPressureOptions _options;

    public MemoryPressureStateEvaluator(IOptions<MemoryPressureOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options.Value;
    }

    /// <inheritdoc />
    public MemoryPressureState Evaluate(long estimatedCacheBytes)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(estimatedCacheBytes, 0);

        if (estimatedCacheBytes == 0)
            return MemoryPressureState.Normal;

        var limit = _options.MaxEstimatedCacheBytes;
        var usedPercent = (double)estimatedCacheBytes / limit * 100.0;
        return usedPercent < _options.HighPressureThresholdPercent ? MemoryPressureState.Normal :
            usedPercent < _options.CriticalPressureThresholdPercent ? MemoryPressureState.High : MemoryPressureState.Critical;
    }
}
