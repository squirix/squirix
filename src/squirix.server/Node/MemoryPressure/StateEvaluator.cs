using System;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Default evaluator using <see cref="IOptions{TOptions}" /> thresholds and limits.
/// </summary>
[Immutable]
internal sealed class StateEvaluator : IMemoryPressureStateEvaluator
{
    private readonly PressureOptions _options;

    internal StateEvaluator(IOptions<PressureOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options.Value;
    }

    /// <inheritdoc />
    public PressureLevel Evaluate(long bytes)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(bytes, 0);

        if (bytes == 0)
            return PressureLevel.Normal;

        var limit = _options.MaxEstimatedCacheBytes;
        var percent = 1d * bytes / limit * 100d;
        return (percent < _options.HighPressureThresholdPercent, percent < _options.CriticalPressureThresholdPercent) switch
        {
            (true, _) => PressureLevel.Normal,
            (false, true) => PressureLevel.High,
            (false, false) => PressureLevel.Critical,
        };
    }
}
