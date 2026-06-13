using Microsoft.Extensions.Options;
using Squirix.Server.Node.MemoryPressure;
using Xunit;

namespace Squirix.Server.UnitTests.Node.MemoryPressure;

/// <summary>
/// Tests for <see cref="MemoryPressureStateEvaluator" /> threshold boundaries.
/// </summary>
public sealed class MemoryPressureStateEvaluatorTests
{
    /// <summary>
    /// Verifies usage above the critical ratio maps to <see cref="MemoryPressureState.Critical" />.
    /// </summary>
    [Fact]
    public void EvaluateReturnsCriticalAboveCriticalThreshold()
    {
        var e = CreateEvaluator(
            new MemoryPressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        Assert.Equal(MemoryPressureState.Critical, e.Evaluate(1000));
    }

    /// <summary>
    /// Verifies usage exactly at the critical ratio maps to <see cref="MemoryPressureState.Critical" />.
    /// </summary>
    [Fact]
    public void EvaluateReturnsCriticalAtExactCriticalThreshold()
    {
        var e = CreateEvaluator(
            new MemoryPressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        Assert.Equal(MemoryPressureState.Critical, e.Evaluate(950));
    }

    /// <summary>
    /// Verifies usage exactly at the high ratio maps to <see cref="MemoryPressureState.High" />.
    /// </summary>
    [Fact]
    public void EvaluateReturnsHighAtExactHighThreshold()
    {
        var e = CreateEvaluator(
            new MemoryPressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        Assert.Equal(MemoryPressureState.High, e.Evaluate(800));
    }

    /// <summary>
    /// Verifies usage between high and critical ratios maps to <see cref="MemoryPressureState.High" />.
    /// </summary>
    [Fact]
    public void EvaluateReturnsHighBetweenThresholds()
    {
        var e = CreateEvaluator(
            new MemoryPressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        Assert.Equal(MemoryPressureState.High, e.Evaluate(900));
    }

    /// <summary>
    /// Verifies usage below the high ratio maps to <see cref="MemoryPressureState.Normal" />.
    /// </summary>
    [Fact]
    public void EvaluateReturnsNormalBelowHighThreshold()
    {
        var e = CreateEvaluator(
            new MemoryPressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        Assert.Equal(MemoryPressureState.Normal, e.Evaluate(799));
    }

    /// <summary>
    /// Verifies zero estimated usage maps to <see cref="MemoryPressureState.Normal" />.
    /// </summary>
    [Fact]
    public void EvaluateReturnsNormalForZeroUsage()
    {
        var e = CreateEvaluator(
            new MemoryPressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        Assert.Equal(MemoryPressureState.Normal, e.Evaluate(0));
    }

    private static MemoryPressureStateEvaluator CreateEvaluator(MemoryPressureOptions options) => new(new MemoryPressureOptionsBinding(options));

    private sealed class MemoryPressureOptionsBinding : IOptions<MemoryPressureOptions>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryPressureOptionsBinding" /> class.
        /// </summary>
        /// <param name="value">Bound options value.</param>
        public MemoryPressureOptionsBinding(MemoryPressureOptions value)
        {
            Value = value;
        }

        /// <inheritdoc />
        public MemoryPressureOptions Value { get; }
    }
}
