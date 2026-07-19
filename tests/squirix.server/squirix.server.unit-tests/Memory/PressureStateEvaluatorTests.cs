using Microsoft.Extensions.Options;
using Squirix.Server.Node.MemoryPressure;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>
/// Tests for <see cref="StateEvaluator" /> threshold boundaries.
/// </summary>
public sealed class PressureStateEvaluatorTests
{
    /// <summary>
    /// Verifies usage above the critical ratio maps to <see cref="PressureLevel.Critical" />.
    /// </summary>
    [Fact]
    public void EvaluateReturnsCriticalAboveCriticalThreshold()
    {
        var e = CreateEvaluator(
            new PressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        Assert.Equal(PressureLevel.Critical, e.Evaluate(1000));
    }

    /// <summary>
    /// Verifies usage exactly at the critical ratio maps to <see cref="PressureLevel.Critical" />.
    /// </summary>
    [Fact]
    public void EvaluateReturnsCriticalAtExactCriticalThreshold()
    {
        var e = CreateEvaluator(
            new PressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        Assert.Equal(PressureLevel.Critical, e.Evaluate(950));
    }

    /// <summary>
    /// Verifies usage exactly at the high ratio maps to <see cref="PressureLevel.High" />.
    /// </summary>
    [Fact]
    public void EvaluateReturnsHighAtExactHighThreshold()
    {
        var e = CreateEvaluator(
            new PressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        Assert.Equal(PressureLevel.High, e.Evaluate(800));
    }

    /// <summary>
    /// Verifies usage between high and critical ratios maps to <see cref="PressureLevel.High" />.
    /// </summary>
    [Fact]
    public void EvaluateReturnsHighBetweenThresholds()
    {
        var e = CreateEvaluator(
            new PressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        Assert.Equal(PressureLevel.High, e.Evaluate(900));
    }

    /// <summary>
    /// Verifies usage below the high ratio maps to <see cref="PressureLevel.Normal" />.
    /// </summary>
    [Fact]
    public void EvaluateReturnsNormalBelowHighThreshold()
    {
        var e = CreateEvaluator(
            new PressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        Assert.Equal(PressureLevel.Normal, e.Evaluate(799));
    }

    /// <summary>
    /// Verifies zero estimated usage maps to <see cref="PressureLevel.Normal" />.
    /// </summary>
    [Fact]
    public void EvaluateReturnsNormalForZeroUsage()
    {
        var e = CreateEvaluator(
            new PressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        Assert.Equal(PressureLevel.Normal, e.Evaluate(0));
    }

    private static StateEvaluator CreateEvaluator(PressureOptions options) => new(new MemoryPressureOptionsBinding(options));

    private sealed class MemoryPressureOptionsBinding : IOptions<PressureOptions>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryPressureOptionsBinding" /> class.
        /// </summary>
        /// <param name="value">Bound options value.</param>
        internal MemoryPressureOptionsBinding(PressureOptions value)
        {
            Value = value;
        }

        /// <inheritdoc />
        public PressureOptions Value { get; }
    }
}
