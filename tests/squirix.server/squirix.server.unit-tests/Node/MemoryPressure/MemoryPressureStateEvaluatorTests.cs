using System;
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
    public void AboveCriticalThresholdIsCritical()
    {
        var e = CreateEvaluator(
            new MemoryPressureOptions
            {
                Enabled = true,
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });
        Assert.Equal(MemoryPressureState.Critical, e.Evaluate(1000));
    }

    /// <summary>
    /// Verifies usage below the high ratio maps to <see cref="MemoryPressureState.Normal" />.
    /// </summary>
    [Fact]
    public void BelowHighThresholdIsNormal()
    {
        var e = CreateEvaluator(
            new MemoryPressureOptions
            {
                Enabled = true,
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });
        Assert.Equal(MemoryPressureState.Normal, e.Evaluate(799));
    }

    /// <summary>
    /// Verifies usage between high and critical ratios maps to <see cref="MemoryPressureState.High" />.
    /// </summary>
    [Fact]
    public void BetweenHighAndCriticalIsHigh()
    {
        var e = CreateEvaluator(
            new MemoryPressureOptions
            {
                Enabled = true,
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });
        Assert.Equal(MemoryPressureState.High, e.Evaluate(900));
    }

    /// <summary>
    /// Verifies disabled options always yield <see cref="MemoryPressureState.Normal" />.
    /// </summary>
    [Fact]
    public void DisabledOptionsAlwaysNormal()
    {
        var e = CreateEvaluator(
            new MemoryPressureOptions
            {
                Enabled = false,
                MaxEstimatedCacheBytes = 100,
                HighPressureThresholdPercent = 1,
                CriticalPressureThresholdPercent = 2,
            });
        Assert.Equal(MemoryPressureState.Normal, e.Evaluate(99));
    }

    /// <summary>
    /// Verifies usage exactly at the critical ratio maps to <see cref="MemoryPressureState.Critical" />.
    /// </summary>
    [Fact]
    public void ExactlyAtCriticalThresholdIsCritical()
    {
        var e = CreateEvaluator(
            new MemoryPressureOptions
            {
                Enabled = true,
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
    public void ExactlyAtHighThresholdIsHigh()
    {
        var e = CreateEvaluator(
            new MemoryPressureOptions
            {
                Enabled = true,
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });
        Assert.Equal(MemoryPressureState.High, e.Evaluate(800));
    }

    /// <summary>
    /// Verifies negative estimated usage throws.
    /// </summary>
    [Fact]
    public void NegativeUsageThrows()
    {
        var e = CreateEvaluator(
            new MemoryPressureOptions
            {
                Enabled = true,
                MaxEstimatedCacheBytes = 100,
                HighPressureThresholdPercent = 50,
                CriticalPressureThresholdPercent = 90,
            });
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => e.Evaluate(-1));
    }

    /// <summary>
    /// Verifies no configured positive limit always yields <see cref="MemoryPressureState.Normal" />.
    /// </summary>
    [Fact]
    public void NoConfiguredLimitAlwaysNormal()
    {
        var e = CreateEvaluator(
            new MemoryPressureOptions
            {
                Enabled = true,
                MaxEstimatedCacheBytes = null,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });
        Assert.Equal(MemoryPressureState.Normal, e.Evaluate(1_000_000));
    }

    /// <summary>
    /// Verifies zero estimated usage maps to <see cref="MemoryPressureState.Normal" />.
    /// </summary>
    [Fact]
    public void ZeroUsageIsNormal()
    {
        var e = CreateEvaluator(
            new MemoryPressureOptions
            {
                Enabled = true,
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
        /// <param name="value">The options snapshot.</param>
        public MemoryPressureOptionsBinding(MemoryPressureOptions value)
        {
            Value = value;
        }

        /// <inheritdoc />
        public MemoryPressureOptions Value { get; }
    }
}
