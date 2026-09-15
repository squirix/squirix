using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.Node.MemoryPressure;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Tests for <see cref="StateEvaluator" /> threshold boundaries.</summary>
[Immutable]
public sealed class PressureStateEvaluatorTests
{
    /// <summary>Verifies usage above the critical ratio maps to <see cref="PressureLevel.Critical" />.</summary>
    [Test]
    public async Task CriticalAboveCriticalThreshold()
    {
        var e = CreateEvaluator(
            new PressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        _ = await Assert.That(e.Evaluate(1000)).IsEqualTo(PressureLevel.Critical);
    }

    /// <summary>Verifies usage exactly at the critical ratio maps to <see cref="PressureLevel.Critical" />.</summary>
    [Test]
    public async Task CriticalAtExactCriticalThreshold()
    {
        var e = CreateEvaluator(
            new PressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        _ = await Assert.That(e.Evaluate(950)).IsEqualTo(PressureLevel.Critical);
    }

    /// <summary>Verifies usage exactly at the high ratio maps to <see cref="PressureLevel.High" />.</summary>
    [Test]
    public async Task EvaluateReturnsHighAtExactHighThreshold()
    {
        var e = CreateEvaluator(
            new PressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        _ = await Assert.That(e.Evaluate(800)).IsEqualTo(PressureLevel.High);
    }

    /// <summary>Verifies usage between high and critical ratios maps to <see cref="PressureLevel.High" />.</summary>
    [Test]
    public async Task EvaluateReturnsHighBetweenThresholds()
    {
        var e = CreateEvaluator(
            new PressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        _ = await Assert.That(e.Evaluate(900)).IsEqualTo(PressureLevel.High);
    }

    /// <summary>Verifies usage below the high ratio maps to <see cref="PressureLevel.Normal" />.</summary>
    [Test]
    public async Task EvaluateReturnsNormalBelowHighThreshold()
    {
        var e = CreateEvaluator(
            new PressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        _ = await Assert.That(e.Evaluate(799)).IsEqualTo(PressureLevel.Normal);
    }

    /// <summary>Verifies zero estimated usage maps to <see cref="PressureLevel.Normal" />.</summary>
    [Test]
    public async Task EvaluateReturnsNormalForZeroUsage()
    {
        var e = CreateEvaluator(
            new PressureOptions
            {
                MaxEstimatedCacheBytes = 1000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });

        _ = await Assert.That(e.Evaluate(0)).IsEqualTo(PressureLevel.Normal);
    }

    private static StateEvaluator CreateEvaluator(PressureOptions options) => new(new PressureOptionsBinding(options));

    [Immutable]
    private sealed class PressureOptionsBinding : IOptions<PressureOptions>
    {
        /// <summary>Initializes a new instance of the <see cref="PressureOptionsBinding" /> class.</summary>
        /// <param name="value">Bound options value.</param>
        internal PressureOptionsBinding(PressureOptions value)
        {
            Value = value;
        }

        /// <inheritdoc />
        public PressureOptions Value { get; }
    }
}
