using System;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>
/// Tests for <see cref="PressureOptions" /> defaults and validation.
/// </summary>
[Immutable]
public sealed class PressureOptionsTests
{
    /// <summary>Verifies invalid threshold combinations are rejected.</summary>
    /// <param name="critical">Critical threshold value.</param>
    /// <param name="high">High threshold value.</param>
    /// <param name="expectedMessageFragment">Expected validation detail fragment.</param>
    [Theory]
    [InlineData(101, 80, nameof(PressureOptions.CriticalPressureThresholdPercent))]
    [InlineData(90, 90, "HighPressureThresholdPercent")]
    [InlineData(90, 0, nameof(PressureOptions.HighPressureThresholdPercent))]
    public static void RejectsInvalidThresholdCombos(int critical, int high, string expectedMessageFragment)
    {
        var options = new PressureOptions
        {
            MaxEstimatedCacheBytes = 1024,
            HighPressureThresholdPercent = high,
            CriticalPressureThresholdPercent = critical,
        };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate());

        Assert.Contains(expectedMessageFragment, ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Verifies non-positive byte limits are rejected.</summary>
    /// <param name="maxBytes">Invalid limit value.</param>
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-1000)]
    public static void ValidateRejectsNonPositiveMaxBytes(long maxBytes)
    {
        var options = new PressureOptions { MaxEstimatedCacheBytes = maxBytes };
        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate());

        Assert.Contains(nameof(PressureOptions.MaxEstimatedCacheBytes), ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Verifies default threshold values match the contract.</summary>
    [Fact]
    public void DefaultsMatchContract()
    {
        var resolved = OptionsResolver.Resolve(new UnresolvedMemoryPressureOptions(), new FixedMemoryBudgetProvider(10_000));
        Assert.Equal(8_000L, resolved.MaxEstimatedCacheBytes);
        Assert.Equal(80, resolved.HighPressureThresholdPercent);
        Assert.Equal(95, resolved.CriticalPressureThresholdPercent);
    }

    /// <summary>Verifies local threshold boundaries remain accepted before cross-property validation runs.</summary>
    [Fact]
    public void FieldValidationAcceptsBoundaries()
    {
        var options = new PressureOptions
        {
            MaxEstimatedCacheBytes = 1,
            HighPressureThresholdPercent = 1,
            CriticalPressureThresholdPercent = 100,
        };

        options.Validate();
        Assert.Equal(1, options.MaxEstimatedCacheBytes);
        Assert.Equal(1, options.HighPressureThresholdPercent);
        Assert.Equal(100, options.CriticalPressureThresholdPercent);
    }

    /// <summary>Verifies JSON binding still applies valid option values through init setters.</summary>
    [Fact]
    public void JsonDeserializeBindsValidatedScalars()
    {
        const string json = """{"maxEstimatedCacheBytes":4096,"highPressureThresholdPercent":60,"criticalPressureThresholdPercent":90}""";
        var options = new ServerJsonSerializer().Deserialize<PressureOptions>(json);
        Assert.NotNull(options);
        options.Validate();
        Assert.Equal(4096, options.MaxEstimatedCacheBytes);
        Assert.Equal(60, options.HighPressureThresholdPercent);
        Assert.Equal(90, options.CriticalPressureThresholdPercent);
    }

    /// <summary>
    /// Verifies a representative valid configuration passes <see cref="PressureOptions.Validate" />.
    /// </summary>
    [Fact]
    public void ValidateAcceptsValidConfiguration()
    {
        var options = new PressureOptions
        {
            MaxEstimatedCacheBytes = 1024,
            HighPressureThresholdPercent = 50,
            CriticalPressureThresholdPercent = 90,
        };

        options.Validate();
    }
}
