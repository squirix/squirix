using System;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>
/// Tests for <see cref="PressureOptions" /> defaults and validation.
/// </summary>
public sealed class PressureOptionsTests
{
    /// <summary>Verifies non-positive byte limits are rejected.</summary>
    /// <param name="maxBytes">Invalid limit value.</param>
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-1000)]
    public static void ValidateRejectsNonPositiveMaxBytes(long maxBytes)
    {
        var options = new PressureOptions { MaxEstimatedCacheBytes = maxBytes };
        var ex = Assert.Throws<InvalidOperationException>(options.Validate);

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
    public void FieldBackedValidationAcceptsThresholdBoundaries()
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
        var o = new PressureOptions
        {
            MaxEstimatedCacheBytes = 1024,
            HighPressureThresholdPercent = 50,
            CriticalPressureThresholdPercent = 90,
        };
        o.Validate();
    }

    /// <summary>Verifies a critical threshold above 100 is rejected.</summary>
    [Fact]
    public void ValidateRejectsCriticalThresholdAboveOneHundred()
    {
        var options = new PressureOptions
        {
            MaxEstimatedCacheBytes = 1024,
            CriticalPressureThresholdPercent = 101,
        };
        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate());

        Assert.Contains(nameof(PressureOptions.CriticalPressureThresholdPercent), ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Verifies a critical threshold above 100 is rejected.</summary>
    [Fact]
    public void ValidateRejectsCriticalThresholdAboveOneHundred()
    {
        var options = new PressureOptions
        {
            MaxEstimatedCacheBytes = 1024,
            CriticalPressureThresholdPercent = 101,
        };
        var ex = Assert.Throws<InvalidOperationException>(options.Validate);

        Assert.Contains(nameof(PressureOptions.CriticalPressureThresholdPercent), ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Verifies the high threshold must be strictly less than the critical threshold.</summary>
    [Fact]
    public void ValidateRejectsHighNotStrictlyBelowCritical()
    {
        var o = new PressureOptions
        {
            MaxEstimatedCacheBytes = 1024,
            HighPressureThresholdPercent = 90,
            CriticalPressureThresholdPercent = 90,
        };
        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(o, static value => value.Validate());
        Assert.Contains("HighPressureThresholdPercent", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Verifies a non-positive high threshold is rejected.</summary>
    [Fact]
    public void ValidateRejectsHighThresholdOutOfRange()
    {
        var options = new PressureOptions
        {
            MaxEstimatedCacheBytes = 1024,
            HighPressureThresholdPercent = 0,
        };
        var ex = Assert.Throws<InvalidOperationException>(options.Validate);

        Assert.Contains(nameof(PressureOptions.HighPressureThresholdPercent), ex.Message, StringComparison.Ordinal);
    }

    private sealed class FixedMemoryBudgetProvider : IMemoryBudgetProvider
    {
        private readonly long _availableBytes;

        internal FixedMemoryBudgetProvider(long availableBytes) => _availableBytes = availableBytes;

        long IMemoryBudgetProvider.GetTotalAvailableBytes() => _availableBytes;
    }
}
