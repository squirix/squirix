using System;
using System.Globalization;
using System.Text.Json;
using Squirix.Server.Node.MemoryPressure;
using Xunit;

namespace Squirix.Server.UnitTests.Node.MemoryPressure;

/// <summary>
/// Tests for <see cref="MemoryPressureOptions" /> defaults and validation.
/// </summary>
public sealed class MemoryPressureOptionsTests
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    /// <summary>
    /// Verifies default option values match the v0.7.1 contract.
    /// </summary>
    [Fact]
    public void DefaultsMatchContract()
    {
        var o = new MemoryPressureOptions();
        Assert.False(o.Enabled);
        Assert.Null(o.MaxEstimatedCacheBytes);
        Assert.Equal(80, o.HighPressureThresholdPercent);
        Assert.Equal(95, o.CriticalPressureThresholdPercent);
        Assert.True(o.RejectWritesOnCriticalPressure);
    }

    /// <summary>
    /// Verifies local threshold boundaries remain accepted before cross-property validation runs.
    /// </summary>
    [Fact]
    public void FieldBackedValidationAcceptsThresholdBoundaries()
    {
        var options = new MemoryPressureOptions
        {
            MaxEstimatedCacheBytes = 0,
            HighPressureThresholdPercent = 1,
            CriticalPressureThresholdPercent = 100,
        };

        options.Validate();
        Assert.Equal(0, options.MaxEstimatedCacheBytes);
        Assert.Equal(1, options.HighPressureThresholdPercent);
        Assert.Equal(100, options.CriticalPressureThresholdPercent);
    }

    /// <summary>
    /// Verifies a critical threshold above 100 is rejected.
    /// </summary>
    [Fact]
    public void FieldBackedValidationRejectsCriticalThresholdAboveOneHundred()
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(static () => new MemoryPressureOptions { CriticalPressureThresholdPercent = 101 });

        Assert.Equal(nameof(MemoryPressureOptions.CriticalPressureThresholdPercent), ex.ParamName);
        Assert.Contains("CriticalPressureThresholdPercent", ex.Message, StringComparison.Ordinal);
        Assert.Contains("101", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies a non-positive high threshold is rejected.
    /// </summary>
    [Fact]
    public void FieldBackedValidationRejectsHighThresholdOutOfRange()
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(static () => new MemoryPressureOptions { HighPressureThresholdPercent = 0 });

        Assert.Equal(nameof(MemoryPressureOptions.HighPressureThresholdPercent), ex.ParamName);
        Assert.Contains("HighPressureThresholdPercent", ex.Message, StringComparison.Ordinal);
        Assert.Contains("0", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies negative byte limits are rejected.
    /// </summary>
    /// <param name="maxBytes">Invalid limit value.</param>
    [Theory]
    [InlineData(-1)]
    [InlineData(-1000)]
    public void FieldBackedValidationRejectsNegativeMaxBytes(long maxBytes)
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => new MemoryPressureOptions { MaxEstimatedCacheBytes = maxBytes });

        Assert.Equal(nameof(MemoryPressureOptions.MaxEstimatedCacheBytes), ex.ParamName);
        Assert.Contains("MaxEstimatedCacheBytes", ex.Message, StringComparison.Ordinal);
        Assert.Contains(maxBytes.ToString(CultureInfo.InvariantCulture), ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies JSON binding still applies valid option values through init setters.
    /// </summary>
    [Fact]
    public void JsonDeserializeBindsValidatedScalars()
    {
        const string json = """
                            {
                              "enabled": true,
                              "maxEstimatedCacheBytes": 4096,
                              "highPressureThresholdPercent": 60,
                              "criticalPressureThresholdPercent": 90,
                              "rejectWritesOnCriticalPressure": false
                            }
                            """;

        var options = JsonSerializer.Deserialize<MemoryPressureOptions>(json, JsonOptions);

        Assert.NotNull(options);
        options.Validate();
        Assert.True(options.Enabled);
        Assert.Equal(4096, options.MaxEstimatedCacheBytes);
        Assert.Equal(60, options.HighPressureThresholdPercent);
        Assert.Equal(90, options.CriticalPressureThresholdPercent);
        Assert.False(options.RejectWritesOnCriticalPressure);
    }

    /// <summary>
    /// Verifies a representative valid configuration passes <see cref="MemoryPressureOptions.Validate" />.
    /// </summary>
    [Fact]
    public void ValidateAcceptsValidConfiguration()
    {
        var o = new MemoryPressureOptions
        {
            Enabled = true,
            MaxEstimatedCacheBytes = 1024,
            HighPressureThresholdPercent = 50,
            CriticalPressureThresholdPercent = 90,
        };
        var ex = Record.Exception(o.Validate);
        Assert.Null(ex);
    }

    /// <summary>
    /// Verifies the high threshold must be strictly less than the critical threshold.
    /// </summary>
    [Fact]
    public void ValidateRejectsHighNotStrictlyBelowCritical()
    {
        var o = new MemoryPressureOptions
        {
            HighPressureThresholdPercent = 90,
            CriticalPressureThresholdPercent = 90,
        };
        var ex = Assert.Throws<InvalidOperationException>(o.Validate);
        Assert.Contains("HighPressureThresholdPercent", ex.Message, StringComparison.Ordinal);
    }
}
