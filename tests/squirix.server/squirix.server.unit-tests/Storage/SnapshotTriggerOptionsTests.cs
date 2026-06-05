using System;
using System.Text.Json;
using Squirix.Server.Storage.Snapshot;
using Xunit;

namespace Squirix.Server.UnitTests.Storage;

/// <summary>
/// Unit tests for <see cref="SnapshotTriggerOptions" /> scalar validation.
/// </summary>
public sealed class SnapshotTriggerOptionsTests
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    /// <summary>
    /// Verifies lower-bound scalar values remain accepted.
    /// </summary>
    [Fact]
    public void FieldBackedValidationAcceptsBoundaryScalars()
    {
        var options = new SnapshotTriggerOptions
        {
            SnapshotInterval = TimeSpan.FromTicks(1),
            SnapshotEveryNOps = 0,
            SnapshotEveryNBytes = 0,
            MinGapBetweenSnapshots = TimeSpan.Zero,
            JournalGrowthThrottleBytes = 0,
            LatencySloMilliseconds = 0,
            LatencyThrottleDuration = TimeSpan.Zero,
        };

        Assert.Equal(TimeSpan.FromTicks(1), options.SnapshotInterval);
        Assert.Equal(0, options.SnapshotEveryNOps);
        Assert.Equal(0, options.SnapshotEveryNBytes);
        Assert.Equal(TimeSpan.Zero, options.MinGapBetweenSnapshots);
        Assert.Equal(0, options.JournalGrowthThrottleBytes);
        Assert.Equal(0, options.LatencySloMilliseconds);
        Assert.Equal(TimeSpan.Zero, options.LatencyThrottleDuration);
    }

    /// <summary>
    /// Verifies invalid scalar values fail at assignment time.
    /// </summary>
    /// <param name="propertyName">Property being validated.</param>
    [Theory]
    [InlineData(nameof(SnapshotTriggerOptions.SnapshotInterval))]
    [InlineData(nameof(SnapshotTriggerOptions.SnapshotEveryNOps))]
    [InlineData(nameof(SnapshotTriggerOptions.SnapshotEveryNBytes))]
    [InlineData(nameof(SnapshotTriggerOptions.MinGapBetweenSnapshots))]
    [InlineData(nameof(SnapshotTriggerOptions.JournalGrowthThrottleBytes))]
    [InlineData(nameof(SnapshotTriggerOptions.LatencySloMilliseconds))]
    [InlineData(nameof(SnapshotTriggerOptions.LatencyThrottleDuration))]
    public void FieldBackedValidationRejectsInvalidScalars(string propertyName)
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => CreateWithInvalidScalar(propertyName));

        Assert.Equal(propertyName, ex.ParamName);
        Assert.Contains(propertyName, ex.Message, StringComparison.Ordinal);
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
                              "snapshotInterval": "00:03:00",
                              "snapshotEveryNOps": 100,
                              "snapshotEveryNBytes": 2048,
                              "minGapBetweenSnapshots": "00:00:05",
                              "journalGrowthThrottleBytes": 1024,
                              "latencySloMilliseconds": 5.5,
                              "latencyThrottleDuration": "00:00:02"
                            }
                            """;

        var options = JsonSerializer.Deserialize<SnapshotTriggerOptions>(json, JsonOptions);

        Assert.NotNull(options);
        Assert.Equal(TimeSpan.FromMinutes(3), options.SnapshotInterval);
        Assert.Equal(100, options.SnapshotEveryNOps);
        Assert.Equal(2048, options.SnapshotEveryNBytes);
        Assert.Equal(TimeSpan.FromSeconds(5), options.MinGapBetweenSnapshots);
        Assert.Equal(1024, options.JournalGrowthThrottleBytes);
        Assert.Equal(5.5, options.LatencySloMilliseconds);
        Assert.Equal(TimeSpan.FromSeconds(2), options.LatencyThrottleDuration);
    }

    private static SnapshotTriggerOptions CreateWithInvalidScalar(string propertyName) => propertyName switch
    {
        nameof(SnapshotTriggerOptions.SnapshotInterval) => new SnapshotTriggerOptions { SnapshotInterval = TimeSpan.Zero },
        nameof(SnapshotTriggerOptions.SnapshotEveryNOps) => new SnapshotTriggerOptions { SnapshotEveryNOps = -1 },
        nameof(SnapshotTriggerOptions.SnapshotEveryNBytes) => new SnapshotTriggerOptions { SnapshotEveryNBytes = -1 },
        nameof(SnapshotTriggerOptions.MinGapBetweenSnapshots) => new SnapshotTriggerOptions { MinGapBetweenSnapshots = TimeSpan.FromTicks(-1) },
        nameof(SnapshotTriggerOptions.JournalGrowthThrottleBytes) => new SnapshotTriggerOptions { JournalGrowthThrottleBytes = -1 },
        nameof(SnapshotTriggerOptions.LatencySloMilliseconds) => new SnapshotTriggerOptions { LatencySloMilliseconds = double.NaN },
        nameof(SnapshotTriggerOptions.LatencyThrottleDuration) => new SnapshotTriggerOptions { LatencyThrottleDuration = TimeSpan.FromTicks(-1) },
        _ => throw new ArgumentOutOfRangeException(nameof(propertyName), propertyName, "Unsupported property name."),
    };
}
