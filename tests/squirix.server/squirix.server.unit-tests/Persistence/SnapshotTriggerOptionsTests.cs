using System;
using Squirix.Server.Serialization;
using Squirix.Server.Storage.Snapshot;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>
/// Unit tests for <see cref="TriggerOptions" /> scalar validation.
/// </summary>
public sealed class SnapshotTriggerOptionsTests
{
    /// <summary>Verifies lower-bound scalar values remain accepted.</summary>
    [Fact]
    public void FieldBackedValidationAcceptsBoundaryScalars()
    {
        var options = new TriggerOptions
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

    /// <summary>Verifies invalid scalar values fail at assignment time.</summary>
    /// <param name="propertyName">Property being validated.</param>
    [Theory]
    [InlineData(nameof(TriggerOptions.SnapshotInterval))]
    [InlineData(nameof(TriggerOptions.SnapshotEveryNOps))]
    [InlineData(nameof(TriggerOptions.SnapshotEveryNBytes))]
    [InlineData(nameof(TriggerOptions.MinGapBetweenSnapshots))]
    [InlineData(nameof(TriggerOptions.JournalGrowthThrottleBytes))]
    [InlineData(nameof(TriggerOptions.LatencySloMilliseconds))]
    [InlineData(nameof(TriggerOptions.LatencyThrottleDuration))]
    public void FieldBackedValidationRejectsInvalidScalars(string propertyName)
    {
        ArgumentOutOfRangeException? ex = null;
        try
        {
            _ = CreateWithInvalidScalar(propertyName);
        }
        catch (ArgumentOutOfRangeException caught)
        {
            ex = caught;
        }

        Assert.NotNull(ex);
        Assert.Equal("value", ex.ParamName);
        Assert.Contains(propertyName, ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Verifies JSON binding still applies valid option values through init setters.</summary>
    [Fact]
    public void JsonDeserializeBindsValidatedScalars()
    {
        const string json =
            """{"enabled":true,"snapshotInterval":"00:03:00","snapshotEveryNOps":100,"snapshotEveryNBytes":2048,"minGapBetweenSnapshots":"00:00:05","journalGrowthThrottleBytes":1024,"latencySloMilliseconds":5.5,"latencyThrottleDuration":"00:00:02"}""";

        var options = new ServerJsonSerializer().Deserialize<TriggerOptions>(json);

        Assert.NotNull(options);
        Assert.Equal(TimeSpan.FromMinutes(3), options.SnapshotInterval);
        Assert.Equal(100, options.SnapshotEveryNOps);
        Assert.Equal(2048, options.SnapshotEveryNBytes);
        Assert.Equal(TimeSpan.FromSeconds(5), options.MinGapBetweenSnapshots);
        Assert.Equal(1024, options.JournalGrowthThrottleBytes);
        Assert.Equal(5.5, options.LatencySloMilliseconds);
        Assert.Equal(TimeSpan.FromSeconds(2), options.LatencyThrottleDuration);
    }

    private static TriggerOptions CreateWithInvalidScalar(string propertyName) => propertyName switch
    {
        nameof(TriggerOptions.SnapshotInterval) => new TriggerOptions { SnapshotInterval = TimeSpan.Zero },
        nameof(TriggerOptions.SnapshotEveryNOps) => new TriggerOptions { SnapshotEveryNOps = -1 },
        nameof(TriggerOptions.SnapshotEveryNBytes) => new TriggerOptions { SnapshotEveryNBytes = -1 },
        nameof(TriggerOptions.MinGapBetweenSnapshots) => new TriggerOptions { MinGapBetweenSnapshots = TimeSpan.FromTicks(-1) },
        nameof(TriggerOptions.JournalGrowthThrottleBytes) => new TriggerOptions { JournalGrowthThrottleBytes = -1 },
        nameof(TriggerOptions.LatencySloMilliseconds) => new TriggerOptions { LatencySloMilliseconds = double.NaN },
        nameof(TriggerOptions.LatencyThrottleDuration) => new TriggerOptions { LatencyThrottleDuration = TimeSpan.FromTicks(-1) },
        _ => throw new ArgumentOutOfRangeException(nameof(propertyName), propertyName, "Unsupported property name."),
    };
}
