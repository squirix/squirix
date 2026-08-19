using System;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>
/// Unit tests for <see cref="TriggerOptions" /> scalar validation.
/// </summary>
[Immutable]
public sealed class TriggerOptionsTests
{
    /// <summary>Verifies invalid scalar values fail during JSON binding.</summary>
    /// <param name="propertyName">Property being validated.</param>
    [Theory]
    [InlineData(nameof(TriggerOptions.SnapshotInterval))]
    [InlineData(nameof(TriggerOptions.SnapshotEveryNOps))]
    [InlineData(nameof(TriggerOptions.SnapshotEveryNBytes))]
    [InlineData(nameof(TriggerOptions.MinGapBetweenSnapshots))]
    [InlineData(nameof(TriggerOptions.JournalGrowthThrottleBytes))]
    [InlineData(nameof(TriggerOptions.LatencySloMilliseconds))]
    [InlineData(nameof(TriggerOptions.LatencyThrottleDuration))]
    public static void FieldBackedValidationRejectsInvalidScalars(string propertyName)
    {
        var ex = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(
            propertyName,
            static value => new ServerJsonSerializer().Deserialize<TriggerOptions>(CreateInvalidJson(value)));
        Assert.Equal("value", ex.ParamName);
        Assert.Contains(propertyName, ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Verifies lower-bound scalar values remain accepted during JSON binding.</summary>
    [Fact]
    public void FieldBackedValidationAcceptsBoundaryScalars()
    {
        const string json =
            """{"snapshotInterval":"00:00:00.0000001","snapshotEveryNOps":0,"snapshotEveryNBytes":0,"minGapBetweenSnapshots":"00:00:00","journalGrowthThrottleBytes":0,"latencySloMilliseconds":0,"latencyThrottleDuration":"00:00:00"}""";
        var options = new ServerJsonSerializer().Deserialize<TriggerOptions>(json);

        Assert.NotNull(options);
        Assert.Equal(TimeSpan.FromTicks(1), options.SnapshotInterval);
        Assert.Equal(0, options.SnapshotEveryNOps);
        Assert.Equal(0, options.SnapshotEveryNBytes);
        Assert.Equal(TimeSpan.Zero, options.MinGapBetweenSnapshots);
        Assert.Equal(0, options.JournalGrowthThrottleBytes);
        Assert.Equal(0, options.LatencySloMilliseconds);
        Assert.Equal(TimeSpan.Zero, options.LatencyThrottleDuration);
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

    private static string CreateInvalidJson(string propertyName) => propertyName switch
    {
        nameof(TriggerOptions.SnapshotInterval) => """{"snapshotInterval":"00:00:00"}""",
        nameof(TriggerOptions.SnapshotEveryNOps) => """{"snapshotEveryNOps":-1}""",
        nameof(TriggerOptions.SnapshotEveryNBytes) => """{"snapshotEveryNBytes":-1}""",
        nameof(TriggerOptions.MinGapBetweenSnapshots) => """{"minGapBetweenSnapshots":"-00:00:00.0000001"}""",
        nameof(TriggerOptions.JournalGrowthThrottleBytes) => """{"journalGrowthThrottleBytes":-1}""",
        nameof(TriggerOptions.LatencySloMilliseconds) => """{"latencySloMilliseconds":"NaN"}""",
        nameof(TriggerOptions.LatencyThrottleDuration) => """{"latencyThrottleDuration":"-00:00:00.0000001"}""",
        _ => throw new ArgumentOutOfRangeException(nameof(propertyName), propertyName, "Unsupported property name."),
    };
}
