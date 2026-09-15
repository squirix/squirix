using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>Unit tests for <see cref="TriggerOptions" /> scalar validation.</summary>
[Immutable]
public sealed class TriggerOptionsTests
{
    /// <summary>Verifies lower-bound scalar values remain accepted during JSON binding.</summary>
    [Test]
    public async Task FieldValidationAcceptsValidScalars()
    {
        const string json =
            """{"snapshotInterval":"00:00:00.0000001","snapshotEveryNOps":0,"snapshotEveryNBytes":0,"minGapBetweenSnapshots":"00:00:00","journalGrowthThrottleBytes":0,"latencySloMilliseconds":0,"latencyThrottleDuration":"00:00:00"}""";
        var options = new ServerJsonSerializer().Deserialize<TriggerOptions>(json);

        _ = await Assert.That(options).IsNotNull();
        _ = await Assert.That(options.SnapshotInterval).IsEqualTo(TimeSpan.FromTicks(1));
        _ = await Assert.That(options.SnapshotEveryNOps).IsEqualTo(0);
        _ = await Assert.That(options.SnapshotEveryNBytes).IsEqualTo(0);
        _ = await Assert.That(options.MinGapBetweenSnapshots).IsEqualTo(TimeSpan.Zero);
        _ = await Assert.That(options.JournalGrowthThrottleBytes).IsEqualTo(0);
        _ = await Assert.That(options.LatencySloMilliseconds).IsEqualTo(0);
        _ = await Assert.That(options.LatencyThrottleDuration).IsEqualTo(TimeSpan.Zero);
    }

    /// <summary>Verifies invalid scalar values fail during JSON binding.</summary>
    /// <param name="propertyName">Property being validated.</param>
    [Test]
    [Arguments(nameof(TriggerOptions.SnapshotInterval))]
    [Arguments(nameof(TriggerOptions.SnapshotEveryNOps))]
    [Arguments(nameof(TriggerOptions.SnapshotEveryNBytes))]
    [Arguments(nameof(TriggerOptions.MinGapBetweenSnapshots))]
    [Arguments(nameof(TriggerOptions.JournalGrowthThrottleBytes))]
    [Arguments(nameof(TriggerOptions.LatencySloMilliseconds))]
    [Arguments(nameof(TriggerOptions.LatencyThrottleDuration))]
    public async Task FieldValidationRejectsBadScalars(string propertyName)
    {
        var ex = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(
            propertyName,
            static value => new ServerJsonSerializer().Deserialize<TriggerOptions>(CreateInvalidJson(value)));
        _ = await Assert.That(ex.ParamName).IsEqualTo("value");
        _ = await Assert.That(ex.Message).Contains(propertyName, StringComparison.Ordinal);
    }

    /// <summary>Verifies JSON binding still applies valid option values through init setters.</summary>
    [Test]
    public async Task JsonDeserializeBindsValidatedScalars()
    {
        const string json =
            """{"enabled":true,"snapshotInterval":"00:03:00","snapshotEveryNOps":100,"snapshotEveryNBytes":2048,"minGapBetweenSnapshots":"00:00:05","journalGrowthThrottleBytes":1024,"latencySloMilliseconds":5.5,"latencyThrottleDuration":"00:00:02"}""";

        var options = new ServerJsonSerializer().Deserialize<TriggerOptions>(json);

        _ = await Assert.That(options).IsNotNull();
        _ = await Assert.That(options.SnapshotInterval).IsEqualTo(TimeSpan.FromMinutes(3));
        _ = await Assert.That(options.SnapshotEveryNOps).IsEqualTo(100);
        _ = await Assert.That(options.SnapshotEveryNBytes).IsEqualTo(2048);
        _ = await Assert.That(options.MinGapBetweenSnapshots).IsEqualTo(TimeSpan.FromSeconds(5));
        _ = await Assert.That(options.JournalGrowthThrottleBytes).IsEqualTo(1024);
        _ = await Assert.That(options.LatencySloMilliseconds).IsEqualTo(5.5);
        _ = await Assert.That(options.LatencyThrottleDuration).IsEqualTo(TimeSpan.FromSeconds(2));
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
