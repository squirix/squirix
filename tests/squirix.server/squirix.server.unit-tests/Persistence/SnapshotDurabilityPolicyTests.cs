using Squirix.Server.Runtime;
using Squirix.Server.Storage.Snapshot;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Tests for <see cref="SnapshotDurabilityPolicy" />.</summary>
public sealed class SnapshotDurabilityPolicyTests
{
    private static readonly TriggerOptions DefaultTriggers = new ServerJsonSerializer().Deserialize<TriggerOptions>(
        """{"snapshotEveryNOps":250000,"snapshotEveryNBytes":134217728}""")!;

    /// <summary>Normal memory pressure never defers snapshots.</summary>
    [Fact]
    public void NormalPressureNeverDefers()
    {
        var defer = SnapshotDurabilityPolicy.ShouldDeferSnapshotUnderCriticalMemoryPressure(
            isCriticalMemoryPressure: false,
            true,
            0,
            0,
            DefaultTriggers);

        Assert.False(defer);
    }

    /// <summary>The bootstrap snapshot is not deferred under critical memory pressure.</summary>
    [Fact]
    public void BootstrapSnapshotNotDeferredUnderCriticalPressure()
    {
        var defer = SnapshotDurabilityPolicy.ShouldDeferSnapshotUnderCriticalMemoryPressure(
            isCriticalMemoryPressure: true,
            false,
            1_000_000,
            1_000_000_000,
            DefaultTriggers);

        Assert.False(defer);
    }

    /// <summary>Volume triggers remain eligible under critical memory pressure.</summary>
    [Fact]
    public void VolumeTriggeredSnapshotNotDeferredUnderCriticalPressure()
    {
        var defer = SnapshotDurabilityPolicy.ShouldDeferSnapshotUnderCriticalMemoryPressure(
            isCriticalMemoryPressure: true,
            true,
            0,
            bytesDelta: DefaultTriggers.SnapshotEveryNBytes,
            DefaultTriggers);

        Assert.False(defer);
    }

    /// <summary>Time-only snapshot attempts may be deferred under critical memory pressure.</summary>
    [Fact]
    public void TimeOnlySnapshotDeferredUnderCriticalPressure()
    {
        var defer = SnapshotDurabilityPolicy.ShouldDeferSnapshotUnderCriticalMemoryPressure(
            isCriticalMemoryPressure: true,
            true,
            1,
            1,
            DefaultTriggers);

        Assert.True(defer);
    }
}
