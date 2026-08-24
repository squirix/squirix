using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Tests for journal disk pressure state evaluation used by health-ready details.</summary>
[Immutable]
public sealed class JournalDiskPressureStateTests : ServerUnitTestBase
{
    /// <summary>Verifies pressure labels for below high-water, high-water, and hard limit.</summary>
    [Fact]
    public void PressureSpansHighWaterToHardLimit()
    {
        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxTotalBytesMb = 10 });
        var max = policy.MaxTotalBytes;
        var highWater = policy.HighWaterBytes;

        Assert.Equal(max * JournalSegmentLimits.HighWaterPercent / 100L, highWater);
        Assert.Equal("normal", JournalSegmentPolicy.EvaluatePressureState(highWater - 1, highWater, max));
        Assert.Equal("high", JournalSegmentPolicy.EvaluatePressureState(highWater, highWater, max));
        Assert.Equal("critical", JournalSegmentPolicy.EvaluatePressureState(max, highWater, max));
        Assert.Equal("critical", JournalSegmentPolicy.EvaluatePressureState(max + 1, highWater, max));
    }
}
