using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Tests for journal disk pressure state evaluation used by health-ready details.</summary>
[Immutable]
public sealed class JournalDiskPressureStateTests : ServerUnitTestBase
{
    /// <summary>Verifies pressure labels for below high-water, high-water, and hard limit.</summary>
    [Test]
    public async Task PressureSpansHighWaterToHardLimit()
    {
        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxTotalBytesMb = 10 });
        var max = policy.MaxTotalBytes;
        var highWater = policy.HighWaterBytes;

        _ = await Assert.That(highWater).IsEqualTo(max * JournalSegmentLimits.HighWaterPercent / 100L);
        _ = await Assert.That(JournalSegmentPolicy.EvaluatePressureState(highWater - 1, highWater, max)).IsEqualTo("normal");
        _ = await Assert.That(JournalSegmentPolicy.EvaluatePressureState(highWater, highWater, max)).IsEqualTo("high");
        _ = await Assert.That(JournalSegmentPolicy.EvaluatePressureState(max, highWater, max)).IsEqualTo("critical");
        _ = await Assert.That(JournalSegmentPolicy.EvaluatePressureState(max + 1, highWater, max)).IsEqualTo("critical");
    }

    /// <summary>Verifies the high-water mark is reached by bytes or by segment count and not before.</summary>
    [Test]
    public async Task HighWaterIsReachedByBytesOrSegmentCount()
    {
        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxTotalBytesMb = 10, JournalMaxSegmentCount = 10 });
        var highWater = policy.HighWaterBytes;

        _ = await Assert.That(policy.IsAtHighWater(highWater - 1, 7)).IsFalse();
        _ = await Assert.That(policy.IsAtHighWater(highWater, 1)).IsTrue();
        _ = await Assert.That(policy.IsAtHighWater(0, 8)).IsTrue();
        _ = await Assert.That(policy.IsAtHighWater(0, 7)).IsFalse();
    }
}
