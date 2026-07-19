using System;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Tests for Pipelined journal segment roll capacity enforcement.</summary>
public sealed class JournalSegmentRollCapacityTests
{
    private const int OneMegabyte = 1024 * 1024;

    /// <summary>Total byte cap rejects an append that would exceed configured journal size.</summary>
    [Fact]
    public void EnsureAppendCapacityOrThrowThrowsWhenAppendExceedsTotalByteCap()
    {
        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxTotalBytesMb = 1 });
        var error = Assert.Throws<JournalCapacityExceededException>(() => policy.EnsureAppendCapacityOrThrow(OneMegabyte, 1));
        Assert.Contains("total bytes", error.Message, StringComparison.Ordinal);
    }

    /// <summary>Per-segment byte cap triggers roll before the next frame would overflow the active segment.</summary>
    [Fact]
    public void ShouldRollSegmentWhenIncomingFrameExceedsSegmentByteCap()
    {
        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxSegmentMb = 1 });
        Assert.True(policy.ShouldRollSegment(OneMegabyte, 1));
    }
}
