using System;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Tests for Pipelined journal segment roll capacity enforcement.</summary>
[Immutable]
public sealed class JournalSegmentRollCapacityTests
{
    private const int OneMegabyte = 1024 * 1024;

    /// <summary>Total byte cap rejects an append that would exceed configured journal size.</summary>
    [Fact]
    public void AppendCapThrowsPastTotalByteLimit()
    {
        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxTotalBytesMb = 1 });
        var error = NodeExceptionAssert.For<JournalCapacityExceededException>().Throws(policy, static value => value.EnsureAppendCapacityOrThrow(OneMegabyte, 1));
        Assert.Contains("total bytes", error.Message, StringComparison.Ordinal);
    }

    /// <summary>Roll is rejected when the next segment would exceed the configured segment-count limit.</summary>
    [Fact]
    public void RollThrowsPastSegmentCountLimit()
    {
        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxSegmentCount = 2, JournalMaxTotalBytesMb = 64 });
        var error = NodeExceptionAssert.For<JournalCapacityExceededException>().Throws(policy, static value => value.EnsureRollCapacityOrThrow(2, 0));
        Assert.Contains("segment count", error.Message, StringComparison.Ordinal);
    }

    /// <summary>Roll is rejected when on-disk total bytes already exceed the configured journal size.</summary>
    [Fact]
    public void RollThrowsPastTotalByteLimit()
    {
        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxTotalBytesMb = 1, JournalMaxSegmentCount = 32 });
        var error = NodeExceptionAssert.For<JournalCapacityExceededException>().Throws(policy, static value => value.EnsureRollCapacityOrThrow(1, OneMegabyte + 1));
        Assert.Contains("total bytes", error.Message, StringComparison.Ordinal);
    }

    /// <summary>Per-segment byte cap triggers roll before the next frame would overflow the active segment.</summary>
    [Fact]
    public void RollTriggersPastSegmentByteCap()
    {
        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxSegmentMb = 1 });
        Assert.True(policy.ShouldRollSegment(OneMegabyte, 1));
    }
}
