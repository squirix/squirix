using System;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Tests for the lazily allocated, configurable journal write-coalescing buffer.</summary>
public sealed class JournalWriteBatchBufferTests
{
    private static readonly byte[] TwoTabFrame = [0x09, 0x09];

    /// <summary>Clearing resets staged bytes and pending appends for reuse.</summary>
    [Fact]
    public void ClearResetsBuffer()
    {
        var buffer = new JournalWriteBatchBuffer(64);
        _ = buffer.TryStageAppend(MakeItem(TwoTabFrame));

        buffer.Clear();

        Assert.True(buffer.IsEmpty);
        Assert.Equal(0, buffer.StagedByteLength);
        Assert.Empty(buffer.PendingAppends);
    }

    /// <summary>A freshly constructed buffer is empty and exposes no staged bytes.</summary>
    [Fact]
    public void NewBufferIsEmpty()
    {
        var buffer = new JournalWriteBatchBuffer();

        Assert.True(buffer.IsEmpty);
        Assert.Equal(0, buffer.StagedByteLength);
        Assert.True(buffer.ActiveSpan.IsEmpty);
        Assert.Empty(buffer.PendingAppends);
    }

    /// <summary>A non-positive capacity is rejected.</summary>
    [Fact]
    public void NonPositiveCapacityThrows() => _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(0, static value => _ = new JournalWriteBatchBuffer(value));

    /// <summary>Staging copies the frame into the buffer and tracks the pending append.</summary>
    [Fact]
    public void StageAppendCopiesFrameAndTracksPending()
    {
        var buffer = new JournalWriteBatchBuffer(64);
        byte[] frame = [1, 2, 3, 4];

        Assert.True(buffer.TryStageAppend(MakeItem(frame)));

        Assert.False(buffer.IsEmpty);
        Assert.Equal(4, buffer.StagedByteLength);
        Assert.True(buffer.ActiveSpan.SequenceEqual(frame));
        _ = Assert.Single(buffer.PendingAppends);
    }

    /// <summary>A frame larger than the configured capacity is rejected so callers fall back to a direct write.</summary>
    [Fact]
    public void StageAppendRejectsFrameLargerThanCapacity()
    {
        var buffer = new JournalWriteBatchBuffer(8);

        Assert.False(buffer.TryStageAppend(MakeItem(new byte[16])));
        Assert.True(buffer.IsEmpty);
    }

    private static JournalWorkItem MakeItem(byte[] frame) => new(JournalWorkKind.Append, frameBytes: frame, frameLength: frame.Length);
}
