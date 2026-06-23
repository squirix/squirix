using System;
using Squirix.Server.Storage.Journaling;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Tests for the lazily allocated, configurable journal write-coalescing buffer.</summary>
public sealed class JournalWriteBatchBufferTests
{
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

    /// <summary>Staging copies the frame into the buffer and tracks the pending append.</summary>
    [Fact]
    public void TryStageAppendCopiesFrameAndTracksPending()
    {
        var buffer = new JournalWriteBatchBuffer(64);
        var frame = new byte[] { 1, 2, 3, 4 };

        Assert.True(buffer.TryStageAppend(MakeItem(frame)));

        Assert.False(buffer.IsEmpty);
        Assert.Equal(4, buffer.StagedByteLength);
        Assert.True(buffer.ActiveSpan.SequenceEqual(frame));
        _ = Assert.Single(buffer.PendingAppends);
    }

    /// <summary>A frame larger than the configured capacity is rejected so callers fall back to a direct write.</summary>
    [Fact]
    public void TryStageAppendRejectsFrameLargerThanCapacity()
    {
        var buffer = new JournalWriteBatchBuffer(8);

        Assert.False(buffer.TryStageAppend(MakeItem(new byte[16])));
        Assert.True(buffer.IsEmpty);
    }

    /// <summary>Clearing resets staged bytes and pending appends for reuse.</summary>
    [Fact]
    public void ClearResetsBuffer()
    {
        var buffer = new JournalWriteBatchBuffer(64);
        _ = buffer.TryStageAppend(MakeItem(new byte[] { 9, 9 }));

        buffer.Clear();

        Assert.True(buffer.IsEmpty);
        Assert.Equal(0, buffer.StagedByteLength);
        Assert.Empty(buffer.PendingAppends);
    }

    /// <summary>A non-positive capacity is rejected.</summary>
    [Fact]
    public void NonPositiveCapacityThrows() => Assert.Throws<ArgumentOutOfRangeException>(static () => new JournalWriteBatchBuffer(0));

    private static JournalWorkItem MakeItem(byte[] frame) => new()
    {
        Kind = JournalWorkKind.Append,
        FrameBytes = frame,
        FrameLength = frame.Length,
    };
}
