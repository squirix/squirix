using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Tests for the lazily allocated, configurable journal write-coalescing buffer.</summary>
[Immutable]
public sealed class JournalWriteBatchBufferTests
{
    private static readonly byte[] TwoTabFrame = [0x09, 0x09];

    /// <summary>Clearing resets staged bytes and pending appends for reuse.</summary>
    [Test]
    public async Task ClearResetsBuffer()
    {
        var buffer = new JournalWriteBatchBuffer(64);
        _ = buffer.TryStageAppend(MakeItem(TwoTabFrame));

        buffer.Clear();

        _ = await Assert.That(buffer.IsEmpty).IsTrue();
        _ = await Assert.That(buffer.StagedByteLength).IsEqualTo(0);
        _ = await Assert.That(buffer.PendingAppends).IsEmpty();
    }

    /// <summary>A freshly constructed buffer is empty and exposes no staged bytes.</summary>
    [Test]
    public async Task NewBufferIsEmpty()
    {
        var buffer = new JournalWriteBatchBuffer();

        _ = await Assert.That(buffer.IsEmpty).IsTrue();
        _ = await Assert.That(buffer.StagedByteLength).IsEqualTo(0);
        _ = await Assert.That(buffer.ActiveSpan.IsEmpty).IsTrue();
        _ = await Assert.That(buffer.PendingAppends).IsEmpty();
    }

    /// <summary>A non-positive capacity is rejected.</summary>
    [Test]
    public void NonPositiveCapacityThrows() => _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(0, static value => _ = new JournalWriteBatchBuffer(value));

    /// <summary>Staging copies the frame into the buffer and tracks the pending append.</summary>
    [Test]
    public async Task StageAppendCopiesFrameAndTracksPending()
    {
        var buffer = new JournalWriteBatchBuffer(64);
        byte[] frame = [1, 2, 3, 4];

        _ = await Assert.That(buffer.TryStageAppend(MakeItem(frame))).IsTrue();

        _ = await Assert.That(buffer.IsEmpty).IsFalse();
        _ = await Assert.That(buffer.StagedByteLength).IsEqualTo(4);
        _ = await Assert.That(buffer.ActiveSpan.SequenceEqual(frame)).IsTrue();
        _ = await Assert.That(buffer.PendingAppends).HasSingleItem();
    }

    /// <summary>A frame larger than the configured capacity is rejected so callers fall back to a direct write.</summary>
    [Test]
    public async Task StageAppendRejectsOversizedFrame()
    {
        var buffer = new JournalWriteBatchBuffer(8);

        _ = await Assert.That(buffer.TryStageAppend(MakeItem(new byte[16]))).IsFalse();
        _ = await Assert.That(buffer.IsEmpty).IsTrue();
    }

    private static JournalWorkItem MakeItem(byte[] frame) => JournalWorkItem.Append(frame, frame.Length);
}
