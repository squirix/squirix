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
    private const int CapacityBytes = 1024 * 1024;
    private const int InitialAllocationBytes = 64 * 1024;

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

    /// <summary>The first staged append allocates the small initial array, not the configured capacity.</summary>
    [Test]
    public async Task FirstAppendAllocatesSmallArray()
    {
        var buffer = new JournalWriteBatchBuffer(CapacityBytes);

        _ = await Assert.That(buffer.AllocatedLength).IsEqualTo(0);
        _ = await Assert.That(buffer.TryStageAppend(MakeItem(MakeFrame(10, 1)))).IsTrue();

        _ = await Assert.That(buffer.AllocatedLength).IsEqualTo(InitialAllocationBytes);
    }

    /// <summary>A capacity below the initial size allocates exactly the capacity.</summary>
    [Test]
    public async Task SmallCapacityAllocatesCapacity()
    {
        var buffer = new JournalWriteBatchBuffer(100);

        _ = await Assert.That(buffer.TryStageAppend(MakeItem(MakeFrame(10, 1)))).IsTrue();

        _ = await Assert.That(buffer.AllocatedLength).IsEqualTo(100);
    }

    /// <summary>Growth across several doublings preserves staged bytes exactly and never exceeds the capacity.</summary>
    [Test]
    public async Task GrowthPreservesStagedBytes()
    {
        var buffer = new JournalWriteBatchBuffer(CapacityBytes);
        const int frameLength = 10_000;
        const int frameCount = 40;
        var expected = new byte[frameLength * frameCount];
        var previousLength = 0;
        for (var i = 0; i < frameCount; i++)
        {
            var frame = MakeFrame(frameLength, i);
            _ = await Assert.That(buffer.TryStageAppend(MakeItem(frame))).IsTrue();
            frame.CopyTo(expected, i * frameLength);

            _ = await Assert.That(buffer.AllocatedLength).IsGreaterThanOrEqualTo(previousLength);
            _ = await Assert.That(buffer.AllocatedLength).IsLessThanOrEqualTo(CapacityBytes);
            previousLength = buffer.AllocatedLength;
            _ = await Assert.That(buffer.ActiveSpan.SequenceEqual(expected.AsSpan(0, (i + 1) * frameLength))).IsTrue();
        }

        _ = await Assert.That(previousLength).IsEqualTo(InitialAllocationBytes * 8);
        _ = await Assert.That(buffer.StagedByteLength).IsEqualTo(frameLength * frameCount);
    }

    /// <summary>A frame that needs more than a doubling grows straight to the needed size.</summary>
    [Test]
    public async Task GrowthReachesNeededSizeBeyondDoubling()
    {
        var buffer = new JournalWriteBatchBuffer(CapacityBytes);
        _ = buffer.TryStageAppend(MakeItem(MakeFrame(10, 1)));
        var frame = MakeFrame(InitialAllocationBytes * 3, 2);

        _ = await Assert.That(buffer.TryStageAppend(MakeItem(frame))).IsTrue();

        _ = await Assert.That(buffer.AllocatedLength).IsEqualTo(10 + frame.Length);
        _ = await Assert.That(buffer.StagedByteLength).IsEqualTo(10 + frame.Length);
    }

    /// <summary>Growth is capped at the configured capacity, which can be filled exactly.</summary>
    [Test]
    public async Task GrowthStopsAtCapacity()
    {
        var buffer = new JournalWriteBatchBuffer(CapacityBytes);

        _ = await Assert.That(buffer.TryStageAppend(MakeItem(MakeFrame(CapacityBytes - 1, 1)))).IsTrue();
        _ = await Assert.That(buffer.TryStageAppend(MakeItem(MakeFrame(1, 2)))).IsTrue();
        _ = await Assert.That(buffer.TryStageAppend(MakeItem(MakeFrame(1, 3)))).IsFalse();

        _ = await Assert.That(buffer.AllocatedLength).IsEqualTo(CapacityBytes);
        _ = await Assert.That(buffer.StagedByteLength).IsEqualTo(CapacityBytes);
    }

    /// <summary>A frame above the capacity is rejected without allocating or growing.</summary>
    [Test]
    public async Task OversizedFrameDoesNotAllocateOrGrow()
    {
        var buffer = new JournalWriteBatchBuffer(CapacityBytes);

        _ = await Assert.That(buffer.TryStageAppend(MakeItem(MakeFrame(CapacityBytes + 1, 1)))).IsFalse();
        _ = await Assert.That(buffer.AllocatedLength).IsEqualTo(0);

        _ = await Assert.That(buffer.TryStageAppend(MakeItem(MakeFrame(10, 2)))).IsTrue();
        _ = await Assert.That(buffer.TryStageAppend(MakeItem(MakeFrame(CapacityBytes, 3)))).IsFalse();
        _ = await Assert.That(buffer.AllocatedLength).IsEqualTo(InitialAllocationBytes);
        _ = await Assert.That(buffer.StagedByteLength).IsEqualTo(10);
    }

    /// <summary>Clearing keeps the grown array and later appends reuse it.</summary>
    [Test]
    public async Task ClearKeepsGrownArray()
    {
        var buffer = new JournalWriteBatchBuffer(CapacityBytes);
        _ = buffer.TryStageAppend(MakeItem(MakeFrame(InitialAllocationBytes + 1, 1)));
        var grownLength = buffer.AllocatedLength;

        buffer.Clear();
        _ = buffer.TryStageAppend(MakeItem(MakeFrame(5, 2)));

        _ = await Assert.That(buffer.AllocatedLength).IsEqualTo(grownLength);
    }

    /// <summary>Batch boundaries and flushed bytes match a capacity-only reference model for mixed frame sizes.</summary>
    [Test]
    public async Task BatchBoundariesMatchCapacityOnlyModel()
    {
        const int capacity = 200_000;
        int[] sizes = [1, 70_000, 5, 130_000, 60_000, 64 * 1024, 199_999, 3, 100_000, 100_000, 100_001, 50_000, 7, 150_000, 49_999, 1];
        var buffer = new JournalWriteBatchBuffer(capacity);
        var model = new byte[capacity];
        var modelLength = 0;
        var flushes = 0;

        for (var i = 0; i < sizes.Length; i++)
        {
            var frame = MakeFrame(sizes[i], i + 1);
            var modelFlushes = modelLength + frame.Length > capacity;
            var staged = buffer.TryStageAppend(MakeItem(frame));

            _ = await Assert.That(staged).IsEqualTo(!modelFlushes);
            if (modelFlushes)
            {
                _ = await Assert.That(buffer.ActiveSpan.SequenceEqual(model.AsSpan(0, modelLength))).IsTrue();
                buffer.Clear();
                modelLength = 0;
                flushes++;
                _ = await Assert.That(buffer.TryStageAppend(MakeItem(frame))).IsTrue();
            }

            frame.CopyTo(model, modelLength);
            modelLength += frame.Length;
            _ = await Assert.That(buffer.StagedByteLength).IsEqualTo(modelLength);
        }

        _ = await Assert.That(buffer.ActiveSpan.SequenceEqual(model.AsSpan(0, modelLength))).IsTrue();
        _ = await Assert.That(flushes).IsGreaterThan(3);
    }

    private static byte[] MakeFrame(int length, int seed)
    {
        var frame = new byte[length];
        for (var i = 0; i < length; i++)
            frame[i] = byte.CreateTruncating(seed + (i * 31));

        return frame;
    }

    private static JournalWorkItem MakeItem(byte[] frame) => JournalWorkItem.Append(frame, frame.Length);
}
