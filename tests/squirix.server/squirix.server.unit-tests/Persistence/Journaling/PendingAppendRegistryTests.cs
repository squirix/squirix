using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Deterministic coverage for the pending-append drain race (issue #569): removal decides ownership,
/// so a drain racing an enqueue failure can never double-release the buffer or the counter slot.
/// </summary>
[Immutable]
public sealed class PendingAppendRegistryTests
{
    /// <summary>Abort tracking ignores the failure latch: the abort is diagnostics-only.</summary>
    [Test]
    public async Task AbortTrackingIgnoresLatch()
    {
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var abort = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.TrackAbort(abort);

        _ = registry.FailAll(new InvalidOperationException("pipeline failed"), NullLogger.Instance, counter);
        _ = await Assert.That(abort.Task.IsFaulted).IsTrue();

        var late = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.TrackAbort(late);
        var taken = registry.TakeAllAborts();
        var single = await Assert.That(taken).HasSingleItem();
        _ = await Assert.That(single).IsSameReferenceAs(late);
    }

    /// <summary>Draining marks tracked appends abandoned so staged batches are dropped, not written.</summary>
    [Test]
    public async Task AnyAbandonedTrueAfterDrain()
    {
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.Append(buffer, 64, ack);
        registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref counter.Value);

        var failure = new InvalidOperationException("pipeline failed");
        _ = registry.FailAll(failure, NullLogger.Instance, counter);

        _ = await Assert.That(registry.AnyAbandoned([item])).IsTrue();
        _ = await Assert.That(ack.Task.Exception?.InnerException).IsSameReferenceAs(failure);
        _ = await Assert.That(counter.Value).IsEqualTo(0);
        _ = await Assert.That(registry.ReturnQuarantinedBuffers()).IsEqualTo(1);
        _ = await Assert.That(registry.ReturnQuarantinedBuffers()).IsEqualTo(0);
    }

    /// <summary>Removing a maintenance ack decides the outcome: the caller cancels only when it wins.</summary>
    [Test]
    public async Task MaintenanceRemoveGateDecidesCancel()
    {
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.TrackMaintenance(ack);

        _ = await Assert.That(registry.RemoveMaintenance(ack)).IsTrue();
        _ = await Assert.That(registry.RemoveMaintenance(ack)).IsFalse();

        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.TrackMaintenance(pending);
        var failure = new InvalidOperationException("pipeline failed");
        _ = registry.FailAll(failure, NullLogger.Instance, counter);

        _ = await Assert.That(pending.Task.Exception?.InnerException).IsSameReferenceAs(failure);
        _ = await Assert.That(registry.RemoveMaintenance(pending)).IsFalse();
    }

    /// <summary>
    /// A drain racing a failed enqueue wins: the late untrack loses, nothing is released twice,
    /// and the quarantined buffer returns to the pool exactly once.
    /// </summary>
    [Test]
    public async Task TakeAllBeatsUntrackForCleanup()
    {
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.Append(buffer, 64, ack);
        registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref counter.Value);

        var failure = new InvalidOperationException("pipeline failed");
        var drained = registry.FailAll(failure, NullLogger.Instance, counter);

        _ = await Assert.That(drained).IsEqualTo(1);
        _ = await Assert.That(ack.Task.Exception?.InnerException).IsSameReferenceAs(failure);
        _ = await Assert.That(counter.Value).IsEqualTo(0);
        _ = await Assert.That(registry.IsAbandoned(item)).IsTrue();

        // The producer-side enqueue failure loses the race: it must release nothing.
        _ = await Assert.That(registry.Untrack(item, out _)).IsFalse();
        _ = await Assert.That(counter.Value).IsEqualTo(0);

        _ = await Assert.That(registry.QuarantinedCount).IsEqualTo(1);
        _ = await Assert.That(registry.ReturnQuarantinedBuffers()).IsEqualTo(1);
        _ = await Assert.That(registry.ReturnQuarantinedBuffers()).IsEqualTo(0);
    }

    /// <summary>Tracking after a drain fails fast with the latched failure.</summary>
    [Test]
    public async Task TrackAfterDrainFailsFast()
    {
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var failure = new InvalidOperationException("pipeline failed");
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var item = JournalWorkItem.Append(buffer, 64);
        registry.Track(item, buffer, 64, null);
        _ = Interlocked.Increment(ref counter.Value);
        _ = registry.FailAll(failure, NullLogger.Instance, counter);
        _ = await Assert.That(counter.Value).IsEqualTo(0);
        _ = await Assert.That(registry.ReturnQuarantinedBuffers()).IsEqualTo(1);

        var lateBuffer = ArrayPool<byte>.Shared.Rent(64);
        try
        {
            var late = JournalWorkItem.Append(lateBuffer, 64);
            var thrown = NodeExceptionAssert.For<InvalidOperationException>().Throws(
                (Registry: registry, Item: late, Buffer: lateBuffer),
                static state => state.Registry.Track(state.Item, state.Buffer, 64, null));

            _ = await Assert.That(thrown).IsSameReferenceAs(failure);
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(lateBuffer);
        }
    }

    /// <summary>Tracking maintenance after a drain fails fast with the latched failure.</summary>
    [Test]
    public async Task TrackMaintenanceAfterDrainFailsFast()
    {
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var failure = new InvalidOperationException("pipeline failed");
        _ = registry.FailAll(failure, NullLogger.Instance, counter);

        var late = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var thrown = NodeExceptionAssert.For<InvalidOperationException>().Throws((Registry: registry, Late: late), static state => state.Registry.TrackMaintenance(state.Late));

        _ = await Assert.That(thrown).IsSameReferenceAs(failure);
    }

    /// <summary>A completion racing a drain wins: the drain finds nothing, the ack stays owned by the thread.</summary>
    [Test]
    public async Task UntrackBeatsTakeAllForCleanup()
    {
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.Append(buffer, 64, ack);
        registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref counter.Value);

        _ = await Assert.That(registry.Untrack(item, out var entry)).IsTrue();
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry.FrameBytes).IsSameReferenceAs(buffer);
        _ = Interlocked.Decrement(ref counter.Value);
        ArrayPool<byte>.Shared.ReturnCleared(buffer);

        var drained = registry.FailAll(new InvalidOperationException("pipeline failed"), NullLogger.Instance, counter);

        _ = await Assert.That(drained).IsEqualTo(0);
        _ = await Assert.That(ack.Task.IsCompleted).IsFalse();
        _ = await Assert.That(counter.Value).IsEqualTo(0);
        _ = await Assert.That(registry.ReturnQuarantinedBuffers()).IsEqualTo(0);
    }
}
