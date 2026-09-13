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
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Deterministic coverage for the pending-append drain race (issue #569): removal decides ownership,
/// so a drain racing an enqueue failure can never double-release the buffer or the counter slot.
/// </summary>
[Immutable]
public sealed class PendingAppendRegistryTests
{
    /// <summary>
    /// A drain racing a failed enqueue wins: the late untrack loses, nothing is released twice,
    /// and the quarantined buffer returns to the pool exactly once.
    /// </summary>
    [Fact]
    public void TakeAllBeatsUntrackForCleanup()
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

        Assert.Equal(1, drained);
        Assert.Same(failure, ack.Task.Exception?.InnerException);
        Assert.Equal(0, counter.Value);
        Assert.True(registry.IsAbandoned(item));

        // The producer-side enqueue failure loses the race: it must release nothing.
        Assert.False(registry.Untrack(item, out _));
        Assert.Equal(0, counter.Value);

        Assert.Equal(1, registry.QuarantinedCount);
        Assert.Equal(1, registry.ReturnQuarantinedBuffers());
        Assert.Equal(0, registry.ReturnQuarantinedBuffers());
    }

    /// <summary>A completion racing a drain wins: the drain finds nothing, the ack stays owned by the thread.</summary>
    [Fact]
    public void UntrackBeatsTakeAllForCleanup()
    {
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.Append(buffer, 64, ack);
        registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref counter.Value);

        Assert.True(registry.Untrack(item, out var entry));
        Assert.NotNull(entry);
        Assert.Same(buffer, entry.FrameBytes);
        _ = Interlocked.Decrement(ref counter.Value);
        ArrayPool<byte>.Shared.ReturnCleared(buffer);

        var drained = registry.FailAll(new InvalidOperationException("pipeline failed"), NullLogger.Instance, counter);

        Assert.Equal(0, drained);
        Assert.False(ack.Task.IsCompleted);
        Assert.Equal(0, counter.Value);
        Assert.Equal(0, registry.ReturnQuarantinedBuffers());
    }

    /// <summary>Tracking after a drain fails fast with the latched failure.</summary>
    [Fact]
    public void TrackAfterDrainFailsFast()
    {
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var failure = new InvalidOperationException("pipeline failed");
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var item = JournalWorkItem.Append(buffer, 64);
        registry.Track(item, buffer, 64, null);
        _ = Interlocked.Increment(ref counter.Value);
        _ = registry.FailAll(failure, NullLogger.Instance, counter);
        Assert.Equal(0, counter.Value);
        Assert.Equal(1, registry.ReturnQuarantinedBuffers());

        var lateBuffer = ArrayPool<byte>.Shared.Rent(64);
        try
        {
            var late = JournalWorkItem.Append(lateBuffer, 64);
            var thrown = NodeExceptionAssert.For<InvalidOperationException>().Throws(
                (Registry: registry, Item: late, Buffer: lateBuffer),
                static state => state.Registry.Track(state.Item, state.Buffer, 64, null));

            Assert.Same(failure, thrown);
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(lateBuffer);
        }
    }

    /// <summary>Abort tracking ignores the failure latch: the abort is diagnostics-only.</summary>
    [Fact]
    public void AbortTrackingIgnoresLatch()
    {
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var abort = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.TrackAbort(abort);

        _ = registry.FailAll(new InvalidOperationException("pipeline failed"), NullLogger.Instance, counter);
        Assert.True(abort.Task.IsFaulted);

        var late = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.TrackAbort(late);
        var taken = registry.TakeAllAborts();
        var single = Assert.Single(taken);
        Assert.Same(late, single);
    }

    /// <summary>Removing a maintenance ack decides the outcome: the caller cancels only when it wins.</summary>
    [Fact]
    public void MaintenanceRemoveGateDecidesCancel()
    {
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.TrackMaintenance(ack);

        Assert.True(registry.RemoveMaintenance(ack));
        Assert.False(registry.RemoveMaintenance(ack));

        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.TrackMaintenance(pending);
        var failure = new InvalidOperationException("pipeline failed");
        _ = registry.FailAll(failure, NullLogger.Instance, counter);

        Assert.Same(failure, pending.Task.Exception?.InnerException);
        Assert.False(registry.RemoveMaintenance(pending));
    }
}
