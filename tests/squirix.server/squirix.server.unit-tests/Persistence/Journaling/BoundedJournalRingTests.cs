using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>A wedged ring observes the pipeline failure on its poll slices instead of parking producers forever.</summary>
[Immutable]
public sealed class BoundedJournalRingTests
{
    /// <summary>A full ring invokes the failure poll on slice expiry and keeps its slot accounting.</summary>
    [Test]
    public async Task SlicePollObservesPipelineFailure()
    {
        using var ring = new BoundedJournalRing(1);
        await ring.EnqueueAsync(JournalWorkItem.Shutdown(), CancellationToken.None);

        var probe = new PipelineFailureProbe();
        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(ring.EnqueueAsync(JournalWorkItem.Shutdown(), CancellationToken.None, probe.Throw).AsTask());
        _ = await Assert.That(thrown).IsSameReferenceAs(probe.Reason);

        _ = await Assert.That(ring.TryDequeue(out var filler)).IsTrue();
        _ = await Assert.That(filler).IsNotNull();
        _ = await Assert.That(ring.TryDequeue(out _)).IsFalse();
    }

    /// <summary>NotifyWorkAvailable must not surface ObjectDisposedException after the ring is disposed.</summary>
    [Test]
    public async Task NotifySafeAfterDispose()
    {
        var ring = new BoundedJournalRing(4);
        ring.Dispose();

        Exception? thrown = null;
        try
        {
            ring.NotifyWorkAvailable();
        }
        catch (ObjectDisposedException ex)
        {
            thrown = ex;
        }

        _ = await Assert.That(thrown).IsNull();
    }

    /// <summary>Full enqueue/dequeue drain must keep slots reusable.</summary>
    [Test]
    public async Task EnqueueDequeueKeepsSlots()
    {
        using var ring = new BoundedJournalRing(2);
        var item = JournalWorkItem.Shutdown();

        await ring.EnqueueAsync(item, CancellationToken.None);
        _ = await Assert.That(ring.TryDequeue(out var first)).IsTrue();
        _ = await Assert.That(first).IsEqualTo(item);

        // Slots must be reusable after a full drain; a leaked slot would block this second round.
        var second = JournalWorkItem.Shutdown();
        await ring.EnqueueAsync(second, CancellationToken.None);
        _ = await Assert.That(ring.TryDequeue(out var third)).IsTrue();
        _ = await Assert.That(third).IsEqualTo(second);
    }

    private sealed class PipelineFailureProbe
    {
        internal InvalidOperationException Reason { get; } = new("pipeline failed");

        internal void Throw() => throw Reason;
    }
}
