using System;
using System.Threading.Tasks;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Regression coverage for BoundedJournalRing admission and teardown-race signaling.</summary>
public sealed class BoundedJournalRingTests : ServerUnitTestBase
{
    /// <summary>NotifyWorkAvailable must not surface ObjectDisposedException after the ring is disposed.</summary>
    [Fact]
    public async Task NotifySafeAfterDispose()
    {
        using var ring = new BoundedJournalRing(4);
        ring.Dispose();

        ObjectDisposedException? thrown = null;
        try
        {
            ring.NotifyWorkAvailable();
        }
        catch (ObjectDisposedException ex)
        {
            thrown = ex;
        }

        Assert.Null(thrown);
    }

    /// <summary>Full enqueue/dequeue drain must keep slots reusable.</summary>
    [Fact]
    public async Task EnqueueDequeueKeepsSlots()
    {
        using var ring = new BoundedJournalRing(2);
        var item = JournalWorkItem.Append([], 0);

        await ring.EnqueueAsync(item, DefaultCancellationToken);
        Assert.True(ring.TryDequeue(out var first));
        Assert.Equal(item, first);

        // Slots must be reusable after a full drain; a leaked slot would block this second round.
        var second = JournalWorkItem.Append([], 0);
        await ring.EnqueueAsync(second, DefaultCancellationToken);
        Assert.True(ring.TryDequeue(out var third));
        Assert.Equal(second, third);
    }
}
