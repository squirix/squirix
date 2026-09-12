using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>A wedged ring observes the pipeline failure on its poll slices instead of parking producers forever.</summary>
[Immutable]
public sealed class BoundedJournalRingTests
{
    /// <summary>A full ring invokes the failure poll on slice expiry and keeps its slot accounting.</summary>
    [Fact]
    public async Task SlicePollObservesPipelineFailure()
    {
        using var ring = new BoundedJournalRing(1);
        await ring.EnqueueAsync(JournalWorkItem.Shutdown(), CancellationToken.None);

        var probe = new PipelineFailureProbe();
        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            ring.EnqueueAsync(JournalWorkItem.Shutdown(), CancellationToken.None, probe.Throw).AsTask());
        Assert.Same(probe.Reason, thrown);

        Assert.True(ring.TryDequeue(out var filler));
        Assert.NotNull(filler);
        Assert.False(ring.TryDequeue(out _));
    }

    private sealed class PipelineFailureProbe
    {
        internal InvalidOperationException Reason { get; } = new("pipeline failed");

        internal void Throw() => throw Reason;
    }
}
