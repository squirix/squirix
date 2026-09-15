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

    private sealed class PipelineFailureProbe
    {
        internal InvalidOperationException Reason { get; } = new("pipeline failed");

        internal void Throw() => throw Reason;
    }
}
