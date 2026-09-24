using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// A durability checkpoint resolves only the ack carried by its own work item. An ack that is
/// already registered but whose checkpoint is still waiting to be enqueued must stay pending when a
/// later caller's checkpoint is processed; otherwise mutations would observe durability before
/// their frames reach the segment file.
/// </summary>
[Immutable]
public sealed class JournalCheckpointAckOwnershipTests : IsolatedStorageTestBase
{
    /// <summary>A foreign checkpoint flush completes only its own ack and leaves earlier registered acks pending.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForeignFlushLeavesAckPending(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 4,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        await journal.WaitForStartupAsync(cancellationToken);
        var coordinator = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;

        var registered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        coordinator.DurabilityAcks.Add(registered);

        // A later caller registers and enqueues its own checkpoint; processing it must not touch
        // the ack registered above.
        await journal.AwaitDurabilityCommitAsync(cancellationToken);

        _ = await Assert.That(registered.Task.IsCompleted).IsFalse();

        _ = coordinator.DurabilityAcks.Remove(registered);
    }

    /// <summary>An ack handed to the journal thread for its fsync can no longer be removed by its caller, and is released only by the thread.</summary>
    [Test]
    public async Task MarkInFlightRefusesCallerRemove()
    {
        var registry = new DurabilityAckRegistry();
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.Add(ack);

        var marked = registry.TryMarkInFlight(ack);
        var removedByCaller = registry.Remove(ack);
        var markedTwice = registry.TryMarkInFlight(ack);
        var trackedWhileInFlight = registry.TakeAll(new ObjectDisposedException(nameof(JournalCoordinator)));
        registry.Complete(ack);

        _ = await Assert.That(marked).IsTrue();
        _ = await Assert.That(removedByCaller).IsFalse();
        _ = await Assert.That(markedTwice).IsFalse();
        _ = await Assert.That(await Assert.That(trackedWhileInFlight).HasSingleItem()).IsSameReferenceAs(ack);
    }

    /// <summary>A drain takes in-flight acks together with pending ones, so shutdown and the failure latch reach a stuck fsync.</summary>
    [Test]
    public async Task TakeAllIncludesInFlightAcks()
    {
        var registry = new DurabilityAckRegistry();
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.Add(pending);
        registry.Add(inFlight);
        _ = registry.TryMarkInFlight(inFlight);

        var drained = registry.TakeAll(new ObjectDisposedException(nameof(JournalCoordinator)), out var inFlightCount);
        var markedAfterDrain = registry.TryMarkInFlight(pending);
        registry.Complete(inFlight);
        var drainedAgain = registry.TakeAll(new ObjectDisposedException(nameof(JournalCoordinator)), out var inFlightAgain);

        _ = await Assert.That(drained.Count).IsEqualTo(2);
        _ = await Assert.That(drained[0]).IsSameReferenceAs(pending);
        _ = await Assert.That(drained[1]).IsSameReferenceAs(inFlight);
        _ = await Assert.That(inFlightCount).IsEqualTo(1);
        _ = await Assert.That(markedAfterDrain).IsFalse();
        _ = await Assert.That(drainedAgain).IsEmpty();
        _ = await Assert.That(inFlightAgain).IsEqualTo(0);
    }

    /// <summary>Ensures a failure drain closes the registry: pending acks drain once, late registrations fail with the recorded reason.</summary>
    [Test]
    public async Task TakeAllClosesRegistryForAdds()
    {
        var registry = new DurabilityAckRegistry();
        var reason = new ObjectDisposedException(nameof(JournalCoordinator));

        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.Add(pending);

        var drained = registry.TakeAll(reason);
        var singleAck = await Assert.That(drained).HasSingleItem();
        _ = await Assert.That(singleAck).IsSameReferenceAs(pending);

        var thrown = NodeExceptionAssert.For<ObjectDisposedException>().Throws(
            registry,
            static r => r.Add(new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)));
        _ = await Assert.That(thrown).IsSameReferenceAs(reason);
    }
}
