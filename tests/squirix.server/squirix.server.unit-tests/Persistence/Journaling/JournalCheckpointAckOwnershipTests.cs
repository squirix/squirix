using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
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
        };

        using var manifestStore = new Ledger(options, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            NullLoggerFactory.Instance,
            TimeProvider.System,
            out _);
        await journal.WaitForStartupAsync(cancellationToken);
        var coordinator = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;

        TaskCompletionSource[] registered =
        [
            new(TaskCreationOptions.RunContinuationsAsynchronously),
            new(TaskCreationOptions.RunContinuationsAsynchronously),
            new(TaskCreationOptions.RunContinuationsAsynchronously),
        ];
        foreach (var ack in registered)
            coordinator.DurabilityAcks.Add(ack);

        // A later caller registers and enqueues its own checkpoint; processing it must not touch
        // the ack registered above.
        for (var i = 0; i < 3; i++)
        {
            await journal.AwaitDurabilityCommitAsync(cancellationToken);
            foreach (var ack in registered)
                _ = await Assert.That(ack.Task.IsCompleted).IsFalse();
        }

        foreach (var ack in registered)
            _ = await Assert.That(coordinator.DurabilityAcks.Remove(ack)).IsTrue();
    }

    /// <summary>Detaching a middle pending ack prevents its handoff without affecting later or earlier checkpoints.</summary>
    [Test]
    public async Task RemovedPendingCannotBecomeInFlight()
    {
        var registry = new DurabilityAckRegistry();
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var removed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var last = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.Add(first);
        registry.Add(removed);
        registry.Add(last);

        _ = await Assert.That(registry.Remove(removed)).IsTrue();
        _ = await Assert.That(registry.Remove(removed)).IsFalse();
        _ = await Assert.That(registry.TryMarkInFlight(removed)).IsFalse();
        _ = await Assert.That(registry.TryMarkInFlight(last)).IsTrue();
        _ = await Assert.That(registry.Remove(last)).IsFalse();
        registry.Complete(last);
        _ = await Assert.That(registry.TryMarkInFlight(last)).IsFalse();
        _ = await Assert.That(registry.TryMarkInFlight(first)).IsTrue();
        registry.Complete(first);

        _ = await Assert.That(registry.TakeAll(new ObjectDisposedException(nameof(JournalCoordinator)), out var inFlightCount)).IsEmpty();
        _ = await Assert.That(inFlightCount).IsEqualTo(0);
    }

    /// <summary>A mixed drain keeps pending acks before in-flight acks and excludes detached and completed checkpoints.</summary>
    [Test]
    public async Task MixedDrainKeepsPendingBeforeInFlight()
    {
        var registry = new DurabilityAckRegistry();
        var firstPending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var removed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstInFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var lastInFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var lastPending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.Add(firstPending);
        registry.Add(removed);
        registry.Add(firstInFlight);
        registry.Add(completed);
        registry.Add(lastInFlight);
        registry.Add(lastPending);
        _ = await Assert.That(registry.Remove(removed)).IsTrue();
        _ = await Assert.That(registry.TryMarkInFlight(lastInFlight)).IsTrue();
        _ = await Assert.That(registry.TryMarkInFlight(completed)).IsTrue();
        registry.Complete(completed);
        _ = await Assert.That(registry.TryMarkInFlight(firstInFlight)).IsTrue();

        var failure = new InvalidOperationException("Injected journal failure.");
        var drained = registry.TakeAll(failure, out var inFlightCount);

        _ = await Assert.That(drained.Count).IsEqualTo(4);
        _ = await Assert.That(inFlightCount).IsEqualTo(2);
        _ = await Assert.That(drained.GetRange(0, 2).Contains(firstPending)).IsTrue();
        _ = await Assert.That(drained.GetRange(0, 2).Contains(lastPending)).IsTrue();
        _ = await Assert.That(drained[2]).IsSameReferenceAs(lastInFlight);
        _ = await Assert.That(drained[3]).IsSameReferenceAs(firstInFlight);
        _ = await Assert.That(registry.Remove(firstPending)).IsFalse();
        _ = await Assert.That(registry.TryMarkInFlight(lastPending)).IsFalse();
        registry.Complete(firstInFlight);
        registry.Complete(lastInFlight);
        _ = await Assert.That(registry.TakeAll(new ObjectDisposedException(nameof(JournalCoordinator)), out var remainingInFlight)).IsEmpty();
        _ = await Assert.That(remainingInFlight).IsEqualTo(0);
        var thrown = NodeExceptionAssert.For<InvalidOperationException>().Throws(
            registry,
            static r => r.Add(new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)));
        _ = await Assert.That(thrown).IsSameReferenceAs(failure);
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
        var trackedWhileInFlight = registry.TakeAll(new ObjectDisposedException(nameof(JournalCoordinator)), out _);
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

        var drained = registry.TakeAll(reason, out _);
        var singleAck = await Assert.That(drained).HasSingleItem();
        _ = await Assert.That(singleAck).IsSameReferenceAs(pending);

        var thrown = NodeExceptionAssert.For<ObjectDisposedException>().Throws(
            registry,
            static r => r.Add(new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)));
        _ = await Assert.That(thrown).IsSameReferenceAs(reason);
    }
}
