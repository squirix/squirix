using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Bounded repair planning and repair queue lifecycle.</summary>
public sealed class RepairPlannerTests : ServerUnitTestBase
{
    /// <summary>An empty leader log at the genesis index selects a valid empty entries batch.</summary>
    [Test]
    public async Task EmptyLeaderLogAtGenesisSelectsEmptyBatch()
    {
        var planner = new ReplicaRepairPlanner(2);

        var selection = planner.SelectRepair([], 1UL, null);

        _ = await Assert.That(selection.Kind).IsEqualTo(ReplicaRepairSelectionKind.Entries);
        _ = await Assert.That(selection.Batch.Entries.IsEmpty).IsTrue();
    }

    /// <summary>A failing repair is delivered to its caller without stopping the repair loop.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailingRepairDoesNotStopLoop(CancellationToken cancellationToken)
    {
        using var service = new ReplicaRepairService(2);
        await service.StartAsync(cancellationToken);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = await Assert.That(service.TryQueue(static _ => throw new IOException("simulated repair failure"), cancellationToken, out var failed)).IsTrue();
        _ = await Assert.That(service.TryQueue(async repairToken => await gate.Task.WaitAsync(repairToken).ConfigureAwait(false), cancellationToken, out var healthy)).IsTrue();

        _ = await NodeAsyncAssert.ThrowsAsync<IOException>(failed);
        _ = gate.TrySetResult();
        await healthy.WaitAsync(cancellationToken);
        await service.StopAsync(cancellationToken);
    }

    /// <summary>The planner selects a bounded run and backs up to the follower's known boundary.</summary>
    [Test]
    public async Task PlannerBoundsSequentialRepair()
    {
        var entries = new[]
        {
            Entry(1UL, 1UL, "one"),
            Entry(2UL, 1UL, "two"),
            Entry(3UL, 1UL, "three"),
            Entry(4UL, 2UL, "four"),
        };
        var planner = new ReplicaRepairPlanner(2);

        var batch = planner.SelectBatch(entries, 2UL);

        _ = await Assert.That(batch.PrevLogIndex).IsEqualTo(1UL);
        _ = await Assert.That(batch.PrevLogTerm).IsEqualTo(1UL);
        _ = await Assert.That(batch.Entries.Length).IsEqualTo(2);
        _ = await Assert.That(batch.Entries.Span[0].LogIndex).IsEqualTo(2UL);
        _ = await Assert.That(batch.Entries.Span[1].LogIndex).IsEqualTo(3UL);
        _ = await Assert.That(ReplicaRepairPlanner.BackUpNextIndex(5UL, 2UL)).IsEqualTo(3UL);
    }

    /// <summary>The bounded service rejects overflow and cancels both active and queued repairs on shutdown.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueueCancelsAndDrainsOnStop(CancellationToken cancellationToken)
    {
        using var service = new ReplicaRepairService(1);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await service.StartAsync(cancellationToken);

        _ = await Assert.That(
            service.TryQueue(
                async repairToken =>
                {
                    _ = started.TrySetResult();
                    await Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, repairToken);
                },
                cancellationToken,
                out var active)).IsTrue();
        await started.Task.WaitAsync(cancellationToken);
        _ = await Assert.That(
                             service.TryQueue(
                                 static repairToken => new ValueTask(Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, repairToken)),
                                 cancellationToken,
                                 out var queued))
                        .IsTrue();
        _ = await Assert.That(service.TryQueue(static _ => ValueTask.CompletedTask, cancellationToken, out _)).IsFalse();
        _ = await Assert.That(service.PendingCount).IsEqualTo(2);

        await service.StopAsync(cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(active);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(queued);
        _ = await Assert.That(service.PendingCount).IsEqualTo(0);
    }

    /// <summary>An unexpected repair fault rejects repairs submitted after worker termination instead of leaving them unsettled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnexpectedRepairFailsCompletionAndLoop(CancellationToken cancellationToken)
    {
        using var service = new ReplicaRepairService(2);
        await service.StartAsync(cancellationToken);

        _ = await Assert.That(service.TryQueue(static _ => throw new NotSupportedException("simulated repair bug"), cancellationToken, out var failed)).IsTrue();
        _ = await NodeAsyncAssert.ThrowsAsync<NotSupportedException>(failed);

        // The faulted loop completes the writer on its way out. The channel completion synchronizes
        // with worker termination, so a later submission is deterministically rejected. StopAsync would
        // complete the writer itself and mask a missing completion, so it runs only after the assert.
        await service.ReaderCompletion.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

        _ = await Assert.That(service.TryQueue(static _ => ValueTask.CompletedTask, cancellationToken, out var rejected)).IsFalse();
        _ = await Assert.That(rejected.IsCompletedSuccessfully).IsTrue();
        _ = await Assert.That(service.PendingCount).IsEqualTo(0);

        await service.StopAsync(cancellationToken);
    }

    private static FollowerLogEntry Entry(ulong index, ulong term, string payload) => new(index, term, Encoding.UTF8.GetBytes(payload));
}
