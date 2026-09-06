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
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Bounded repair planning and repair queue lifecycle.</summary>
public sealed class RepairPlannerTests : ServerUnitTestBase
{
    /// <summary>An empty leader log at the genesis index selects a valid empty entries batch.</summary>
    [Fact]
    public void EmptyLeaderLogAtGenesisSelectsEmptyBatch()
    {
        var planner = new ReplicaRepairPlanner(2);

        var selection = planner.SelectRepair([], 1UL, null);

        Assert.Equal(ReplicaRepairSelectionKind.Entries, selection.Kind);
        Assert.True(selection.Batch.Entries.IsEmpty);
    }

    /// <summary>A failing repair is delivered to its caller without stopping the repair loop.</summary>
    [Fact]
    public async Task FailingRepairDoesNotStopLoop()
    {
        using var service = new ReplicaRepairService(2);
        await service.StartAsync(DefaultCancellationToken);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        Assert.True(service.TryQueue(static _ => throw new IOException("simulated repair failure"), DefaultCancellationToken, out var failed));
        Assert.True(service.TryQueue(async cancellationToken => await gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false), DefaultCancellationToken, out var healthy));

        _ = await NodeAsyncAssert.ThrowsAsync<IOException>(failed);
        _ = gate.TrySetResult();
        await healthy.WaitAsync(DefaultCancellationToken);
        await service.StopAsync(DefaultCancellationToken);
    }

    /// <summary>An unexpected repair fault rejects repairs submitted after worker termination instead of leaving them unsettled.</summary>
    [Fact]
    public async Task UnexpectedRepairFailsCompletionAndLoop()
    {
        using var service = new ReplicaRepairService(2);
        await service.StartAsync(DefaultCancellationToken);

        Assert.True(service.TryQueue(static _ => throw new NotSupportedException("simulated repair bug"), DefaultCancellationToken, out var failed));
        _ = await NodeAsyncAssert.ThrowsAsync<NotSupportedException>(failed);

        // The faulted loop completes the writer on its way out. The channel completion synchronizes
        // with worker termination, so a later submission is deterministically rejected. StopAsync would
        // complete the writer itself and mask a missing completion, so it runs only after the assert.
        await service.ReaderCompletion.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, DefaultCancellationToken);

        Assert.False(service.TryQueue(static _ => ValueTask.CompletedTask, DefaultCancellationToken, out var rejected));
        Assert.True(rejected.IsCompletedSuccessfully);
        Assert.Equal(0, service.PendingCount);

        await service.StopAsync(DefaultCancellationToken);
    }

    /// <summary>The planner selects a bounded run and backs up to the follower's known boundary.</summary>
    [Fact]
    public void PlannerBoundsSequentialRepair()
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

        Assert.Equal(1UL, batch.PrevLogIndex);
        Assert.Equal(1UL, batch.PrevLogTerm);
        Assert.Equal(2, batch.Entries.Length);
        Assert.Equal(2UL, batch.Entries.Span[0].LogIndex);
        Assert.Equal(3UL, batch.Entries.Span[1].LogIndex);
        Assert.Equal(3UL, ReplicaRepairPlanner.BackUpNextIndex(5UL, 2UL));
    }

    /// <summary>The bounded service rejects overflow and cancels both active and queued repairs on shutdown.</summary>
    [Fact]
    public async Task QueueCancelsAndDrainsOnStop()
    {
        using var service = new ReplicaRepairService(1);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await service.StartAsync(DefaultCancellationToken);

        Assert.True(
            service.TryQueue(
                async cancellationToken =>
                {
                    _ = started.TrySetResult();
                    await Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, cancellationToken);
                },
                DefaultCancellationToken,
                out var active));
        await started.Task.WaitAsync(DefaultCancellationToken);
        Assert.True(
            service.TryQueue(
                static cancellationToken => new ValueTask(Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, cancellationToken)),
                DefaultCancellationToken,
                out var queued));
        Assert.False(service.TryQueue(static _ => ValueTask.CompletedTask, DefaultCancellationToken, out _));
        Assert.Equal(2, service.PendingCount);

        await service.StopAsync(DefaultCancellationToken);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(active);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(queued);
        Assert.Equal(0, service.PendingCount);
    }

    private static FollowerLogEntry Entry(ulong index, ulong term, string payload) => new(index, term, Encoding.UTF8.GetBytes(payload));
}
