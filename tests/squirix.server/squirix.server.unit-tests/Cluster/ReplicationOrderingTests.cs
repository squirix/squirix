using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Bounded admission and group log-index ordering checks.</summary>
[Immutable]
public sealed class ReplicationOrderingTests : DisposableServerUnitTestBase
{
    private readonly ReplicaLogIndexSequencer _concurrentSequencer = new(0);

    /// <summary>Concurrent mutations receive distinct increasing indexes.</summary>
    [Fact(DisplayName = "ConcurrentMutationsGetDistinctIncreasingIndexes")]
    public async Task ConcurrentMutationsGetOrderedIndexes()
    {
        var indexes = new List<ulong>();
        var sync = new Lock();
        var start = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = new Task[32];
        for (var i = 0; i < tasks.Length; i++)
            tasks[i] = ReserveAndAppendAsync(_concurrentSequencer, indexes, sync, start.Task, DefaultCancellationToken);

        start.SetResult(true);
        await Task.WhenAll(tasks);

        indexes.Sort();
        Assert.Equal(tasks.Length, indexes.Count);
        var expected = 1UL;
        for (var i = 0; i < indexes.Count; i++)
            Assert.Equal(expected++, indexes[i]);
    }

    /// <summary>A failed local append leaves its index available to the next mutation.</summary>
    [Fact]
    public async Task FailedLocalAppendDoesNotLeaveIndexGap()
    {
        using var sequencer = new ReplicaLogIndexSequencer(7);
        using (var failed = await sequencer.ReserveAsync(DefaultCancellationToken))
            Assert.Equal(8UL, failed.Index);

        using var retry = await sequencer.ReserveAsync(DefaultCancellationToken);
        Assert.Equal(8UL, retry.Index);
        retry.MarkAppended();
    }

    /// <summary>Capacity and stripe leases are released after completion and cancellation.</summary>
    [Fact]
    public async Task CancelledMutationReleasesKeyGate()
    {
        using var gate = new ReplicaMutationGate(1, 2);
        using var first = await gate.EnterAsync(7, DefaultCancellationToken);
        using var cancellation = new CancellationTokenSource();
        var waiting = gate.EnterAsync(7, cancellation.Token);
        await cancellation.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, ReplicaMutationLease>(waiting);
        Assert.Equal(1, gate.ActiveCount);

        // ReSharper disable once DisposeOnUsingVariable — intentional early release: the test asserts the count drops before the scope ends.
        first.Dispose();
        Assert.Equal(0, gate.ActiveCount);

        using var next = await gate.EnterAsync(7, DefaultCancellationToken);
        Assert.Equal(1, gate.ActiveCount);

        // ReSharper disable once DisposeOnUsingVariable — intentional early release: the test asserts the count drops before the scope ends.
        next.Dispose();
        Assert.Equal(0, gate.ActiveCount);
        Assert.Equal(1, gate.MaxInFlight);
        Assert.Equal(2, gate.StripeCount);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _concurrentSequencer.Dispose();

    private static async Task ReserveAndAppendAsync(
        ReplicaLogIndexSequencer sequencer,
        List<ulong> indexes,
        Lock sync,
        Task start,
        CancellationToken cancellationToken)
    {
        await start.WaitAsync(cancellationToken);
        using var reservation = await sequencer.ReserveAsync(cancellationToken);
        lock (sync)
            indexes.Add(reservation.Index);
        reservation.MarkAppended();
    }
}
