using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Bounded admission and group log-index ordering checks.</summary>
[Immutable]
public sealed class ReplicationOrderingTests : DisposableServerUnitTestBase
{
    private readonly ReplicaLogIndexSequencer _concurrentSequencer = new(0);

    /// <summary>Capacity and stripe leases are released after completion and cancellation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelledMutationReleasesKeyGate(CancellationToken cancellationToken)
    {
        using var gate = new ReplicaMutationGate(1, 2);
        using var first = await gate.EnterAsync(7, cancellationToken);
        using var cancellation = new CancellationTokenSource();
        var waiting = gate.EnterAsync(7, cancellation.Token);
        await cancellation.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, ReplicaMutationLease>(waiting);
        _ = await Assert.That(gate.ActiveCount).IsEqualTo(1);

        // ReSharper disable once DisposeOnUsingVariable — intentional early release: the test asserts the count drops before the scope ends.
        first.Dispose();
        _ = await Assert.That(gate.ActiveCount).IsEqualTo(0);

        using var next = await gate.EnterAsync(7, cancellationToken);
        _ = await Assert.That(gate.ActiveCount).IsEqualTo(1);

        // ReSharper disable once DisposeOnUsingVariable — intentional early release: the test asserts the count drops before the scope ends.
        next.Dispose();
        _ = await Assert.That(gate.ActiveCount).IsEqualTo(0);
        _ = await Assert.That(gate.MaxInFlight).IsEqualTo(1);
        _ = await Assert.That(gate.StripeCount).IsEqualTo(2);
    }

    /// <summary>The final index cannot complete an append; the boundary stays refused.</summary>
    [Test]
    public void CompleteFinalIndexAppendThrows()
    {
        using var sequencer = new ReplicaLogIndexSequencer(ulong.MaxValue - 1);
        var state = (sequencer, index: ulong.MaxValue);
        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(state, static s => s.sequencer.Complete(s.index, true));
    }

    /// <summary>Completion for a foreign index is refused without touching the next index.</summary>
    [Test]
    public void CompleteForeignIndexThrows()
    {
        using var sequencer = new ReplicaLogIndexSequencer(7);
        var state = (sequencer, index: 999UL);
        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(state, static s => s.sequencer.Complete(s.index, true));
    }

    /// <summary>Concurrent mutations receive distinct increasing indexes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentMutationsGetOrderedIndexes(CancellationToken cancellationToken)
    {
        var indexes = new List<ulong>();
        var sync = new Lock();
        var start = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = new Task[32];
        for (var i = 0; i < tasks.Length; i++)
            tasks[i] = ReserveAndAppendAsync(_concurrentSequencer, indexes, sync, start.Task, cancellationToken);

        start.SetResult(true);
        await Task.WhenAll(tasks);

        indexes.Sort();
        _ = await Assert.That(indexes.Count).IsEqualTo(tasks.Length);
        var expected = 1UL;
        for (var i = 0; i < indexes.Count; i++)
            _ = await Assert.That(indexes[i]).IsEqualTo(expected++);
    }

    /// <summary>An exhausted index is refused at reservation time instead of handed out and failed later.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExhaustedLogIndexRefusesReservation(CancellationToken cancellationToken)
    {
        using var sequencer = new ReplicaLogIndexSequencer(ulong.MaxValue - 1);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReplicaIndexReservation>(sequencer.ReserveAsync(cancellationToken));

        // The refused reservation must release the gate: a second attempt fails instead of hanging.
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReplicaIndexReservation>(sequencer.ReserveAsync(cancellationToken));
    }

    /// <summary>A failed local append leaves its index available to the next mutation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedLocalAppendDoesNotLeaveIndexGap(CancellationToken cancellationToken)
    {
        using var sequencer = new ReplicaLogIndexSequencer(7);
        using (var failed = await sequencer.ReserveAsync(cancellationToken))
            _ = await Assert.That(failed.Index).IsEqualTo(8UL);

        using var retry = await sequencer.ReserveAsync(cancellationToken);
        _ = await Assert.That(retry.Index).IsEqualTo(8UL);
        retry.MarkAppended();
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _concurrentSequencer.Dispose();

    private static async Task ReserveAndAppendAsync(ReplicaLogIndexSequencer sequencer, List<ulong> indexes, Lock sync, Task start, CancellationToken cancellationToken)
    {
        await start.WaitAsync(cancellationToken);
        using var reservation = await sequencer.ReserveAsync(cancellationToken);
        lock (sync)
            indexes.Add(reservation.Index);
        reservation.MarkAppended();
    }
}
