using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ReplicaBatchKit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>How the per-follower observation of a dense run records acknowledgements, lagging followers, and the prefix a majority covers.</summary>
[Immutable]
public sealed class ReplicaCommitBatchFanOutTests : ServerUnitTestBase
{
    private const int FollowerSlot = 1;

    /// <summary>A follower that fails one entry of a run is reported lagging once at that entry, and the prefix before it still commits.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedEntryMarksFollowerLaggingOnce(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { FaultFollowerAt = [2] };
        var idempotency = NewIdempotency();
        var clock = new DueTimerClock(CommitBudget);
        var fanOutStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var coordinator = NewCoordinator(pipeline, idempotency, clock, hooks: ReplicaFaultHooks.CreateAtStage(ReplicaCommitStage.FollowerFanOutStarted, fanOutStarted));
        var run = Run(1, 3);
        var commit = coordinator.CommitBatchAsync(run, CommitBudget, cancellationToken).AsTask();
        await fanOutStarted.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        var error = await ExpireAsync(commit, clock, cancellationToken);

        _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
        await SequenceAssert.EqualAsync([(FollowerSlot, 2UL)], pipeline.LaggingReplicas.ToArray());
        await SequenceAssert.EqualAsync([1UL], pipeline.CommitIndexes.ToArray());
        await SequenceAssert.EqualAsync([1UL], pipeline.Applied.ToArray());
        _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("FUU");
    }

    /// <summary>
    /// A follower whose later entries fail before an earlier one is answered, as a disposed sender fails its waiting entries first, records in
    /// log order: nothing is recorded until the earlier entry completes, and then the acknowledgement comes before the single lagging report.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LaterEntriesFailingFirstRecordInLogOrder(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { HoldFollowerAt = [1], FaultFollowerAt = [2, 3] };
        var idempotency = NewIdempotency();
        var clock = new DueTimerClock(CommitBudget);
        var fanOutStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var coordinator = NewCoordinator(pipeline, idempotency, clock, hooks: ReplicaFaultHooks.CreateAtStage(ReplicaCommitStage.FollowerFanOutStarted, fanOutStarted));
        var run = Run(1, 3);
        try
        {
            var commit = coordinator.CommitBatchAsync(run, CommitBudget, cancellationToken).AsTask();
            await fanOutStarted.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await ExpireAsync(commit, clock, cancellationToken);
            _ = await Assert.That(pipeline.LaggingReplicas.Count).IsEqualTo(0);
            _ = await Assert.That(coordinator.MatchIndexFor(FollowerSlot)).IsEqualTo(0UL);

            pipeline.ReleaseHeld();
            await pipeline.AppliedAsync(1).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(await coordinator.ApplyCommittedAsync()).IsFalse();

            await SequenceAssert.EqualAsync([(FollowerSlot, 2UL)], pipeline.LaggingReplicas.ToArray());
            await SequenceAssert.EqualAsync([1UL], pipeline.CommitIndexes.ToArray());
            _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("FUU");
        }
        finally
        {
            pipeline.ReleaseHeld();
        }
    }

    /// <summary>With three replicas, two followers that cover different prefixes commit the highest prefix a majority holds, with one advance, and apply it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PrefixesOfTwoFollowersCommitMajority(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { AckThrough = 0, ReplicaAckThrough = new Dictionary<int, ulong> { [1] = 1, [2] = 2 } };
        var idempotency = NewIdempotency();
        var clock = new DueTimerClock(CommitBudget);
        var fanOutStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var coordinator = NewCoordinator(pipeline, idempotency, clock, 3, ReplicaFaultHooks.CreateAtStage(ReplicaCommitStage.FollowerFanOutStarted, fanOutStarted));
        var run = Run(1, 3);
        try
        {
            var commit = coordinator.CommitBatchAsync(run, CommitBudget, cancellationToken).AsTask();
            await fanOutStarted.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            var error = await ExpireAsync(commit, clock, cancellationToken);

            _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
            await SequenceAssert.EqualAsync([2UL], pipeline.CommitIndexes.ToArray());
            await SequenceAssert.EqualAsync([1UL, 2UL], pipeline.Applied.ToArray());
            _ = await Assert.That(coordinator.CommitIndex).IsEqualTo(2UL);
            _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("FFU");
            _ = await Assert.That(pipeline.LaggingReplicas.Count).IsEqualTo(0);
        }
        finally
        {
            pipeline.ReleaseLate();
        }
    }

    /// <summary>A failed advance of the covered prefix keeps the failure of the majority wait as a cause, and the outcome stays unknown.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PrefixFailureKeepsWaitFailure(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { AckThrough = 2, AdvanceFailure = new IOException("Injected commit index failure.") };
        var idempotency = NewIdempotency();
        var clock = new DueTimerClock(CommitBudget);
        var fanOutStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var coordinator = NewCoordinator(pipeline, idempotency, clock, hooks: ReplicaFaultHooks.CreateAtStage(ReplicaCommitStage.FollowerFanOutStarted, fanOutStarted));
        try
        {
            var commit = StartAsync(coordinator, Run(1, 3), cancellationToken);
            await fanOutStarted.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            var error = await ExpireAsync(commit, clock, cancellationToken);

            _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
            var cause = error.InnerException as AggregateException;
            _ = await Assert.That(cause).IsNotNull();
            _ = await Assert.That(cause!.InnerExceptions.Count).IsEqualTo(2);
            _ = await Assert.That(cause.InnerExceptions[0]).IsAssignableTo<OperationCanceledException>();
            _ = await Assert.That(cause.InnerExceptions[1]).IsAssignableTo<IOException>();
            _ = await Assert.That(pipeline.Applied.Count).IsEqualTo(0);
        }
        finally
        {
            pipeline.ReleaseLate();
        }
    }

    /// <summary>A failure after the follower acknowledgements already hold the majority of the whole run does not fail the run: it commits and applies every entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompleteMajorityOutlivesWaitFailure(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline();
        var idempotency = NewIdempotency();
        var fanOutStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var hooks = ReplicaFaultHooks.CreateAtStage(ReplicaCommitStage.FollowerFanOutStarted, fanOutStarted, Task.FromException(new InvalidOperationException("Injected hook failure.")));
        await using var coordinator = NewCoordinator(pipeline, idempotency, TimeProvider.System, hooks: hooks);
        var run = Run(1, 3);

        await coordinator.CommitBatchAsync(run, StallTimeout, cancellationToken);

        await SequenceAssert.EqualAsync([3UL], pipeline.CommitIndexes.ToArray());
        await SequenceAssert.EqualAsync([1UL, 2UL, 3UL], pipeline.Applied.ToArray());
        _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("FFF");
    }

    /// <summary>When every follower fails the run, the commit reports an unknown outcome without waiting for its budget, and reports each follower lagging once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedFollowersMakeRunUnknown(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { FaultFollowerAt = [1, 2, 3] };
        var idempotency = NewIdempotency();
        await using var coordinator = NewCoordinator(pipeline, idempotency, TimeProvider.System);
        var run = Run(1, 3);

        var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(CommitAsync(coordinator, run, cancellationToken));

        _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
        await SequenceAssert.EqualAsync([(FollowerSlot, 1UL)], pipeline.LaggingReplicas.ToArray());
        _ = await Assert.That(pipeline.CommitIndexes.Count).IsEqualTo(0);
        _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("UUU");
        _ = await Assert.That(coordinator.HasPendingApply).IsTrue();
    }

    /// <summary>Disposal after a run returned unknown, while a follower is still pending, ends the background observation, completes without a leak, and applies nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeEndsObservationAfterRun(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { AckThrough = 0 };
        var budgetClock = new DueTimerClock(CommitBudget);
        var observeClock = new DueTimerClock(TimeSpan.FromSeconds(5));
        var leaks = new ConcurrentQueue<TimeSpan>();
        var fanOutStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var coordinator = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(2, 0, 0, 2),
            pipeline,
            ReplicaFaultHooks.CreateAtStage(ReplicaCommitStage.FollowerFanOutStarted, fanOutStarted),
            NewIdempotency())
        {
            ObserveTimeProvider = observeClock,
            BudgetTimeProvider = budgetClock,
            ShutdownBudget = StallTimeout,
            ShutdownLeakReporter = leaks.Enqueue,
        };
        try
        {
            var commit = StartAsync(coordinator, Run(1, 3), cancellationToken);
            await fanOutStarted.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await ExpireAsync(commit, budgetClock, cancellationToken);

            // The observation registers its bound timer in the background; once the bound passes it waits for a follower or for disposal.
            _ = await observeClock.TimerCreated.WaitAsync(StallTimeout, cancellationToken);
            observeClock.Advance(TimeSpan.FromSeconds(6));
            await coordinator.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            _ = await Assert.That(leaks.Count).IsEqualTo(0);
            _ = await Assert.That(pipeline.Applied.Count).IsEqualTo(0);
        }
        finally
        {
            pipeline.ReleaseLate();
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>
    /// A majority that counts only recovered entries of an older term, under a run whose own entries no follower holds, commits nothing: the older
    /// entries wait for a current-term entry that a majority holds, and then commit with the run.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveredTailNotCommittedByRunCount(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { AckThrough = 0 };
        var idempotency = NewIdempotency();
        var clock = new DueTimerClock(CommitBudget);
        var fanOutStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tail = new ReplicaRecoveredTail([new FollowerLogEntry(1, 1, new byte[] { 1 }), new FollowerLogEntry(2, 1, new byte[] { 2 })], 2, new TailRebuilder());
        await using var coordinator = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(2, 2, 0, 2),
            pipeline,
            ReplicaFaultHooks.CreateAtStage(ReplicaCommitStage.FollowerFanOutStarted, fanOutStarted),
            idempotency,
            null,
            tail)
        {
            ObserveTimeProvider = TimeProvider.System,
            BudgetTimeProvider = clock,
        };
        coordinator.AdmitReplica(FollowerSlot, 2);
        PreparedReplicaMutation[] run = [Mutation(3, 2), Mutation(4, 2)];
        try
        {
            var commit = coordinator.CommitBatchAsync(run, CommitBudget, cancellationToken).AsTask();
            await fanOutStarted.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            var error = await ExpireAsync(commit, clock, cancellationToken);

            _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
            _ = await Assert.That(pipeline.CommitIndexes.Count).IsEqualTo(0);
            _ = await Assert.That(pipeline.Applied.Count).IsEqualTo(0);
            _ = await Assert.That(coordinator.CommitIndex).IsEqualTo(0UL);
            _ = await Assert.That(coordinator.HasPendingApply).IsTrue();

            pipeline.ReleaseLate();
            await pipeline.AppliedAsync(4).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(await coordinator.ApplyCommittedAsync()).IsTrue();

            await SequenceAssert.EqualAsync([1UL, 2UL, 3UL, 4UL], pipeline.Applied.ToArray());
            _ = await Assert.That(coordinator.CommitIndex).IsEqualTo(4UL);
            _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("FF");
        }
        finally
        {
            pipeline.ReleaseLate();
        }
    }

    /// <summary>Rebuilds the prepared form of a recovered entry from its index and term alone.</summary>
    [Immutable]
    private sealed class TailRebuilder : IReplicaTailRebuilder
    {
        public PreparedReplicaMutation Rebuild(FollowerLogEntry entry) => Mutation(entry.LogIndex, entry.Term);
    }
}
