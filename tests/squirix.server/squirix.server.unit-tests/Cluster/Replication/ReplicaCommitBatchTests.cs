using System;
using System.Collections.Concurrent;
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

/// <summary>RF=2 commit of a dense run of entries under one admission, one local append, and one majority.</summary>
[Immutable]
public sealed class ReplicaCommitBatchTests : ServerUnitTestBase
{
    /// <summary>A run is appended as one request, takes one commit-index advance to its last entry, and applies and resolves every entry in order.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchAdvancesCommitIndexOnce(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline();
        var idempotency = NewIdempotency();
        await using var coordinator = NewCoordinator(pipeline, idempotency, TimeProvider.System);
        var run = Run(1, 3);

        await coordinator.CommitBatchAsync(run, StallTimeout, cancellationToken);

        await SequenceAssert.EqualAsync([(1UL, 3)], pipeline.LocalRuns.ToArray());
        await SequenceAssert.EqualAsync([3UL], pipeline.CommitIndexes.ToArray());
        await SequenceAssert.EqualAsync([1UL, 2UL, 3UL], pipeline.Applied.ToArray());
        _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("FFF");
        _ = await Assert.That(coordinator.CommitIndex).IsEqualTo(3UL);
        _ = await Assert.That(coordinator.HasPendingApply).IsFalse();
    }

    /// <summary>A run of one entry behaves like any run: its entry resolves as it is applied, without the scope of the caller, and no operation is left behind.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchOfOneEntryResolvesUnscoped(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline();
        var idempotency = NewIdempotency();
        await using var coordinator = NewCoordinator(pipeline, idempotency, TimeProvider.System);
        pipeline.AmbientScope.Value = "caller";
        var run = Run(1, 1);

        await coordinator.CommitBatchAsync(run, StallTimeout, cancellationToken);

        _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("F");
        _ = await Assert.That(idempotency.HasUnresolvedThrough(1)).IsFalse();
        await SequenceAssert.EqualAsync([1UL], pipeline.Applied.ToArray());
        await SequenceAssert.EqualAsync<string?>([null], pipeline.AppliedScopes.ToArray(), StringComparer.Ordinal);
        _ = await Assert.That(coordinator.HasPendingApply).IsFalse();
    }

    /// <summary>A run that is not dense, or not in one term, is refused before anything is reserved or appended.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchRejectsNonDenseRange(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline();
        var idempotency = NewIdempotency();
        await using var coordinator = NewCoordinator(pipeline, idempotency, TimeProvider.System);
        var gap = new[] { Mutation(1), Mutation(3) };
        var mixedTerm = new[] { Mutation(1), Mutation(2, 2) };

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(CommitAsync(coordinator, gap, cancellationToken));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(CommitAsync(coordinator, mixedTerm, cancellationToken));

        _ = await Assert.That(pipeline.LocalRuns.Count).IsEqualTo(0);
        _ = await Assert.That(LookupStates(idempotency, gap)).IsEqualTo("MM");

        await coordinator.CommitBatchAsync(Run(1, 2), StallTimeout, cancellationToken);
        _ = await Assert.That(coordinator.CommitIndex).IsEqualTo(2UL);
    }

    /// <summary>A run refused before the local append releases only its own pins and consumes no index, so the same run commits afterwards.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchRefusedBeforeAppendReleasesPins(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { LocalFailure = new InvalidOperationException("Injected refusal before the local append.") };
        var idempotency = NewIdempotency();
        await using var coordinator = NewCoordinator(pipeline, idempotency, TimeProvider.System);
        var run = Run(1, 3);

        var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(CommitAsync(coordinator, run, cancellationToken));

        _ = await Assert.That(error.Message).DoesNotContain(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
        _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("MMM");
        _ = await Assert.That(coordinator.HasPendingApply).IsFalse();

        pipeline.LocalFailure = null;
        await coordinator.CommitBatchAsync(run, StallTimeout, cancellationToken);
        await SequenceAssert.EqualAsync([1UL, 2UL, 3UL], pipeline.Applied.ToArray());
    }

    /// <summary>An unconfirmed local append leaves every entry unknown with its pins in place, and a retry of any entry reports the unknown outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnconfirmedBatchIsUnknownForEveryEntry(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { LocalFailure = new GroupLogAppendUnconfirmedException() };
        var idempotency = NewIdempotency();
        await using var coordinator = NewCoordinator(pipeline, idempotency, TimeProvider.System);
        var run = Run(1, 3);

        var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(CommitAsync(coordinator, run, cancellationToken));

        _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
        _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("UUU");
        var retry = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(coordinator.CommitAsync(run[1], StallTimeout, cancellationToken));
        _ = await Assert.That(retry.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
    }

    /// <summary>Without a majority the run is unknown, nothing is committed or applied, and the entries stay pending.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchWithoutMajorityStaysPending(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { AckThrough = 0 };
        var idempotency = NewIdempotency();
        var clock = new DueTimerClock(CommitBudget);
        await using var coordinator = NewCoordinator(pipeline, idempotency, clock);
        var run = Run(1, 3);
        try
        {
            var error = await ExpireAsync(coordinator, run, clock, cancellationToken);

            _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
            _ = await Assert.That(pipeline.CommitIndexes.Count).IsEqualTo(0);
            _ = await Assert.That(pipeline.Applied.Count).IsEqualTo(0);
            _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("UUU");
            _ = await Assert.That(coordinator.HasPendingApply).IsTrue();
        }
        finally
        {
            pipeline.ReleaseLate();
        }
    }

    /// <summary>A majority that covers only a prefix commits and applies that prefix with one advance, the whole run stays unknown, and the rest commits once its majority arrives.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchPartialMajorityCommitsCoveredPrefix(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { AckThrough = 2 };
        var idempotency = NewIdempotency();
        var clock = new DueTimerClock(CommitBudget);
        var fanOutStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var coordinator = NewCoordinator(pipeline, idempotency, clock, hooks: ReplicaFaultHooks.CreateAtStage(ReplicaCommitStage.FollowerFanOutStarted, fanOutStarted));
        var run = Run(1, 3);
        try
        {
            var commit = coordinator.CommitBatchAsync(run, CommitBudget, cancellationToken).AsTask();

            // The hook fires after the follower acknowledgements of the prefix were recorded.
            await fanOutStarted.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(coordinator.MatchIndexFor(1)).IsEqualTo(2UL);
            var error = await ExpireAsync(commit, clock, cancellationToken);

            _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
            await SequenceAssert.EqualAsync([2UL], pipeline.CommitIndexes.ToArray());
            await SequenceAssert.EqualAsync([1UL, 2UL], pipeline.Applied.ToArray());
            _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("FFU");
            _ = await Assert.That(coordinator.HasPendingApply).IsTrue();
            _ = await Assert.That(await coordinator.ApplyCommittedAsync()).IsFalse();

            pipeline.ReleaseLate();
            await pipeline.AppliedAsync(3).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            await SequenceAssert.EqualAsync([2UL, 3UL], pipeline.CommitIndexes.ToArray());
            _ = await Assert.That(await coordinator.ApplyCommittedAsync()).IsTrue();
            _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("FFF");
        }
        finally
        {
            pipeline.ReleaseLate();
        }
    }

    /// <summary>
    /// An apply that fails at entry i leaves the entries before it applied and resolved, i and the later ones pending and unresolved, and the whole run
    /// unknown; the next drive applies the rest.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ApplyFailureResolvesAppliedPrefix(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { FailApplyAt = 3 };
        var idempotency = NewIdempotency();
        await using var coordinator = NewCoordinator(pipeline, idempotency, TimeProvider.System);
        var run = Run(1, 4);

        var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(CommitAsync(coordinator, run, cancellationToken));

        _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);

        // The background drive that follows the failed commit fails the same way; once it did, the state is final.
        await pipeline.FailedApplyAsync(2).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("FFUU");
        _ = await Assert.That(idempotency.HasUnresolvedThrough(2)).IsFalse();
        _ = await Assert.That(idempotency.HasUnresolvedThrough(3)).IsTrue();
        _ = await Assert.That(coordinator.HasPendingApply).IsTrue();
        await SequenceAssert.EqualAsync([1UL, 2UL], pipeline.Applied.ToArray());

        pipeline.StopFailingApply();
        _ = await Assert.That(await coordinator.ApplyCommittedAsync()).IsTrue();

        await SequenceAssert.EqualAsync([1UL, 2UL, 3UL, 4UL], pipeline.Applied.ToArray());
        _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("FFFF");
        var retried = await coordinator.CommitAsync(run[2], StallTimeout, cancellationToken);
        await SequenceAssert.EqualMemoryAsync(run[2].OutcomePayload, retried);
    }

    /// <summary>A later commit applies the entries of an earlier run that is committed but not applied, in log order, before its own entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LaterCommitAppliesBatchInOrder(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { FailApplyAt = 1 };
        var idempotency = NewIdempotency();
        await using var coordinator = NewCoordinator(pipeline, idempotency, TimeProvider.System);
        var run = Run(1, 2);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(CommitAsync(coordinator, run, cancellationToken));
        await pipeline.FailedApplyAsync(2).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        _ = await Assert.That(pipeline.Applied.Count).IsEqualTo(0);

        pipeline.StopFailingApply();
        _ = await coordinator.CommitAsync(Mutation(3), StallTimeout, cancellationToken);

        await SequenceAssert.EqualAsync([1UL, 2UL, 3UL], pipeline.Applied.ToArray());
        _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("FF");
        _ = await Assert.That(coordinator.HasPendingApply).IsFalse();
    }

    /// <summary>A failed reservation of entry j releases the pins of the entries before it and throws before anything is appended.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReserveFailureMidBatchReleasesEarlier(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline();
        var idempotency = new GroupIdempotencyState(4, TimeSpan.MaxValue, TimeProvider.System);
        await using var coordinator = NewCoordinator(pipeline, idempotency, TimeProvider.System);
        foreach (var other in new[] { Mutation(90), Mutation(91) })
        {
            var reserved = idempotency.Reserve(other.OperationScope, other.OperationId, other.OperationFingerprint.Span, GroupRecordKinds.FromScope(other.OperationScope), other.LogIndex, other.Term, new GroupReserveOptions(true, 1));
            _ = await Assert.That(reserved).IsEqualTo(GroupIdempotencyReserveResult.Success);
        }

        var run = Run(1, 3);

        var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(CommitAsync(coordinator, run, cancellationToken));

        _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.IdempotencyCapacityCode, StringComparison.Ordinal);
        _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("MMM");
        _ = await Assert.That(pipeline.LocalRuns.Count).IsEqualTo(0);
    }

    /// <summary>Dispose does not tear the gates down under a run stuck in its apply, reports the leak, and the run completes once the apply recovers.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeLeaksRunningBatch(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { StallApply = true };
        var leaks = new ConcurrentQueue<TimeSpan>();
        var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(2, 0, 0, 2), pipeline, ReplicaFaultHooks.CreateNoOp(), NewIdempotency())
        {
            ObserveTimeProvider = TimeProvider.System,
            BudgetTimeProvider = TimeProvider.System,
            ShutdownBudget = TimeSpan.FromMilliseconds(200),
            ShutdownLeakReporter = leaks.Enqueue,
        };
        var commit = CommitAsync(coordinator, Run(1, 3), cancellationToken);
        try
        {
            await pipeline.ApplyEntered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            await coordinator.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(leaks.Count).IsEqualTo(1);

            pipeline.ReleaseApply();
            await commit.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            await SequenceAssert.EqualAsync([1UL, 2UL, 3UL], pipeline.Applied.ToArray());
        }
        finally
        {
            pipeline.ReleaseApply();
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>
    /// Dispose does not tear the gates down under a run that is stuck after its last entry was applied and resolved, where no operation of the run
    /// is left in the table, and the run completes once the stage returns.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeLeaksBatchAfterEveryEntryResolved(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline();
        var leaks = new ConcurrentQueue<TimeSpan>();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var hooks = ReplicaFaultHooks.CreateAtStage(ReplicaCommitStage.MemoryApplied, entered, release.Task);
        var idempotency = NewIdempotency();
        var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(2, 0, 0, 2), pipeline, hooks, idempotency)
        {
            ObserveTimeProvider = TimeProvider.System,
            BudgetTimeProvider = TimeProvider.System,
            ShutdownBudget = TimeSpan.FromMilliseconds(200),
            ShutdownLeakReporter = leaks.Enqueue,
        };
        var run = Run(1, 3);
        var commit = CommitAsync(coordinator, run, cancellationToken);
        try
        {
            await entered.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("FFF");

            await coordinator.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(leaks.Count).IsEqualTo(1);

            _ = release.TrySetResult();
            await commit.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            _ = release.TrySetResult();
            await coordinator.DisposeAsync();
        }
    }
}
