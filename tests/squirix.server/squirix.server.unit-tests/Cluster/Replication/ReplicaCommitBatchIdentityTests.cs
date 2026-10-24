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
using static Squirix.Server.UnitTests.Cluster.Replication.ReplicaBatchKit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Operation identities, ownership of the run, and the ambient scope of the entries of a dense-run commit.</summary>
[Immutable]
public sealed class ReplicaCommitBatchIdentityTests : ServerUnitTestBase
{
    /// <summary>A run with an entry that reuses the identity of a committed operation is refused before anything is reserved, and the committed record stays.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchReusingCommittedIdentityIsRefused(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline();
        var idempotency = NewIdempotency();
        await using var coordinator = NewCoordinator(pipeline, idempotency, TimeProvider.System);
        var committed = Mutation(1);
        _ = await coordinator.CommitAsync(committed, StallTimeout, cancellationToken);
        PreparedReplicaMutation[] run = [Mutation(2), Mutation(3), Mutation(4, 1, 1)];

        var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(CommitAsync(coordinator, run, cancellationToken));

        _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.BatchIdentityKnownCode, StringComparison.Ordinal);
        _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("MMF");
        _ = await Assert.That(LookupStates(idempotency, [committed])).IsEqualTo("F");
        await SequenceAssert.EqualAsync([(1UL, 1)], pipeline.LocalRuns.ToArray());
        await SequenceAssert.EqualAsync([1UL], pipeline.CommitIndexes.ToArray());
        await SequenceAssert.EqualAsync([1UL], pipeline.Applied.ToArray());
    }

    /// <summary>A run that holds one identity twice is refused before anything is reserved.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchRepeatingIdentityIsRefused(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline();
        var idempotency = NewIdempotency();
        await using var coordinator = NewCoordinator(pipeline, idempotency, TimeProvider.System);
        PreparedReplicaMutation[] run = [Mutation(1), Mutation(2), Mutation(3, 1, 1)];

        var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(CommitAsync(coordinator, run, cancellationToken));

        _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.BatchIdentityKnownCode, StringComparison.Ordinal);
        _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("MMM");
        _ = await Assert.That(pipeline.LocalRuns.Count).IsEqualTo(0);

        await coordinator.CommitBatchAsync(Run(1, 3), StallTimeout, cancellationToken);
        await SequenceAssert.EqualAsync([1UL, 2UL, 3UL], pipeline.Applied.ToArray());
    }

    /// <summary>A run with an entry that reuses an identity still unresolved from a commit in flight is refused and leaves the pin of that commit alone.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchReusingInFlightIdentityIsRefused(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { AckThrough = 0 };
        var idempotency = NewIdempotency();
        var fanOutStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var coordinator = NewCoordinator(pipeline, idempotency, TimeProvider.System, hooks: ReplicaFaultHooks.CreateAtStage(ReplicaCommitStage.FollowerFanOutStarted, fanOutStarted));
        var inFlight = Mutation(1);
        var first = coordinator.CommitAsync(inFlight, StallTimeout, cancellationToken).AsTask();
        try
        {
            await fanOutStarted.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            PreparedReplicaMutation[] run = [Mutation(2), Mutation(3), Mutation(4, 1, 1)];

            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(CommitAsync(coordinator, run, cancellationToken));

            _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.BatchIdentityKnownCode, StringComparison.Ordinal);
            _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("MMU");
            _ = await Assert.That(LookupStates(idempotency, [inFlight])).IsEqualTo("U");
            _ = await Assert.That(pipeline.LocalRuns.Count).IsEqualTo(1);
        }
        finally
        {
            pipeline.ReleaseLate();
        }

        _ = await first;
        _ = await Assert.That(LookupStates(idempotency, [inFlight])).IsEqualTo("F");
    }

    /// <summary>A same-identity retry of an entry of a run in flight replays the outcome of that entry, not the outcome of the last entry the run resolves with.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetryOfRunEntryReplaysItsOwnOutcome(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { AckThrough = 0 };
        var idempotency = NewIdempotency();
        var fanOutStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var coordinator = NewCoordinator(pipeline, idempotency, TimeProvider.System, hooks: ReplicaFaultHooks.CreateAtStage(ReplicaCommitStage.FollowerFanOutStarted, fanOutStarted));
        var run = Run(1, 3);
        var batch = CommitAsync(coordinator, run, cancellationToken);
        try
        {
            await fanOutStarted.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var retry = coordinator.CommitAsync(run[0], StallTimeout, cancellationToken).AsTask();
            pipeline.ReleaseLate();

            await batch.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var outcome = await retry.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            await SequenceAssert.EqualMemoryAsync(run[0].OutcomePayload, outcome);
            _ = await Assert.That(outcome.Span.SequenceEqual(run[2].OutcomePayload.Span)).IsFalse();
        }
        finally
        {
            pipeline.ReleaseLate();
        }
    }

    /// <summary>The run is copied: emptying the caller's list after the call returns does not disturb the background observation that still reads the run.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchOutlivesCallerList(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { AckThrough = 2 };
        var idempotency = NewIdempotency();
        var clock = new DueTimerClock(CommitBudget);
        var fanOutStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var coordinator = NewCoordinator(pipeline, idempotency, clock, hooks: ReplicaFaultHooks.CreateAtStage(ReplicaCommitStage.FollowerFanOutStarted, fanOutStarted));
        var run = Run(1, 3);
        List<PreparedReplicaMutation> callerList = [.. run];
        try
        {
            var commit = coordinator.CommitBatchAsync(callerList, CommitBudget, cancellationToken).AsTask();
            await fanOutStarted.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await ExpireAsync(commit, clock, cancellationToken);
            callerList.Clear();

            pipeline.ReleaseLate();
            await pipeline.AppliedAsync(3).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(await coordinator.ApplyCommittedAsync()).IsTrue();

            await SequenceAssert.EqualAsync([2UL, 3UL], pipeline.CommitIndexes.ToArray());
            _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("FFF");
        }
        finally
        {
            pipeline.ReleaseLate();
        }
    }

    /// <summary>The entries of a run are applied without the ambient scope of the caller, while a single entry still is applied in it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchEntriesApplyWithoutCallerScope(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline();
        await using var coordinator = NewCoordinator(pipeline, NewIdempotency(), TimeProvider.System);
        pipeline.AmbientScope.Value = "caller";

        await coordinator.CommitBatchAsync(Run(1, 3), StallTimeout, cancellationToken);
        _ = await coordinator.CommitAsync(Mutation(4), StallTimeout, cancellationToken);

        string?[] expected = [null, null, null, "caller"];
        await SequenceAssert.EqualAsync(expected, pipeline.AppliedScopes.ToArray(), StringComparer.Ordinal);
    }
}
