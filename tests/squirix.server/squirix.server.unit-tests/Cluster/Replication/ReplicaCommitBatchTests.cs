using System;
using System.Collections.Concurrent;
using System.Globalization;
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

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>RF=2 commit of a dense run of entries under one admission, one local append, and one majority.</summary>
[Immutable]
public sealed class ReplicaCommitBatchTests : ServerUnitTestBase
{
    private static readonly TimeSpan CommitBudget = TimeSpan.FromMilliseconds(200);

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

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
        await using var coordinator = NewCoordinator(pipeline, idempotency, clock);
        var run = Run(1, 3);
        try
        {
            var commit = coordinator.CommitBatchAsync(run, CommitBudget, cancellationToken).AsTask();
            await WaitUntilAsync(() => coordinator.MatchIndexFor(1) == 2, cancellationToken);
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
        _ = await Assert.That(LookupStates(idempotency, run)).IsEqualTo("FFUU");
        _ = await Assert.That(idempotency.HasUnresolvedThrough(2)).IsFalse();
        _ = await Assert.That(idempotency.HasUnresolvedThrough(3)).IsTrue();
        _ = await Assert.That(coordinator.HasPendingApply).IsTrue();
        await SequenceAssert.EqualAsync([1UL, 2UL], pipeline.Applied.ToArray());

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
        _ = await Assert.That(pipeline.Applied.Count).IsEqualTo(0);

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

    /// <summary>A run with an identity that is already retained is refused before anything is reserved.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchWithKnownIdentityIsRefused(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline();
        var idempotency = NewIdempotency();
        await using var coordinator = NewCoordinator(pipeline, idempotency, TimeProvider.System);
        _ = await coordinator.CommitAsync(Mutation(1), StallTimeout, cancellationToken);
        var run = new[] { Mutation(2), Mutation(1) };

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(CommitAsync(coordinator, run, cancellationToken));

        _ = await Assert.That(LookupStates(idempotency, [run[0]])).IsEqualTo("M");
        _ = await Assert.That(pipeline.LocalRuns.Count).IsEqualTo(1);
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

    private static Task CommitAsync(ReplicaCommitCoordinator coordinator, PreparedReplicaMutation[] run, CancellationToken cancellationToken) =>
        coordinator.CommitBatchAsync(run, StallTimeout, cancellationToken).AsTask();

    private static Task<InvalidOperationException> ExpireAsync(ReplicaCommitCoordinator coordinator, PreparedReplicaMutation[] run, DueTimerClock clock, CancellationToken cancellationToken) =>
        ExpireAsync(coordinator.CommitBatchAsync(run, CommitBudget, cancellationToken).AsTask(), clock, cancellationToken);

    private static async Task<InvalidOperationException> ExpireAsync(Task commit, DueTimerClock clock, CancellationToken cancellationToken)
    {
        _ = await clock.TimerCreated.WaitAsync(StallTimeout, cancellationToken);
        clock.Advance(CommitBudget);
        return await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(commit.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
    }

    private static string LookupStates(GroupIdempotencyState idempotency, PreparedReplicaMutation[] run)
    {
        var states = new char[run.Length];
        for (var i = 0; i < run.Length; i++)
        {
            var mutation = run[i];
            var lookup = idempotency.Lookup(mutation.OperationScope, mutation.OperationId, mutation.OperationFingerprint.Span, out _);
            states[i] = lookup switch
            {
                GroupIdempotencyLookup.Found => 'F',
                GroupIdempotencyLookup.Unresolved => 'U',
                GroupIdempotencyLookup.Miss => 'M',
                GroupIdempotencyLookup.Mismatch => 'X',
                _ => throw new ArgumentOutOfRangeException(nameof(run), lookup, "Unsupported lookup."),
            };
        }

        return new string(states);
    }

    private static GroupIdempotencyState NewIdempotency() => new(16, TimeSpan.MaxValue, TimeProvider.System);

    private static ReplicaCommitCoordinator NewCoordinator(RunPipeline pipeline, GroupIdempotencyState idempotency, TimeProvider budgetClock) =>
        new(new ReplicaCommitCoordinatorOptions(2, 0, 0, 2), pipeline, ReplicaFaultHooks.CreateNoOp(), idempotency)
        {
            ObserveTimeProvider = TimeProvider.System,
            BudgetTimeProvider = budgetClock,
        };

    private static PreparedReplicaMutation Mutation(ulong logIndex, ulong term = 1) => new(
        new ReplicaOperationIdentity("group-a", "client", logIndex.ToString("x32", CultureInfo.InvariantCulture), new byte[] { 1 }),
        term,
        logIndex,
        new ReplicaMutationPayload(new byte[] { 2 }, new[] { byte.CreateChecked(10 + logIndex) }, 4));

    private static PreparedReplicaMutation[] Run(ulong first, int count)
    {
        var run = new PreparedReplicaMutation[count];
        for (var i = 0; i < count; i++)
            run[i] = Mutation(first + ulong.CreateChecked(i));

        return run;
    }

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken cancellationToken)
    {
        using var guard = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        guard.CancelAfter(StallTimeout);
        while (!condition())
            await Task.Delay(1, guard.Token);
    }
}
