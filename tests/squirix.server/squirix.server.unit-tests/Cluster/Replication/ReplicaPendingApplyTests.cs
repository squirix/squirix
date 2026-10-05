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

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>RF=2 entries left locally appended but unapplied by their own commit, and how they are applied later.</summary>
[Immutable]
public sealed class ReplicaPendingApplyTests : ServerUnitTestBase
{
    private const string ForeignOperationId = "00000000000000000000000000000002";

    /// <summary>
    /// Caller-scoped async-local operation id standing in for the RPC idempotency scope that stamps cache-WAL frames: both flow with the
    /// execution context, which is what the coordinator must not carry into a re-applied earlier entry.
    /// </summary>
    private static readonly AsyncLocal<string?> CallerOperationId = new();

    private static readonly TimeSpan CommitBudget = TimeSpan.FromMilliseconds(200);

    /// <summary>Past the coordinator's 5 s bound on the first wait of background follower observation.</summary>
    private static readonly TimeSpan PastObserveBound = TimeSpan.FromSeconds(6);

    /// <summary>The coordinator's bound on the first wait of background follower observation, as the due time its timer is armed with.</summary>
    private static readonly TimeSpan ObserveBound = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Disposal stops background follower observation that is still waiting, past its first bound, for a follower that never answers,
    /// instead of waiting out the drain bound, which would be reported as a shutdown leak.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeStopsUnboundedFollowerObserver(CancellationToken cancellationToken)
    {
        var pipeline = new ScriptedPipeline(false);
        var budgetClock = new DueTimerClock(CommitBudget);
        var observeClock = new DueTimerClock(ObserveBound);
        var leaks = new ConcurrentQueue<TimeSpan>();
        var coordinator = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(2, 0, 0, 1),
            pipeline,
            ReplicaFaultHooks.CreateNoOp(),
            new GroupIdempotencyState(4, TimeSpan.MaxValue))
        {
            BudgetTimeProvider = budgetClock,
            ObserveTimeProvider = observeClock,
            ShutdownBudget = StallTimeout,
            ShutdownLeakReporter = leaks.Enqueue,
        };
        var commit = coordinator.CommitAsync(CreateMutation(1, "00000000000000000000000000000001", 11), CommitBudget, cancellationToken);
        try
        {
            await pipeline.FollowerEntered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            budgetClock.Advance(CommitBudget);
            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(commit);
            _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);

            // The observer is past its first bound once the observe clock moved beyond it, so only disposal can stop it.
            _ = await Assert.That(await observeClock.TimerCreated.WaitAsync(StallTimeout, cancellationToken)).IsTrue();
            observeClock.Advance(PastObserveBound);

            await coordinator.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(leaks.IsEmpty).IsTrue();
        }
        finally
        {
            pipeline.ReleaseFollower();
        }
    }

    /// <summary>
    /// A follower acknowledgement arriving after background observation's first bounded wait expired still completes the majority: the
    /// entry is applied and its idempotency record resolved, nothing stays pending, and the coordinator commits the next write.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LateAckAfterObserveTimeoutResolves(CancellationToken cancellationToken)
    {
        var pipeline = new ScriptedPipeline(false);
        var idempotency = new GroupIdempotencyState(4, TimeSpan.MaxValue);
        var budgetClock = new DueTimerClock(CommitBudget);
        var observeClock = new DueTimerClock(ObserveBound);
        await using var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(2, 0, 0, 1), pipeline, ReplicaFaultHooks.CreateNoOp(), idempotency)
        {
            BudgetTimeProvider = budgetClock,
            ObserveTimeProvider = observeClock,
        };
        var mutation = CreateMutation(1, "00000000000000000000000000000001", 11);
        var commit = coordinator.CommitAsync(mutation, CommitBudget, cancellationToken);
        await pipeline.FollowerEntered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        budgetClock.Advance(CommitBudget);
        var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(commit);
        _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);

        _ = await Assert.That(await observeClock.TimerCreated.WaitAsync(StallTimeout, cancellationToken)).IsTrue();
        observeClock.Advance(PastObserveBound);
        pipeline.ReleaseFollower();
        await pipeline.FirstApplied.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        // The committer's pre-prepare drive: nothing is left pending, so writes are accepted again.
        _ = await Assert.That(await coordinator.ApplyCommittedAsync()).IsTrue();
        _ = await coordinator.CommitAsync(CreateMutation(2, ForeignOperationId, 12), StallTimeout, cancellationToken);

        var lookup = idempotency.Lookup(mutation.OperationScope, mutation.OperationId, mutation.OperationFingerprint.Span, out var record);
        _ = await Assert.That(lookup).IsEqualTo(GroupIdempotencyLookup.Found);
        await SequenceAssert.EqualMemoryAsync(mutation.OutcomePayload, record.OutcomePayload);
        await SequenceAssert.EqualAsync([1UL, 2UL], pipeline.AppliedIndexes());
    }

    /// <summary>
    /// A follower acknowledgement arriving after the commit budget gave up on the majority commits and applies the entry in the
    /// background, and resolves its idempotency record so a same-identity retry replays the outcome instead of staying unknown.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LateMajorityResolvesPendingApply(CancellationToken cancellationToken)
    {
        var pipeline = new ScriptedPipeline(false);
        var idempotency = new GroupIdempotencyState(4, TimeSpan.MaxValue);
        var budgetClock = new DueTimerClock(CommitBudget);
        var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(2, 0, 0, 1), pipeline, ReplicaFaultHooks.CreateNoOp(), idempotency)
        {
            BudgetTimeProvider = budgetClock,
        };
        var mutation = CreateMutation(1, "00000000000000000000000000000001", 11);
        var commit = coordinator.CommitAsync(mutation, CommitBudget, cancellationToken);
        try
        {
            await pipeline.FollowerEntered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            budgetClock.Advance(CommitBudget);
            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(commit);
            _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
            _ = await Assert.That(pipeline.Applied.IsEmpty).IsTrue();

            pipeline.ReleaseFollower();
        }
        finally
        {
            // Disposal drains the background follower observation, which owns the late-majority apply.
            await coordinator.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }

        var lookup = idempotency.Lookup(mutation.OperationScope, mutation.OperationId, mutation.OperationFingerprint.Span, out var record);
        _ = await Assert.That(lookup).IsEqualTo(GroupIdempotencyLookup.Found);
        await SequenceAssert.EqualMemoryAsync(mutation.OutcomePayload, record.OutcomePayload);
        await SequenceAssert.EqualAsync([1UL], pipeline.CommitIndexes.ToArray());
        await SequenceAssert.EqualAsync([1UL], pipeline.AppliedIndexes());
    }

    /// <summary>
    /// A later commit that re-applies an earlier entry runs it without the later commit's execution context, so the earlier entry does not
    /// observe the later caller's async-local scope (in production, the RPC idempotency scope that stamps its cache-WAL frame with a foreign
    /// operation id); the later commit's own entry keeps its scope.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReappliedEntryNotStampedWithForeignId(CancellationToken cancellationToken)
    {
        var pipeline = new ScriptedPipeline(true);
        await using var coordinator = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(2, 0, 0, 1),
            pipeline,
            ReplicaFaultHooks.CreateNoOp(),
            new GroupIdempotencyState(4, TimeSpan.MaxValue));
        pipeline.ReleaseFollower();
        var firstError = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(
            coordinator.CommitAsync(CreateMutation(1, "00000000000000000000000000000001", 11), StallTimeout, cancellationToken));
        _ = await Assert.That(firstError.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);

        CallerOperationId.Value = ForeignOperationId;
        try
        {
            _ = await coordinator.CommitAsync(CreateMutation(2, ForeignOperationId, 12), StallTimeout, cancellationToken);
        }
        finally
        {
            CallerOperationId.Value = null;
        }

        await SequenceAssert.EqualAsync([1UL, 2UL], pipeline.AppliedIndexes());
        _ = await Assert.That(pipeline.StampFor(1)).IsNull();
        _ = await Assert.That(pipeline.StampFor(2)).IsEqualTo(ForeignOperationId);
    }

    private static PreparedReplicaMutation CreateMutation(ulong logIndex, string operationId, byte outcome) => new(
        new ReplicaOperationIdentity("group-a", "client", operationId, new byte[] { 1 }),
        1,
        logIndex,
        new ReplicaMutationPayload(new byte[] { 2 }, new[] { outcome }, 4));

    /// <summary>
    /// Majority pipeline whose follower acknowledges once released, optionally fails its first memory apply, and records the commit
    /// indexes, the applied entries, and the caller operation id each apply ran under.
    /// </summary>
    [ThreadSafe]
    private sealed class ScriptedPipeline : IReplicaCommitPipeline
    {
        private readonly TaskCompletionSource _firstApplied = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _followerEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _followerReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _failFirstApply;

        internal ScriptedPipeline(bool failFirstApply)
        {
            _failFirstApply = failFirstApply ? 1 : 0;
        }

        internal ConcurrentQueue<(ulong Index, string? OperationId)> Applied { get; } = new();

        internal ConcurrentQueue<ulong> CommitIndexes { get; } = new();

        internal Task FirstApplied => _firstApplied.Task;

        /// <summary>Gets a task that completes when the follower append started and waits for its release.</summary>
        internal Task FollowerEntered => _followerEntered.Task;

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken)
        {
            CommitIndexes.Enqueue(commitIndex);
            return ValueTask.CompletedTask;
        }

        public async ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            // The follower ignores the majority budget, as a slow peer does: its acknowledgement arrives only once released.
            _ = _followerEntered.TrySetResult();
            await _followerReleased.Task.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            return new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true);
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            if (Interlocked.Exchange(ref _failFirstApply, 0) == 1)
                return ValueTask.FromException(new InvalidOperationException("Injected memory apply failure after the majority."));

            Applied.Enqueue((mutation.LogIndex, CallerOperationId.Value));
            _ = _firstApplied.TrySetResult();
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }

        internal ulong[] AppliedIndexes()
        {
            var applied = Applied.ToArray();
            var indexes = new ulong[applied.Length];
            for (var i = 0; i < applied.Length; i++)
                indexes[i] = applied[i].Index;

            return indexes;
        }

        internal void ReleaseFollower() => _ = _followerReleased.TrySetResult();

        internal string? StampFor(ulong index)
        {
            foreach (var (applied, operationId) in Applied)
            {
                if (applied == index)
                    return operationId;
            }

            throw new InvalidOperationException($"Entry {index} was never applied.");
        }
    }
}
