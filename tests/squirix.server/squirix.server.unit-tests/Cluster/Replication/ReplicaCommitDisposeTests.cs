using System;
using System.Collections.Concurrent;
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

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Disposing an RF=2 commit coordinator while a commit past its majority is stuck in the memory apply.</summary>
[Immutable]
public sealed class ReplicaCommitDisposeTests
{
    private static readonly TimeSpan QueuedBudget = TimeSpan.FromMilliseconds(300);

    private static readonly TimeSpan ShutdownBudget = TimeSpan.FromMilliseconds(200);

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Dispose gives up on a commit stuck after its majority within the shutdown budget, leaks the gates and the sequencer loudly instead
    /// of disposing them under the commit, and the commit then completes with its outcome once the apply recovers.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeDoesNotTearDownLiveCommit(CancellationToken cancellationToken)
    {
        var pipeline = new StallingApplyPipeline();
        var leaks = new LeakRecorder();
        var coordinator = CreateCoordinator(pipeline, leaks);
        var mutation = ReplicaMutationTestKit.CreateMutation();
        var commit = coordinator.CommitAsync(mutation, StallTimeout, CancellationToken.None).AsTask();
        try
        {
            await pipeline.ApplyEntered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            await coordinator.DisposeAsync();
            var refused = coordinator.CommitAsync(ReplicaMutationTestKit.CreateMutation(), StallTimeout, cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, ReadOnlyMemory<byte>>(refused);
            _ = await Assert.That(leaks.Count).IsEqualTo(1);
            _ = await Assert.That(leaks.FirstBudget).IsEqualTo(ShutdownBudget);

            pipeline.ReleaseApply();
            var outcome = await commit;
            await SequenceAssert.EqualMemoryAsync(mutation.OutcomePayload, outcome);
        }
        finally
        {
            pipeline.ReleaseApply();
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>
    /// Dispose does not tear the gates and the sequencer down under an apply of committed entries running outside any commit, reports the
    /// leak, and refuses a new apply; the running apply then completes once the memory apply recovers.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeDoesNotTearDownLiveApply(CancellationToken cancellationToken)
    {
        var pipeline = new StallingApplyPipeline(true);
        var leaks = new LeakRecorder();
        var coordinator = CreateCoordinator(pipeline, leaks);
        try
        {
            // The first apply fails after the majority: the commit ends unknown and its entry stays pending, with no commit left running.
            var commit = StartCommitAsync(coordinator, ReplicaMutationTestKit.CreateMutation(), StallTimeout);
            _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(commit.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            var apply = coordinator.ApplyCommittedAsync();
            await pipeline.ApplyEntered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            await coordinator.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(coordinator.ApplyCommittedAsync());
            _ = await Assert.That(leaks.Count).IsEqualTo(1);
            _ = await Assert.That(leaks.FirstBudget).IsEqualTo(ShutdownBudget);

            pipeline.ReleaseApply();
            _ = await Assert.That(await apply.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken)).IsTrue();
        }
        finally
        {
            pipeline.ReleaseApply();
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>A commit abandoned by dispose that fails after the shutdown budget is reported to the owner instead of only being observed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AbandonedFaultIsReported(CancellationToken cancellationToken)
    {
        var pipeline = new StallingApplyPipeline();
        var faults = new FaultRecorder();
        var coordinator = CreateCoordinator(pipeline, new LeakRecorder(), faults.Report);
        try
        {
            var commit = StartCommitAsync(coordinator, ReplicaMutationTestKit.CreateMutation(), StallTimeout);
            await pipeline.ApplyEntered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            await coordinator.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            pipeline.FailApply(new IOException("Injected memory apply failure."));
            _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(commit.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

            var reported = await faults.Reported.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(reported.GetBaseException()).IsTypeOf<IOException>();
        }
        finally
        {
            pipeline.ReleaseApply();
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>
    /// A late majority applied by the background follower observation that is still stuck when dispose gives up is reported as a leak, and
    /// its failure afterwards, even an <see cref="ObjectDisposedException" /> from a resource the host disposed, reaches the owner's fault reporter.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AbandonedObserverFaultIsReported(CancellationToken cancellationToken)
    {
        var pipeline = new StallingApplyPipeline(false, true);
        var leaks = new LeakRecorder();
        var faults = new FaultRecorder();
        var coordinator = CreateCoordinator(pipeline, leaks, faults.Report);
        try
        {
            // The commit gives up before the follower answers; the late answer completes the majority in the background.
            var commit = StartCommitAsync(coordinator, ReplicaMutationTestKit.CreateMutation(), QueuedBudget);
            _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(commit.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            pipeline.ReleaseFollower();
            await pipeline.ApplyEntered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            await coordinator.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(leaks.Count).IsEqualTo(1);

            pipeline.FailApply(new ObjectDisposedException("log"));

            var reported = await faults.Reported.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(reported.GetBaseException()).IsTypeOf<ObjectDisposedException>();
        }
        finally
        {
            pipeline.ReleaseFollower();
            pipeline.ReleaseApply();
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>
    /// A commit queued for admission behind a stuck one still ends at its own budget after dispose gave up, instead of waiting forever on
    /// a semaphore disposed under it; the stuck commit then completes once the apply recovers.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeKeepsQueuedCommitCancelable(CancellationToken cancellationToken)
    {
        var pipeline = new StallingApplyPipeline();
        var coordinator = CreateCoordinator(pipeline, new LeakRecorder());
        var stuck = StartCommitAsync(coordinator, CreateMutation(1, "00000000000000000000000000000001"), StallTimeout);
        try
        {
            await pipeline.ApplyEntered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var queued = StartCommitAsync(coordinator, CreateMutation(2, "00000000000000000000000000000002"), QueuedBudget);

            await coordinator.DisposeAsync();
            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(queued.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

            pipeline.ReleaseApply();
            _ = await stuck;
        }
        finally
        {
            pipeline.ReleaseApply();
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>Disposing a coordinator whose commits all finished releases its gates without reporting a leak.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IdleDisposeDoesNotReportLeak(CancellationToken cancellationToken)
    {
        var pipeline = new StallingApplyPipeline();
        var leaks = new LeakRecorder();
        var coordinator = CreateCoordinator(pipeline, leaks);
        pipeline.ReleaseApply();
        var mutation = ReplicaMutationTestKit.CreateMutation();
        var outcome = await coordinator.CommitAsync(mutation, StallTimeout, cancellationToken);

        await coordinator.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        await SequenceAssert.EqualMemoryAsync(mutation.OutcomePayload, outcome);
        _ = await Assert.That(leaks.Count).IsEqualTo(0);
    }

    private static ReplicaCommitCoordinator CreateCoordinator(StallingApplyPipeline pipeline, LeakRecorder leaks, Action<Exception>? faults = null) =>
        new(new ReplicaCommitCoordinatorOptions(2, 0, 0, 1), pipeline, ReplicaFaultHooks.CreateNoOp(), new GroupIdempotencyState(4, TimeSpan.MaxValue))
        {
            ShutdownBudget = ShutdownBudget,
            ShutdownLeakReporter = leaks.Report,
            AbandonedWorkFaultReporter = faults,
        };

    private static PreparedReplicaMutation CreateMutation(ulong logIndex, string operationId) => new(
        new ReplicaOperationIdentity("group-a", "client", operationId, new byte[] { 1 }),
        1,
        logIndex,
        new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 3 }, 4));

    private static Task<ReadOnlyMemory<byte>> StartCommitAsync(ReplicaCommitCoordinator coordinator, PreparedReplicaMutation mutation, TimeSpan timeout) =>
        coordinator.CommitAsync(mutation, timeout, CancellationToken.None).AsTask();

    /// <summary>Abandoned-work fault reporter double keeping the first reported fault.</summary>
    [ThreadSafe]
    private sealed class FaultRecorder
    {
        private readonly TaskCompletionSource<Exception> _reported = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task<Exception> Reported => _reported.Task;

        internal void Report(Exception fault) => _ = _reported.TrySetResult(fault);
    }

    /// <summary>Shutdown leak reporter double recording the budget of every leak the coordinator reports.</summary>
    [ThreadSafe]
    private sealed class LeakRecorder
    {
        private readonly ConcurrentQueue<TimeSpan> _budgets = new();

        internal int Count => _budgets.Count;

        internal TimeSpan? FirstBudget => _budgets.TryPeek(out var budget) ? budget : null;

        internal void Report(TimeSpan budget) => _budgets.Enqueue(budget);
    }

    /// <summary>Majority pipeline whose follower acknowledges at once and whose memory apply stalls, ignoring cancellation, until released.</summary>
    [ThreadSafe]
    private sealed class StallingApplyPipeline : IReplicaCommitPipeline
    {
        private readonly bool _failFirstApply;
        private readonly TaskCompletionSource _followerReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly bool _holdFollower;
        private readonly TaskCompletionSource _applyEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _applyReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private int _applyCalls;

        internal StallingApplyPipeline(bool failFirstApply = false, bool holdFollower = false)
        {
            _failFirstApply = failFirstApply;
            _holdFollower = holdFollower;
            if (!holdFollower)
                _ = _followerReleased.TrySetResult();
        }

        /// <summary>Gets a task that completes when an apply stalled.</summary>
        internal Task ApplyEntered => _applyEntered.Task;

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public async ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            // A held follower ignores the commit budget, as a slow peer does: its acknowledgement arrives only once released.
            if (_holdFollower)
                await _followerReleased.Task.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            return new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true);
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            if (_failFirstApply && Interlocked.Increment(ref _applyCalls) == 1)
                return ValueTask.FromException(new InvalidOperationException("Injected memory apply failure after the majority."));

            _ = _applyEntered.TrySetResult();
            return new ValueTask(_applyReleased.Task);
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }

        internal void FailApply(Exception error) => _ = _applyReleased.TrySetException(error);

        internal void ReleaseFollower() => _ = _followerReleased.TrySetResult();

        internal void ReleaseApply() => _ = _applyReleased.TrySetResult();
    }
}
