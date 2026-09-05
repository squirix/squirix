using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Durable majority of pipeline ordering and ownership tests.</summary>
[Immutable]
public sealed class DurableReplicationPipelineTests : ServerUnitTestBase
{
    /// <summary>The golden trace proves every durable and memory boundary is ordered.</summary>
    [Fact]
    public async Task AppliesMemoryAfterMajorityCommit()
    {
        var pipeline = new RecordingPipeline(1);
        var hooks = new RecordingHooks(pipeline.Trace);
        var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(3, 0, 0, 4), pipeline, hooks, new GroupIdempotencyState(10, TimeSpan.MaxValue));
        var mutation = CreateMutation();
        try
        {
            var outcome = await coordinator.CommitAsync(mutation, TimeSpan.FromSeconds(5), DefaultCancellationToken);
            Assert.Equal(new byte[] { 7 }, outcome.ToArray());
            var retryOutcome = await coordinator.CommitAsync(mutation, TimeSpan.FromSeconds(5), DefaultCancellationToken);
            Assert.Equal(outcome.ToArray(), retryOutcome.ToArray());
            Assert.Equal(2, pipeline.FollowerCalls);
            Assert.Equal(1, pipeline.MemoryApplyCount);
            Assert.Equal(
                [
                    "stage:Prepared",
                    "local:1",
                    "stage:LocalAppendDurable",
                    "send:1",
                    "send:2",
                    "stage:FollowerFanOutStarted",
                    "stage:MajorityReached",
                    "commit:1",
                    "stage:CommitIndexDurable",
                    "apply:1",
                    "stage:MemoryApplied",
                    "stage:ResponseReady",
                ],
                pipeline.Trace);
            Assert.Contains(2, pipeline.LaggingReplicas);
        }
        finally
        {
            pipeline.ReleaseFollowers();
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>A catching-up follower contributes no quorum copy until a repair session marks it ready.</summary>
    [Fact]
    public async Task CatchingUpFollowerCountsOnlyWhenReady()
    {
        var eligibility = new ReplicaEligibility(3);
        var ready = Progress(1UL, 0UL, 0UL, 0UL, 1UL);
        Assert.True(eligibility.TryMarkReady(0, in ready, in ready));

        var stalledPipeline = new RecordingPipeline(1);
        var stalledHooks = new RecordingHooks(stalledPipeline.Trace);
        var stalled = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(3, 0, 0, 4),
            stalledPipeline,
            stalledHooks,
            new GroupIdempotencyState(10, TimeSpan.MaxValue),
            eligibility);
        try
        {
            var stalledCommit = stalled.CommitAsync(CreateMutation(), TimeSpan.FromMilliseconds(200), DefaultCancellationToken);
            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(stalledCommit);
            Assert.Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, error.Message, StringComparison.Ordinal);
        }
        finally
        {
            stalledPipeline.ReleaseFollowers();
            await stalled.DisposeAsync();
        }

        Assert.True(eligibility.TryMarkReady(1, in ready, in ready));
        var pipeline = new RecordingPipeline(1);
        var hooks = new RecordingHooks(pipeline.Trace);
        var coordinator = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(3, 0, 0, 4),
            pipeline,
            hooks,
            new GroupIdempotencyState(10, TimeSpan.MaxValue),
            eligibility);
        try
        {
            var outcome = await coordinator.CommitAsync(CreateMutation(), TimeSpan.FromSeconds(5), DefaultCancellationToken);
            Assert.Equal(new byte[] { 7 }, outcome.ToArray());
        }
        finally
        {
            pipeline.ReleaseFollowers();
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>Client cancellation after local durability does not abandon resolution or compensate memory.</summary>
    [Fact]
    public async Task CancellationKeepsResolutionOwned()
    {
        var pipeline = new RecordingPipeline(0);
        var hooks = new RecordingHooks(pipeline.Trace);
        var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(3, 0, 0, 4), pipeline, hooks, new GroupIdempotencyState(10, TimeSpan.MaxValue));
        var mutation = CreateMutation();
        using var cancellation = new CancellationTokenSource();
        try
        {
            var operation = coordinator.CommitAsync(mutation, TimeSpan.FromSeconds(5), cancellation.Token);
            _ = await pipeline.LocalAppended.Task.WaitAsync(DefaultCancellationToken);
            await cancellation.CancelAsync();
            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(operation);
            Assert.Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, error.Message, StringComparison.Ordinal);
            Assert.Equal(0, pipeline.MemoryApplyCount);

            pipeline.ReleaseFollowers();
            await coordinator.DisposeAsync();
            Assert.Equal(1, pipeline.MemoryApplyCount);
        }
        finally
        {
            pipeline.ReleaseFollowers();
        }
    }

    /// <summary>Disposal closes admission before draining an in-flight operation.</summary>
    [Fact]
    public async Task DisposeStopsAndDrains()
    {
        var pipeline = new RecordingPipeline(1, true);
        var hooks = new RecordingHooks(pipeline.Trace);
        var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(3, 0, 0, 4), pipeline, hooks, new GroupIdempotencyState(10, TimeSpan.MaxValue));
        Task? disposal = null;
        try
        {
            var currentOperation = coordinator.CommitAsync(CreateMutation(), TimeSpan.FromSeconds(5), DefaultCancellationToken);
            var current = currentOperation.AsTask();
            _ = await pipeline.LocalAppended.Task.WaitAsync(DefaultCancellationToken);
            disposal = coordinator.DisposeAsync().AsTask();
            Assert.False(disposal.IsCompleted);

            var rejected = coordinator.CommitAsync(CreateMutation(2, "fedcba9876543210fedcba9876543210"), TimeSpan.FromSeconds(5), DefaultCancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, ReadOnlyMemory<byte>>(rejected);

            pipeline.ReleaseLocalAppend();
            _ = await current;
            pipeline.ReleaseFollowers();
            await disposal;
        }
        finally
        {
            pipeline.ReleaseLocalAppend();
            pipeline.ReleaseFollowers();
            if (disposal == null)
                await coordinator.DisposeAsync();
        }
    }

    /// <summary>Follower work started before an exceptional exit remains owned until disposal.</summary>
    [Fact]
    public async Task ExceptionalFanOutStillOwnsFollowerTasks()
    {
        var pipeline = new RecordingPipeline(0);
        var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(3, 0, 0, 4), pipeline, ThrowOnFanOutHooks.Instance, new GroupIdempotencyState(10, TimeSpan.MaxValue));
        Task? disposal = null;
        try
        {
            var operation = coordinator.CommitAsync(CreateMutation(), TimeSpan.FromSeconds(5), DefaultCancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(operation);

            disposal = coordinator.DisposeAsync().AsTask();
            Assert.False(disposal.IsCompleted);
            pipeline.ReleaseFollowers();
            await disposal;
        }
        finally
        {
            pipeline.ReleaseFollowers();
            if (disposal == null)
                await coordinator.DisposeAsync();
        }
    }

    /// <summary>A late acknowledgement advances its replica before that replica's next acknowledgement is released.</summary>
    [Fact]
    public async Task LateAcknowledgementSupportsNextCommit()
    {
        var pipeline = new LateFollowerPipeline();
        var coordinator = CreateCoordinator(5, pipeline);
        try
        {
            _ = await coordinator.CommitAsync(CreateMutation(), TimeSpan.FromSeconds(2), DefaultCancellationToken);
            var second = coordinator.CommitAsync(CreateMutation(2, "00000000000000000000000000000002"), TimeSpan.FromSeconds(2), DefaultCancellationToken);
            await pipeline.SecondReplicaThreeStarted.WaitAsync(DefaultCancellationToken);

            pipeline.ReleaseFirstReplicaThree();
            await pipeline.FirstReplicaThreeAcknowledged.WaitAsync(DefaultCancellationToken);

            // FirstReplicaThreeAcknowledged fires when the pipeline produces the index 1
            // acknowledgement, before the coordinator records it through TryRecord on the
            // background observe path. Wait until replica 3's match index actually advances
            // to 1 so the buffered index 2 acknowledgement cannot overtake it.
            await WaitForMatchIndexAsync(coordinator, 3, 1, DefaultCancellationToken);
            pipeline.ReleaseSecondReplicaThree();

            _ = await second;
            Assert.Equal(2, pipeline.MemoryApplyCount);
        }
        finally
        {
            pipeline.ReleaseFirstReplicaThree();
            pipeline.ReleaseSecondReplicaThree();
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>A follower response completing after the deadline is still recorded, not marked lagging.</summary>
    [Fact]
    public async Task LateFollowerResponseIsRecorded()
    {
        var pipeline = new DeferredFollowersPipeline();
        var coordinator = CreateCoordinator(3, pipeline);
        try
        {
            var operation = coordinator.CommitAsync(CreateMutation(), TimeSpan.FromMilliseconds(100), DefaultCancellationToken);
            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(operation);
            Assert.Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, error.Message, StringComparison.Ordinal);

            // Wait for the observable deadline signal so late responses truly arrive after the deadline.
            await pipeline.DeadlineElapsed.WaitAsync(DefaultCancellationToken);
            pipeline.ReleaseFollowers(CreateMutation());
        }
        finally
        {
            await coordinator.DisposeAsync();
        }

        Assert.Equal(2, pipeline.FollowerCalls);
        Assert.Equal([], pipeline.LaggingReplicas);
    }

    /// <summary>A later commit applies skipped earlier entries in index order.</summary>
    [Fact]
    public async Task LaterCommitAppliesSkippedEntries()
    {
        var pipeline = new GatedFollowersPipeline();
        var coordinator = CreateCoordinator(3, pipeline, ThrowOnFirstMajorityHooks.Instance);
        try
        {
            // Release up front so every acknowledgement is recorded in the foreground drain loop.
            // The injected post-majority failure leaves index 1 retained but unapplied without
            // relying on the racy 100ms-budget-versus-background-observe ordering.
            pipeline.ReleaseFollowers();
            var first = coordinator.CommitAsync(CreateMutation(1, "00000000000000000000000000000001"), TimeSpan.FromSeconds(2), DefaultCancellationToken);
            var firstError = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(first);
            Assert.Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, firstError.Message, StringComparison.Ordinal);

            var outcome = await coordinator.CommitAsync(CreateMutation(2, "00000000000000000000000000000002"), TimeSpan.FromSeconds(2), DefaultCancellationToken);
            Assert.Equal(new byte[] { 7 }, outcome.ToArray());
        }
        finally
        {
            await coordinator.DisposeAsync();
        }

        Assert.Equal([1UL, 2UL], pipeline.AppliedIndexes);
    }

    /// <summary>A memory-apply failure after the commit index advances is retried by the next commit.</summary>
    [Fact]
    public async Task FailedMemoryApplyIsRetriedByLaterCommit()
    {
        var pipeline = new FlakyMemoryPipeline();
        var coordinator = CreateCoordinator(3, pipeline);
        try
        {
            var first = coordinator.CommitAsync(CreateMutation(1, "00000000000000000000000000000001"), TimeSpan.FromSeconds(2), DefaultCancellationToken);
            var firstError = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(first);
            Assert.Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, firstError.Message, StringComparison.Ordinal);

            var outcome = await coordinator.CommitAsync(CreateMutation(2, "00000000000000000000000000000002"), TimeSpan.FromSeconds(2), DefaultCancellationToken);
            Assert.Equal(new byte[] { 7 }, outcome.ToArray());
        }
        finally
        {
            await coordinator.DisposeAsync();
        }

        Assert.Equal([1UL, 2UL], pipeline.AppliedIndexes);
    }

    /// <summary>A recovered uncommitted tail must be reconciled before new writes are admitted.</summary>
    [Fact]
    public void RejectsUnreconciledDurableTail()
    {
        var pipeline = new RecordingPipeline(1);
        var hooks = new RecordingHooks(pipeline.Trace);

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(
            pipeline,
            hooks,
            static (value, faultHooks) => _ = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(3, 2, 1, 4), value, faultHooks, new GroupIdempotencyState(10, TimeSpan.MaxValue)));
    }

    /// <summary>One shared task instance cannot count as acknowledgements from multiple replicas.</summary>
    [Fact]
    public async Task SharedFollowerTaskCountsOnce()
    {
        var pipeline = new SharedFollowerTaskPipeline();
        var coordinator = CreateCoordinator(5, pipeline);
        try
        {
            var operation = coordinator.CommitAsync(CreateMutation(), TimeSpan.FromSeconds(2), DefaultCancellationToken);

            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(operation);
            Assert.Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, error.Message, StringComparison.Ordinal);
            Assert.Equal(0, pipeline.MemoryApplyCount);
        }
        finally
        {
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>The timeout budget includes admission and local durable append work.</summary>
    [Fact]
    public async Task TimeoutIncludesLocalAppend()
    {
        var pipeline = new RecordingPipeline(1, true);
        var hooks = new RecordingHooks(pipeline.Trace);
        var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(3, 0, 0, 4), pipeline, hooks, new GroupIdempotencyState(10, TimeSpan.MaxValue));
        try
        {
            var operation = coordinator.CommitAsync(CreateMutation(), TimeSpan.FromMilliseconds(100), DefaultCancellationToken);
            _ = await pipeline.LocalAppended.Task.WaitAsync(DefaultCancellationToken);
            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, ReadOnlyMemory<byte>>(operation);
            Assert.Equal(0, pipeline.MemoryApplyCount);
        }
        finally
        {
            pipeline.ReleaseLocalAppend();
            await coordinator.DisposeAsync();
        }
    }

    private static ReplicaCommitCoordinator CreateCoordinator(int replicaCount, IReplicaCommitPipeline pipeline, IReplicaCommitFaultHooks? hooks = null) => new(
        new ReplicaCommitCoordinatorOptions(replicaCount, 0, 0, 8),
        pipeline,
        hooks ?? NoOpHooks.Instance,
        new GroupIdempotencyState(16, TimeSpan.MaxValue));

    private static PreparedReplicaMutation CreateMutation(ulong logIndex = 1, string operationId = "0123456789abcdef0123456789abcdef") => new(
        new ReplicaOperationIdentity("group-a", "client", operationId, new byte[] { 1, 2, 3 }),
        1,
        logIndex,
        new ReplicaMutationPayload(new byte[] { 4, 5, 6 }, new byte[] { 7 }, 42),
        0);

    private static ReplicaProgress Progress(ulong nextIndex, ulong matchIndex, ulong commitIndex, ulong appliedIndex, ulong lastTerm) => new(
        nextIndex,
        matchIndex,
        commitIndex,
        appliedIndex,
        lastTerm,
        new byte[] { 9 },
        1UL,
        7U);

    private static async Task WaitForMatchIndexAsync(ReplicaCommitCoordinator coordinator, int replicaIndex, ulong matchIndex, CancellationToken cancellationToken)
    {
        while (coordinator.MatchIndexFor(replicaIndex) < matchIndex)
            await Task.Delay(5, cancellationToken).ConfigureAwait(false);
    }

    [Mutable]
    private sealed class DeferredFollowersPipeline : IReplicaCommitPipeline
    {
        private readonly TaskCompletionSource<bool> _deadlineElapsed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<ReplicaDurableAcknowledgement> _first = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<ReplicaDurableAcknowledgement> _second = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task DeadlineElapsed => _deadlineElapsed.Task;

        internal int FollowerCalls { get; private set; }

        internal List<int> LaggingReplicas { get; } = [];

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken)
        {
            _ = commitIndex;
            _ = cancellationToken;
            return ValueTask.CompletedTask;
        }

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = mutation;
            _ = cancellationToken.Register(() => _ = _deadlineElapsed.TrySetResult(true));
            FollowerCalls++;
            return new ValueTask<ReplicaDurableAcknowledgement>(replicaIndex == 1 ? _first.Task : _second.Task);
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ApplyMemoryAsync(mutation, cancellationToken);

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = mutation;
            _ = cancellationToken;
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
            _ = logIndex;
            LaggingReplicas.Add(replicaIndex);
        }

        internal void ReleaseFollowers(PreparedReplicaMutation mutation)
        {
            var acknowledgement = new ReplicaDurableAcknowledgement(
                mutation.GroupId,
                mutation.Term,
                mutation.LogIndex,
                mutation.OperationFingerprint,
                mutation.PayloadChecksum,
                true,
                true);
            _ = _first.TrySetResult(acknowledgement);
            _ = _second.TrySetResult(acknowledgement);
        }
    }

    [Mutable]
    private sealed class GatedFollowersPipeline : IReplicaCommitPipeline
    {
        private readonly TaskCompletionSource<bool> _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal List<ulong> AppliedIndexes { get; } = [];

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken)
        {
            _ = commitIndex;
            _ = cancellationToken;
            return ValueTask.CompletedTask;
        }

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = replicaIndex;
            return new ValueTask<ReplicaDurableAcknowledgement>(WaitAndAcknowledgeAsync(mutation, cancellationToken));
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = mutation;
            _ = cancellationToken;
            return ValueTask.CompletedTask;
        }

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            AppliedIndexes.Add(mutation.LogIndex);
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
            _ = replicaIndex;
            _ = logIndex;
        }

        internal void ReleaseFollowers() => _ = _released.TrySetResult(true);

        private async Task<ReplicaDurableAcknowledgement> WaitAndAcknowledgeAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = await _released.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true);
        }
    }

    [Mutable]
    private sealed class FlakyMemoryPipeline : IReplicaCommitPipeline
    {
        private bool _failedOnce;

        internal List<ulong> AppliedIndexes { get; } = [];

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken)
        {
            _ = commitIndex;
            _ = cancellationToken;
            return ValueTask.CompletedTask;
        }

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = replicaIndex;
            _ = cancellationToken;
            return ValueTask.FromResult(
                new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true));
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = mutation;
            _ = cancellationToken;
            return ValueTask.CompletedTask;
        }

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            if (!_failedOnce)
            {
                _failedOnce = true;
                return ValueTask.FromException(new InvalidOperationException("Injected memory-apply failure after commit index advanced."));
            }

            AppliedIndexes.Add(mutation.LogIndex);
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
            _ = replicaIndex;
            _ = logIndex;
        }
    }

    [Mutable]
    private sealed class LateFollowerPipeline : IReplicaCommitPipeline
    {
        private readonly TaskCompletionSource<bool> _firstReplicaThree = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _firstReplicaThreeAcknowledged = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _secondReplicaThree = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _secondReplicaThreeStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task FirstReplicaThreeAcknowledged => _firstReplicaThreeAcknowledged.Task;

        internal int MemoryApplyCount { get; private set; }

        internal Task SecondReplicaThreeStarted => _secondReplicaThreeStarted.Task;

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            if (replicaIndex == 3)
                return new ValueTask<ReplicaDurableAcknowledgement>(AcknowledgeReplicaThreeAsync(mutation, cancellationToken));

            var ready = mutation.LogIndex == 1 || replicaIndex == 1;
            return ValueTask.FromResult(BuildAcknowledgement(mutation, ready));
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            MemoryApplyCount++;
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }

        internal void ReleaseFirstReplicaThree() => _ = _firstReplicaThree.TrySetResult(true);

        internal void ReleaseSecondReplicaThree() => _ = _secondReplicaThree.TrySetResult(true);

        private static ReplicaDurableAcknowledgement BuildAcknowledgement(PreparedReplicaMutation mutation) => BuildAcknowledgement(mutation, true);

        private static ReplicaDurableAcknowledgement BuildAcknowledgement(PreparedReplicaMutation mutation, bool ready) => new(
            mutation.GroupId,
            mutation.Term,
            mutation.LogIndex,
            mutation.OperationFingerprint,
            mutation.PayloadChecksum,
            true,
            ready);

        private async Task<ReplicaDurableAcknowledgement> AcknowledgeReplicaThreeAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            if (mutation.LogIndex == 1)
            {
                _ = await _firstReplicaThree.Task.WaitAsync(cancellationToken);
                _ = _firstReplicaThreeAcknowledged.TrySetResult(true);
            }
            else
            {
                _ = _secondReplicaThreeStarted.TrySetResult(true);
                _ = await _firstReplicaThree.Task.WaitAsync(cancellationToken);
                _ = await _secondReplicaThree.Task.WaitAsync(cancellationToken);
            }

            return BuildAcknowledgement(mutation);
        }
    }

    private sealed class NoOpHooks : IReplicaCommitFaultHooks
    {
        internal static NoOpHooks Instance { get; } = new();

        public ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    [Mutable]
    private sealed class RecordingHooks : IReplicaCommitFaultHooks
    {
        private readonly List<string> _trace;

        internal RecordingHooks(List<string> trace)
        {
            _trace = trace;
        }

        public ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _trace.Add($"stage:{stage}");
            return ValueTask.CompletedTask;
        }
    }

    [Mutable]
    private sealed class RecordingPipeline : IReplicaCommitPipeline
    {
        private readonly bool _blockLocalAppend;

        private readonly TaskCompletionSource<ReplicaDurableAcknowledgement> _followers = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly int _immediateFollowerIndex;

        private readonly TaskCompletionSource<bool> _localAppendRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private PreparedReplicaMutation? _recordedMutation;

        internal RecordingPipeline(int immediateFollowerIndex, bool blockLocalAppend = false)
        {
            _immediateFollowerIndex = immediateFollowerIndex;
            _blockLocalAppend = blockLocalAppend;
        }

        internal int FollowerCalls { get; private set; }

        internal List<int> LaggingReplicas { get; } = [];

        internal TaskCompletionSource<bool> LocalAppended { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal int MemoryApplyCount { get; private set; }

        internal List<string> Trace { get; } = [];

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken)
        {
            Trace.Add($"commit:{commitIndex}");
            return ValueTask.CompletedTask;
        }

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            FollowerCalls++;
            Trace.Add($"send:{replicaIndex}");
            _recordedMutation ??= mutation;
            return replicaIndex == _immediateFollowerIndex ? ValueTask.FromResult(CreateReadyAcknowledgement(mutation))
                : new ValueTask<ReplicaDurableAcknowledgement>(_followers.Task);
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            Trace.Add($"local:{mutation.LogIndex}");
            _ = LocalAppended.TrySetResult(true);
            return _blockLocalAppend ? new ValueTask(_localAppendRelease.Task.WaitAsync(cancellationToken)) : ValueTask.CompletedTask;
        }

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            MemoryApplyCount++;
            Trace.Add($"apply:{mutation.LogIndex}");
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex) => LaggingReplicas.Add(replicaIndex);

        internal void ReleaseFollowers()
        {
            if (_recordedMutation is not { } recorded)
                return;

            _ = _followers.TrySetResult(CreateReadyAcknowledgement(recorded));
        }

        internal void ReleaseLocalAppend() => _ = _localAppendRelease.TrySetResult(true);

        private static ReplicaDurableAcknowledgement CreateReadyAcknowledgement(PreparedReplicaMutation mutation) => new(
            mutation.GroupId,
            mutation.Term,
            mutation.LogIndex,
            mutation.OperationFingerprint,
            mutation.PayloadChecksum,
            true,
            true);
    }

    [Mutable]
    private sealed class SharedFollowerTaskPipeline : IReplicaCommitPipeline
    {
        private Task<ReplicaDurableAcknowledgement>? _shared;

        internal int MemoryApplyCount { get; private set; }

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _shared ??= Task.FromResult(BuildAcknowledgement(mutation));
            return new ValueTask<ReplicaDurableAcknowledgement>(_shared);
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            MemoryApplyCount++;
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }

        private static ReplicaDurableAcknowledgement BuildAcknowledgement(PreparedReplicaMutation mutation) => new(
            mutation.GroupId,
            mutation.Term,
            mutation.LogIndex,
            mutation.OperationFingerprint,
            mutation.PayloadChecksum,
            true,
            true);
    }

    private sealed class ThrowOnFanOutHooks : IReplicaCommitFaultHooks
    {
        internal static ThrowOnFanOutHooks Instance { get; } = new();

        public ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = mutation;
            _ = cancellationToken;
            var exception = new InvalidOperationException("Injected fan-out failure.");
            return stage == ReplicaCommitStage.FollowerFanOutStarted ? ValueTask.FromException(exception) : ValueTask.CompletedTask;
        }
    }

    private sealed class ThrowOnFirstMajorityHooks : IReplicaCommitFaultHooks
    {
        internal static ThrowOnFirstMajorityHooks Instance { get; } = new();

        public ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            var exception = new InvalidOperationException("Injected post-majority failure.");
            return stage == ReplicaCommitStage.MajorityReached && mutation.LogIndex == 1 ? ValueTask.FromException(exception) : ValueTask.CompletedTask;
        }
    }
}
