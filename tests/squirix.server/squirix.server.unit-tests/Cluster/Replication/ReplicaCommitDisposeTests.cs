using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Disposing an RF=2 commit coordinator while a commit past its majority is stuck in the memory apply.</summary>
[Immutable]
public sealed class ReplicaCommitDisposeTests
{
    private const int LeakedOnShutdownEventId = 4007;

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
        var log = new LeakRecordingLogger();
        var coordinator = CreateCoordinator(pipeline, log);
        var mutation = ReplicaCommitTestKit.CreateMutation();
        var commit = coordinator.CommitAsync(mutation, StallTimeout, CancellationToken.None).AsTask();
        try
        {
            await pipeline.ApplyEntered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            await coordinator.DisposeAsync();
            var refused = coordinator.CommitAsync(ReplicaCommitTestKit.CreateMutation(), StallTimeout, cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, ReadOnlyMemory<byte>>(refused);
            _ = await Assert.That(log.LeakCount).IsEqualTo(1);
            _ = await Assert.That(log.LeakLevel).IsEqualTo(LogLevel.Error);

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
    /// A commit queued for admission behind a stuck one still ends at its own budget after dispose gave up, instead of waiting forever on
    /// a semaphore disposed under it; the stuck commit then completes once the apply recovers.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeKeepsQueuedCommitCancelable(CancellationToken cancellationToken)
    {
        var pipeline = new StallingApplyPipeline();
        var coordinator = CreateCoordinator(pipeline, new LeakRecordingLogger());
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
        var log = new LeakRecordingLogger();
        var coordinator = CreateCoordinator(pipeline, log);
        pipeline.ReleaseApply();
        var mutation = ReplicaCommitTestKit.CreateMutation();
        var outcome = await coordinator.CommitAsync(mutation, StallTimeout, cancellationToken);

        await coordinator.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        await SequenceAssert.EqualMemoryAsync(mutation.OutcomePayload, outcome);
        _ = await Assert.That(log.LeakCount).IsEqualTo(0);
    }

    private static ReplicaCommitCoordinator CreateCoordinator(StallingApplyPipeline pipeline, ILogger log) =>
        new(new ReplicaCommitCoordinatorOptions(2, 0, 0, 1), pipeline, NoOpHooks.Instance, new GroupIdempotencyState(4, TimeSpan.MaxValue))
        {
            ShutdownBudget = ShutdownBudget,
            ShutdownLeakReporter = budget => LogManager.ReplicaCoordinatorLeakedOnShutdown(log, budget),
        };

    private static PreparedReplicaMutation CreateMutation(ulong logIndex, string operationId) => new(
        new ReplicaOperationIdentity("group-a", "client", operationId, new byte[] { 1 }),
        1,
        logIndex,
        new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 3 }, 4));

    private static Task<ReadOnlyMemory<byte>> StartCommitAsync(ReplicaCommitCoordinator coordinator, PreparedReplicaMutation mutation, TimeSpan timeout) =>
        coordinator.CommitAsync(mutation, timeout, CancellationToken.None).AsTask();

    /// <summary>Fault hooks that never interfere.</summary>
    [Immutable]
    private sealed class NoOpHooks : IReplicaCommitFaultHooks
    {
        internal static NoOpHooks Instance { get; } = new();

        public ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    /// <summary>Logger double recording the coordinator's shutdown leak event and its level.</summary>
    [ThreadSafe]
    private sealed class LeakRecordingLogger : ILogger
    {
        private readonly ConcurrentQueue<LogLevel> _leaks = new();

        internal int LeakCount => _leaks.Count;

        internal LogLevel? LeakLevel => _leaks.TryPeek(out var level) ? level : null;

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (eventId.Id == LeakedOnShutdownEventId)
                _leaks.Enqueue(logLevel);
        }
    }

    /// <summary>Majority pipeline whose follower acknowledges at once and whose memory apply stalls, ignoring cancellation, until released.</summary>
    [ThreadSafe]
    private sealed class StallingApplyPipeline : IReplicaCommitPipeline
    {
        private readonly TaskCompletionSource _applyEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _applyReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task ApplyEntered => _applyEntered.Task;

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true));

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = _applyEntered.TrySetResult();
            return new ValueTask(_applyReleased.Task);
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }

        internal void ReleaseApply() => _ = _applyReleased.TrySetResult();
    }
}
