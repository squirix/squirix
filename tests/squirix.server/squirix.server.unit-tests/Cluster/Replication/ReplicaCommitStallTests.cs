using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>RF=2 commits whose post-majority memory apply stalls in journal I/O past the commit budget.</summary>
[Immutable]
public sealed class ReplicaCommitStallTests : IsolatedStorageTestBase
{
    private const string AppliedKey = "replica-a";

    private static readonly TimeSpan CommitBudget = TimeSpan.FromMilliseconds(300);

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// An entry whose first apply failed after its majority is re-applied by the next commit; a retry of its operation then replays the
    /// committed outcome instead of reporting an unknown outcome, and the entry is not applied again.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReappliedPendingEntryResolvesIdempotency(CancellationToken cancellationToken)
    {
        var pipeline = new FailFirstApplyPipeline();
        await using var coordinator = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(2, 0, 0, 1),
            pipeline,
            CancellationHonoringHooks.Instance,
            new GroupIdempotencyState(4, TimeSpan.MaxValue));
        var first = CreateMutation(1, "00000000000000000000000000000001", 11);
        var firstError = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(coordinator.CommitAsync(first, StallTimeout, cancellationToken));
        _ = await Assert.That(firstError.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);

        _ = await coordinator.CommitAsync(CreateMutation(2, "00000000000000000000000000000002", 12), StallTimeout, cancellationToken);
        var retried = await coordinator.CommitAsync(first, StallTimeout, cancellationToken);

        await SequenceAssert.EqualMemoryAsync(first.OutcomePayload, retried);
        await SequenceAssert.EqualAsync([1UL, 2UL], pipeline.Applied);
    }

    /// <summary>
    /// A majority-acknowledged commit whose local apply is stuck in fsync past the budget completes once the disk recovers, with memory
    /// applied, instead of reporting an unknown outcome: no step after the majority observes the budget.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StallAfterMajorityCompletesWithLatency(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var pipeline = new JournalApplyPipeline(journal.Journal);
        await using var coordinator = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(2, 0, 0, 1),
            pipeline,
            CancellationHonoringHooks.Instance,
            new GroupIdempotencyState(4, TimeSpan.MaxValue));
        var mutation = ReplicaCommitTestKit.CreateMutation();
        journal.Writer.Flush.Arm();

        var commit = coordinator.CommitAsync(mutation, CommitBudget, CancellationToken.None).AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var applyCanceled = await pipeline.ApplyBudgetExpiredWithinAsync(CommitBudget * 3, cancellationToken);
        journal.Writer.Flush.Release();
        var outcome = await commit;

        await SequenceAssert.EqualMemoryAsync(mutation.OutcomePayload, outcome);
        _ = await Assert.That(pipeline.Memory.Snapshot).IsEqualTo(CacheKey.Default(AppliedKey).ToString());
        _ = await Assert.That(applyCanceled).IsFalse();
    }

    private static PreparedReplicaMutation CreateMutation(ulong logIndex, string operationId, byte outcome) => new(
        new ReplicaOperationIdentity("group-a", "client", operationId, new byte[] { 1 }),
        1,
        logIndex,
        new ReplicaMutationPayload(new byte[] { 2 }, new[] { outcome }, 4));

    /// <summary>Fault hooks that honor their token, as any budget-aware step would: a canceled token faults the stage.</summary>
    [Immutable]
    private sealed class CancellationHonoringHooks : IReplicaCommitFaultHooks
    {
        internal static CancellationHonoringHooks Instance { get; } = new();

        public ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
            cancellationToken.IsCancellationRequested ? ValueTask.FromCanceled(cancellationToken) : ValueTask.CompletedTask;
    }

    /// <summary>Majority pipeline whose follower acknowledges at once and whose first memory apply fails.</summary>
    [Mutable]
    private sealed class FailFirstApplyPipeline : IReplicaCommitPipeline
    {
        private bool _failed;

        internal List<ulong> Applied { get; } = [];

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
            ValueTask.FromResult(
                new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true));

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            if (!_failed)
            {
                _failed = true;
                return ValueTask.FromException(new InvalidOperationException("Injected memory apply failure after the majority."));
            }

            Applied.Add(mutation.LogIndex);
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }
    }

    /// <summary>Majority pipeline whose follower acknowledges at once and whose memory apply runs a real durable mutation through the journal.</summary>
    [Mutable]
    private sealed class JournalApplyPipeline : IReplicaCommitPipeline
    {
        private readonly TaskCompletionSource _applyBudgetExpired = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly DurableMutationExecutor _executor;
        private readonly JournalCoordinator _journal;

        internal JournalApplyPipeline(JournalCoordinator journal)
        {
            _journal = journal;
            _executor = new DurableMutationExecutor(journal);
        }

        internal AppliedKeys Memory { get; } = new();

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
            ValueTask.FromResult(
                new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true));

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public async ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            await using var budgetWatch = cancellationToken.Register(
                static state =>
                {
                    if (state is TaskCompletionSource signal)
                        _ = signal.TrySetResult();
                },
                _applyBudgetExpired);
            _ = await Memory.PutAsync(_executor, _journal, AppliedKey, cancellationToken);
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }

        /// <summary>
        /// Returns once the budget token handed to the apply is canceled, or after <paramref name="window" /> when the apply runs without
        /// a cancelable budget; the stall then outlasts the budget either way.
        /// </summary>
        /// <param name="window">Wait used when the apply token never cancels.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns><see langword="true" /> when the apply token was canceled.</returns>
        internal Task<bool> ApplyBudgetExpiredWithinAsync(TimeSpan window, CancellationToken cancellationToken) =>
            StallableJournal.CompletesWithinAsync(_applyBudgetExpired, window, cancellationToken);
    }
}
