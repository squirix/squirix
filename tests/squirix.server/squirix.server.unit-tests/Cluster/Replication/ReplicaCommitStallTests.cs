using System;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
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
    /// A majority-acknowledged commit whose local apply is stuck in fsync past the budget completes once the disk recovers, with memory
    /// applied, instead of reporting an unknown outcome.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Skip("Fails until #677")]
    public async Task StallAfterMajorityCompletesWithLatency(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var pipeline = new JournalApplyPipeline(journal.Journal);
        var hooks = new IReplicaCommitFaultHooksCreateExpectations();
        _ = hooks.Setups.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        await using var coordinator = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(2, 0, 0, 1),
            pipeline,
            hooks.Instance(),
            new GroupIdempotencyState(4, TimeSpan.MaxValue));
        var mutation = ReplicaCommitTestKit.CreateMutation();
        journal.Writer.Flush.Arm();

        var commit = coordinator.CommitAsync(mutation, CommitBudget, CancellationToken.None).AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        _ = await pipeline.ApplyBudgetExpiredWithinAsync(CommitBudget * 3, cancellationToken);
        journal.Writer.Flush.Release();
        var outcome = await commit;

        await SequenceAssert.EqualMemoryAsync(mutation.OutcomePayload, outcome);
        _ = await Assert.That(pipeline.Memory.Snapshot).IsEqualTo(CacheKey.Default(AppliedKey).ToString());
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
            ValueTask.FromResult(new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true));

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
