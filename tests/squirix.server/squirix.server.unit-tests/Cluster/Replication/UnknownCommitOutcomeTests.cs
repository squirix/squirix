using System;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Pre-append release and post-append ambiguity contract tests.</summary>
[Immutable]
public sealed class UnknownCommitOutcomeTests : ServerUnitTestBase
{
    /// <summary>A duplicate that joins after local durability shares the original append boundary.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DuplicateRetryKeepsAppendBoundary(CancellationToken cancellationToken)
    {
        var pipeline = new ReplicaCommitTestKit.Pipeline(blockFollowers: true);
        await using var coordinator = ReplicaCommitTestKit.CreateCoordinator(pipeline);
        var mutation = ReplicaCommitTestKit.CreateMutation();
        var first = coordinator.CommitAsync(mutation, TimeSpan.FromSeconds(1), cancellationToken);
        await pipeline.LocalAppended.WaitAsync(cancellationToken);
        var duplicate = coordinator.CommitAsync(mutation, TimeSpan.FromSeconds(1), cancellationToken);

        pipeline.FailBlockedFollowers();

        var firstError = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(first);
        var duplicateError = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(duplicate);
        _ = await Assert.That(firstError.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
        _ = await Assert.That(duplicateError.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
        _ = await Assert.That(pipeline.LocalAppendCount).IsEqualTo(1);
    }

    /// <summary>A follower failure after local durability returns the stable ambiguous result.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailureAfterAppendIsCommitUnknown(CancellationToken cancellationToken)
    {
        var pipeline = new ReplicaCommitTestKit.Pipeline(true);
        await using var coordinator = ReplicaCommitTestKit.CreateCoordinator(pipeline);
        var operation = coordinator.CommitAsync(ReplicaCommitTestKit.CreateMutation(), TimeSpan.FromSeconds(1), cancellationToken);

        var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(operation);
        _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
        _ = await Assert.That(pipeline.LocalAppendCount).IsEqualTo(1);
    }

    /// <summary>A definite local-append failure leaves the same index available to a later operation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreAppendFailureAllowsSameIndexRetry(CancellationToken cancellationToken)
    {
        var pipeline = new RetryLocalPipeline();
        var hooksExpectations = new IReplicaCommitFaultHooksCreateExpectations();
        _ = hooksExpectations.Setups.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>())
                             .ReturnValue(ValueTask.CompletedTask);
        var coordinator = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(3, 0, 0, 8),
            pipeline,
            hooksExpectations.Instance(),
            new GroupIdempotencyState(16, TimeSpan.MaxValue));
        try
        {
            var first = coordinator.CommitAsync(CreateMutation("00000000000000000000000000000001"), TimeSpan.FromSeconds(2), cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(first);

            var outcome = await coordinator.CommitAsync(CreateMutation("00000000000000000000000000000002"), TimeSpan.FromSeconds(2), cancellationToken);

            await SequenceAssert.EqualAsync<byte>([7], outcome.ToArray());
            _ = await Assert.That(pipeline.LocalAppendCount).IsEqualTo(2);
        }
        finally
        {
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>A definite pre-append failure can release its unresolved reservation.</summary>
    [Test]
    public async Task PreAppendFailureReleasesReservation()
    {
        var state = new GroupIdempotencyState(1, TimeSpan.MaxValue);
        _ = await Assert.That(state.Reserve("client", "op-a", [1], GroupRecordKind.UserMutation, 1, 1)).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(state.TryReleaseUnresolved("client", "op-a", 1, 1)).IsTrue();
        _ = await Assert.That(state.Reserve("client", "op-b", [2], GroupRecordKind.UserMutation, 1, 1)).IsEqualTo(GroupIdempotencyReserveResult.Success);
    }

    /// <summary>The stable internal ambiguity code is available after local append.</summary>
    [Test]
    public async Task UnknownOutcomeUsesStableCode() => _ = await Assert.That(ReplicaCommitCoordinator.CommitOutcomeUnknownCode).IsEqualTo("COMMIT_OUTCOME_UNKNOWN");

    private static PreparedReplicaMutation CreateMutation(string operationId) => new(
        new ReplicaOperationIdentity("group-a", "client", operationId, new byte[] { 1 }),
        1,
        1,
        new ReplicaMutationPayload(new byte[] { 4, 5, 6 }, new byte[] { 7 }, 1));

    [Mutable]
    private sealed class RetryLocalPipeline : IReplicaCommitPipeline
    {
        internal int LocalAppendCount { get; private set; }

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
            ValueTask.FromResult(CreateAcknowledgement(mutation));

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            LocalAppendCount++;
            return LocalAppendCount == 1 ? ValueTask.FromException(new InvalidOperationException("Injected pre-append failure.")) : ValueTask.CompletedTask;
        }

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }

        private static ReplicaDurableAcknowledgement CreateAcknowledgement(PreparedReplicaMutation mutation) => new(
            mutation.GroupId,
            mutation.Term,
            mutation.LogIndex,
            mutation.OperationFingerprint,
            mutation.PayloadChecksum,
            true,
            true);
    }
}
