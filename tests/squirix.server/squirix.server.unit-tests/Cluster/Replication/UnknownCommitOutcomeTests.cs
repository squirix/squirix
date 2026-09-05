using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Pre-append release and post-append ambiguity contract tests.</summary>
[Immutable]
public sealed class UnknownCommitOutcomeTests : ServerUnitTestBase
{
    /// <summary>A duplicate that joins after local durability shares the original append boundary.</summary>
    [Fact]
    public async Task DuplicateRetryKeepsAppendBoundary()
    {
        var pipeline = new ReplicaCommitTestKit.Pipeline(blockFollowers: true);
        await using var coordinator = ReplicaCommitTestKit.CreateCoordinator(pipeline);
        var mutation = ReplicaCommitTestKit.CreateMutation();
        var first = coordinator.CommitAsync(mutation, TimeSpan.FromSeconds(1), DefaultCancellationToken);
        await pipeline.LocalAppended.WaitAsync(DefaultCancellationToken);
        var duplicate = coordinator.CommitAsync(mutation, TimeSpan.FromSeconds(1), DefaultCancellationToken);

        pipeline.FailBlockedFollowers();

        var firstError = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(first);
        var duplicateError = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(duplicate);
        Assert.Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, firstError.Message, StringComparison.Ordinal);
        Assert.Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, duplicateError.Message, StringComparison.Ordinal);
        Assert.Equal(1, pipeline.LocalAppendCount);
    }

    /// <summary>A follower failure after local durability returns the stable ambiguous result.</summary>
    [Fact(DisplayName = "FailureAfterLocalAppendReturnsCommitUnknown")]
    public async Task FailureAfterAppendIsCommitUnknown()
    {
        var pipeline = new ReplicaCommitTestKit.Pipeline(true);
        await using var coordinator = ReplicaCommitTestKit.CreateCoordinator(pipeline);
        var operation = coordinator.CommitAsync(ReplicaCommitTestKit.CreateMutation(), TimeSpan.FromSeconds(1), DefaultCancellationToken);

        var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(operation);
        Assert.Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, error.Message, StringComparison.Ordinal);
        Assert.Equal(1, pipeline.LocalAppendCount);
    }

    /// <summary>A definite local-append failure leaves the same index available to a later operation.</summary>
    [Fact]
    public async Task PreAppendFailureAllowsSameIndexRetry()
    {
        var pipeline = new RetryLocalPipeline();
        var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(3, 0, 0, 8), pipeline, NoOpHooks.Instance, new GroupIdempotencyState(16, TimeSpan.MaxValue));
        try
        {
            var first = coordinator.CommitAsync(CreateMutation("00000000000000000000000000000001"), TimeSpan.FromSeconds(2), DefaultCancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(first);

            var outcome = await coordinator.CommitAsync(CreateMutation("00000000000000000000000000000002"), TimeSpan.FromSeconds(2), DefaultCancellationToken);

            Assert.Equal(new byte[] { 7 }, outcome.ToArray());
            Assert.Equal(2, pipeline.LocalAppendCount);
        }
        finally
        {
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>A definite pre-append failure can release its unresolved reservation.</summary>
    [Fact]
    public void PreAppendFailureReleasesReservation()
    {
        var state = new GroupIdempotencyState(1, TimeSpan.MaxValue);
        Assert.Equal(GroupIdempotencyReserveResult.Success, state.Reserve("client", "op-a", new byte[] { 1 }, GroupRecordKind.UserMutation, 1, 1));
        Assert.True(state.TryReleaseUnresolved("client", "op-a", 1, 1));
        Assert.Equal(GroupIdempotencyReserveResult.Success, state.Reserve("client", "op-b", new byte[] { 2 }, GroupRecordKind.UserMutation, 1, 1));
    }

    /// <summary>The stable internal ambiguity code is available after local append.</summary>
    [Fact]
    public void UnknownOutcomeUsesStableCode() => Assert.Equal("COMMIT_OUTCOME_UNKNOWN", ReplicaCommitCoordinator.CommitOutcomeUnknownCode);

    private static PreparedReplicaMutation CreateMutation(string operationId) => new(
        new ReplicaOperationIdentity("group-a", "client", operationId, new byte[] { 1 }),
        1,
        1,
        new ReplicaMutationPayload(new byte[] { 4, 5, 6 }, new byte[] { 7 }, 1),
        0);

    private sealed class NoOpHooks : IReplicaCommitFaultHooks
    {
        internal static NoOpHooks Instance { get; } = new();

        public ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

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
