using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Exercises the post-append cancellation boundary through the stable gRPC contract.</summary>
public sealed class CommitUnknownTransportTests : NodeIntegrationTestBase
{
    /// <summary>Cancellation after local durability projects to unavailable commit unknown.</summary>
    [Fact(DisplayName = "CancellationAfterLocalAppendReturnsCommitUnknown")]
    public async Task CancellationAfterAppendIsUnknown()
    {
        var pipeline = new BlockingFollowerPipeline();
        await using var coordinator = new ReplicaCommitCoordinator(3, 0, 0, 1, pipeline, NoOpHooks.Instance, new GroupIdempotencyState(4, TimeSpan.MaxValue));
        var mutation = CreateMutation();
        try
        {
            using var cancellation = new CancellationTokenSource();
            var operation = coordinator.CommitAsync(mutation, TimeSpan.FromSeconds(5), cancellation.Token);

            _ = await pipeline.LocalAppended.Task.WaitAsync(DefaultCancellationToken);
            await cancellation.CancelAsync();
            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(operation);

            Assert.Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, error.Message, StringComparison.Ordinal);
            var transport = ServerOpContract.CommitOutcomeUnknown();
            Assert.Equal(StatusCode.Unavailable, SquirixErrorMapper.ToGrpcStatusCode(transport.Code));
            Assert.Equal(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, SquirixErrorMapper.ToPublicCode(transport.Code));
        }
        finally
        {
            pipeline.Release(mutation);
        }
    }

    private static PreparedReplicaMutation CreateMutation() => new(
        new ReplicaOperationIdentity("transport-group", "client", "123456789abcdef0123456789abcdef0", new byte[] { 1 }),
        1,
        1,
        new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 3 }, 1),
        0);

    private sealed class BlockingFollowerPipeline : IReplicaCommitPipeline
    {
        private readonly TaskCompletionSource<ReplicaDurableAcknowledgement> _acknowledgement = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource<bool> LocalAppended { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken)
        {
            _ = commitIndex;
            _ = cancellationToken;
            return ValueTask.CompletedTask;
        }

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = replicaIndex;
            _ = mutation;
            _ = cancellationToken;
            return new ValueTask<ReplicaDurableAcknowledgement>(_acknowledgement.Task);
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = mutation;
            _ = cancellationToken;
            LocalAppended.SetResult(true);
            return ValueTask.CompletedTask;
        }

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = mutation;
            _ = cancellationToken;
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
            _ = replicaIndex;
            _ = logIndex;
        }

        internal void Release(PreparedReplicaMutation mutation) => _ = _acknowledgement.TrySetResult(
            new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true));
    }

    private sealed class NoOpHooks : IReplicaCommitFaultHooks
    {
        internal static NoOpHooks Instance { get; } = new();

        public ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = stage;
            _ = mutation;
            _ = cancellationToken;
            return ValueTask.CompletedTask;
        }
    }
}
