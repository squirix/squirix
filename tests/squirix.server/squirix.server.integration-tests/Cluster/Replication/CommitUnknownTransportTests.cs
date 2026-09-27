using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Exercises the post-append cancellation boundary through the stable gRPC contract.</summary>
public sealed class CommitUnknownTransportTests : NodeIntegrationTestBase
{
    /// <summary>Cancellation after local durability projects to unavailable commit unknown.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancellationAfterAppendIsUnknown(CancellationToken cancellationToken)
    {
        var expectations = new IReplicaCommitFaultHooksCreateExpectations();
        _ = expectations.Setups.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        var localAppended = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var acknowledgement = new TaskCompletionSource<ReplicaDurableAcknowledgement>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = CreateBlockingFollowerPipeline(localAppended, acknowledgement);
        var options = new ReplicaCommitCoordinatorOptions(3, 0, 0, 1);
        await using var coordinator = new ReplicaCommitCoordinator(options, pipeline, expectations.Instance(), new GroupIdempotencyState(4, TimeSpan.MaxValue));
        var mutation = CreateMutation();
        try
        {
            using var cancellation = new CancellationTokenSource();
            var operation = coordinator.CommitAsync(mutation, TimeSpan.FromSeconds(5), cancellation.Token);

            _ = await localAppended.Task.WaitAsync(cancellationToken);
            await cancellation.CancelAsync();
            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(operation);

            _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
            var transport = ServerOpContract.CommitOutcomeUnknown();
            _ = await Assert.That(SquirixErrorMapper.ToGrpcStatusCode(transport.Code)).IsEqualTo(StatusCode.Unavailable);
            _ = await Assert.That(SquirixErrorMapper.ToPublicCode(transport.Code)).IsEqualTo(ReplicaCommitCoordinator.CommitOutcomeUnknownCode);
        }
        finally
        {
            _ = acknowledgement.TrySetResult(
                new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true));
        }
    }

    private static PreparedReplicaMutation CreateMutation()
    {
        var identity = new ReplicaOperationIdentity("transport-group", "client", "123456789abcdef0123456789abcdef0", new byte[] { 1 });
        return new PreparedReplicaMutation(identity, 1, 1, new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 3 }, 1));
    }

    private static IReplicaCommitPipeline CreateBlockingFollowerPipeline(
        TaskCompletionSource<bool> localAppended,
        TaskCompletionSource<ReplicaDurableAcknowledgement> acknowledgement)
    {
        var expectations = new IReplicaCommitPipelineCreateExpectations();
        _ = expectations.Setups.AdvanceCommitIndexAsync(Arg.Any<ulong>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        _ = expectations.Setups.AppendFollowerAsync(Arg.Any<int>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>())
            .Callback((_, _, _) => new ValueTask<ReplicaDurableAcknowledgement>(acknowledgement.Task));
        _ = expectations.Setups.AppendLocalAsync(Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>()).Callback((_, _) =>
        {
            localAppended.SetResult(true);
            return ValueTask.CompletedTask;
        });
        _ = expectations.Setups.ApplyMemoryAsync(Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        _ = expectations.Setups.RecordLaggingReplica(Arg.Any<int>(), Arg.Any<ulong>());
        return expectations.Instance();
    }
}
