using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Testing;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Exercises the post-append cancellation boundary through the stable gRPC contract.</summary>
public sealed class CommitUnknownTransportTests : NodeIntegrationTestBase
{
    /// <summary>Cancellation after local durability projects to unavailable commit unknown.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancellationAfterAppendIsUnknown(CancellationToken cancellationToken)
    {
        var expectations = IReplicaCommitFaultHooks.Mock(MockBehavior.Strict);
        _ = expectations.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>()).ReturnsAsync(ValueTask.CompletedTask);
        var localAppended = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var acknowledgement = new TaskCompletionSource<ReplicaDurableAcknowledgement>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = CreateBlockingFollowerPipeline(localAppended, acknowledgement);
        var options = new ReplicaCommitCoordinatorOptions(3, 0, 0, 1);
        await using var coordinator = new ReplicaCommitCoordinator(options, pipeline, expectations.Object, new GroupIdempotencyState(4, TimeSpan.MaxValue, TimeProvider.System))
        {
            ObserveTimeProvider = TimeProvider.System,
            BudgetTimeProvider = new FakeTimeProvider(DateTimeOffset.UnixEpoch),
        };
        var mutation = CreateMutation();
        try
        {
            using var cancellation = new CancellationTokenSource();
            var operation = coordinator.CommitAsync(mutation, TimeSpan.FromSeconds(5), cancellation.Token);

            _ = await localAppended.Task.WaitAsync(cancellationToken);
            await cancellation.CancelAsync();
            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(operation, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

            _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
            var transport = ServerOpContract.CommitOutcomeUnknown();
            _ = await Assert.That(SquirixErrorMapper.ToFrameStatus(transport.Code)).IsEqualTo(FrameStatus.Unavailable);
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
        var expectations = IReplicaCommitPipeline.Mock(MockBehavior.Strict);
        _ = expectations.AdvanceCommitIndexAsync(Arg.Any<ulong>(), Arg.Any<CancellationToken>()).ReturnsAsync(ValueTask.CompletedTask);
        _ = expectations.AppendFollowerAsync(Arg.Any<int>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>())
            .ReturnsAsync((_, _, _) => new ValueTask<ReplicaDurableAcknowledgement>(acknowledgement.Task));
        _ = expectations.AppendLocalAsync(Arg.Any<IReadOnlyList<PreparedReplicaMutation>>(), Arg.Any<CancellationToken>()).ReturnsAsync((_, _) =>
        {
            localAppended.SetResult(true);
            return ValueTask.CompletedTask;
        });
        _ = expectations.ApplyMemoryAsync(Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>()).ReturnsAsync(ValueTask.CompletedTask);
        _ = expectations.RecordLaggingReplica(Arg.Any<int>(), Arg.Any<ulong>());
        return expectations.Object;
    }
}
