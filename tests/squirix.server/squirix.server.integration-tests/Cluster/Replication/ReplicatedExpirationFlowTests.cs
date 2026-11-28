using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Integration evidence for the leader-owned expiration path over the common majority pipeline.</summary>
public sealed class ReplicatedExpirationFlowTests : NodeIntegrationTestBase
{
    /// <summary>The expiry callers of one key share one tombstone commit, applied before any of them sees the miss.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredFlowCommitsBeforeMiss(CancellationToken cancellationToken)
    {
        var trace = new List<string>();
        var pipeline = CreateImmediatePipeline(trace);
        var expectations = IReplicaCommitFaultHooks.Mock(MockBehavior.Strict);
        _ = expectations.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>()).ReturnsAsync(ValueTask.CompletedTask);
        var options = new ReplicaCommitCoordinatorOptions(3, 0, 0, 1);
        await using var commit = new ReplicaCommitCoordinator(options, pipeline, expectations.Object, new GroupIdempotencyState(4, TimeSpan.MaxValue, TimeProvider.System))
        {
            ObserveTimeProvider = TimeProvider.System,
            BudgetTimeProvider = new FakeTimeProvider(DateTimeOffset.UnixEpoch),
        };
        var expiresUtc = new DateTime(638900000000000000, DateTimeKind.Utc);
        var operationId = ReplicaExpirationOperationId.Create("group-a", "default", "key-a", 1, expiresUtc);
        var tombstone = new PreparedReplicaMutation(
            new ReplicaOperationIdentity("group-a", ReplicaExpirationOperationId.OperationScope, operationId, new byte[] { 1 }),
            1,
            1,
            new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 3 }, 4));
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var expiration = new ReplicaExpirationCoordinator<string>(
            Bind.To<(ReplicaCommitCoordinator Commit, PreparedReplicaMutation Tombstone, TaskCompletionSource Started), Func<IReadOnlyList<(string CacheName, string Key)>, Task<(string? Entry, Exception? Failure)[]>>>(
                (commit, tombstone, started),
                static shared => async keys =>
                {
                    await shared.Started.Task.ConfigureAwait(false);
                    _ = await shared.Commit.CommitAsync(shared.Tombstone, TimeSpan.FromSeconds(2), CancellationToken.None).ConfigureAwait(false);
                    return new (string? Entry, Exception? Failure)[keys.Count];
                }))
        { ShutdownTimeProvider = TimeProvider.System };

        var first = expiration.ExpireAsync("default", "key-a", cancellationToken);
        var second = expiration.ExpireAsync("default", "key-a", cancellationToken);
        started.SetResult();
        var misses = await Task.WhenAll(first, second);

        trace.Add("miss");
        _ = await Assert.That(misses[0]).IsNull();
        _ = await Assert.That(misses[1]).IsNull();
        await SequenceAssert.EqualAsync(["local", "follower", "follower", "commit", "apply", "miss"], trace, StringComparer.Ordinal);
    }

    private static IReplicaCommitPipeline CreateImmediatePipeline(List<string> trace)
    {
        var expectations = IReplicaCommitPipeline.Mock(MockBehavior.Strict);
        _ = expectations.AdvanceCommitIndexAsync(Arg.Any<ulong>(), Arg.Any<CancellationToken>()).ReturnsAsync((_, _) =>
        {
            trace.Add("commit");
            return ValueTask.CompletedTask;
        });
        _ = expectations.AppendFollowerAsync(Arg.Any<int>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>()).ReturnsAsync((_, mutation, _) =>
        {
            trace.Add("follower");
            var result = new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true);
            return ValueTask.FromResult(result);
        });
        _ = expectations.AppendLocalAsync(Arg.Any<IReadOnlyList<PreparedReplicaMutation>>(), Arg.Any<CancellationToken>()).ReturnsAsync((_, _) =>
        {
            trace.Add("local");
            return ValueTask.CompletedTask;
        });
        _ = expectations.ApplyMemoryAsync(Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>()).ReturnsAsync((_, _) =>
        {
            trace.Add("apply");
            return ValueTask.CompletedTask;
        });
        _ = expectations.RecordLaggingReplica(Arg.Any<int>(), Arg.Any<ulong>());
        return expectations.Object;
    }
}
