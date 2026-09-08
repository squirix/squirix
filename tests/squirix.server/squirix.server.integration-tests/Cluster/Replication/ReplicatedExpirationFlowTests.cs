using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Integration evidence for the disabled leader-owned expiration path.</summary>
public sealed class ReplicatedExpirationFlowTests : NodeIntegrationTestBase
{
    /// <summary>The common majority pipeline applies a tombstone before exposing the miss.</summary>
    [Fact]
    public async Task ExpiredFlowCommitsBeforeMiss()
    {
        var pipeline = new ImmediatePipeline();
        var expectations = new IReplicaCommitFaultHooksCreateExpectations();
        _ = expectations.Setups.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        var options = new ReplicaCommitCoordinatorOptions(3, 0, 0, 1);
        await using var commit = new ReplicaCommitCoordinator(options, pipeline, expectations.Instance(), new GroupIdempotencyState(4, TimeSpan.MaxValue));
        await using var expiration = new ReplicaExpirationCoordinator(commit, true, 1);
        var expiresUtc = new DateTime(638900000000000000, DateTimeKind.Utc);

        var miss = await expiration.CommitExpiredMissAsync(
            new ReplicaExpirationRequest
            {
                GroupId = "group-a",
                CacheName = "default",
                Key = "key-a",
                UtcNow = expiresUtc.AddTicks(1),
                ReadRaw = _ => ValueTask.FromResult<ReplicaExpirationCandidate?>(new ReplicaExpirationCandidate(1, expiresUtc)),
                PrepareTombstone = static (candidate, operationId) => new PreparedReplicaMutation(
                    new ReplicaOperationIdentity("group-a", ReplicaExpirationOperationId.OperationScope, operationId, new byte[] { 1 }),
                    1,
                    1,
                    new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 3 }, 4),
                    candidate.ExpiresUtc.Ticks),
                Timeout = TimeSpan.FromSeconds(2),
                CancellationToken = DefaultCancellationToken,
            });

        pipeline.Trace.Add("miss");
        Assert.True(miss);
        Assert.Equal(["local", "follower", "follower", "commit", "apply", "miss"], pipeline.Trace);
    }

    private sealed class ImmediatePipeline : IReplicaCommitPipeline
    {
        internal List<string> Trace { get; } = [];

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken)
        {
            Trace.Add("commit");
            return ValueTask.CompletedTask;
        }

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            Trace.Add("follower");
            var result = new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true);
            return ValueTask.FromResult(result);
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            Trace.Add("local");
            return ValueTask.CompletedTask;
        }

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            Trace.Add("apply");
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }
    }
}
