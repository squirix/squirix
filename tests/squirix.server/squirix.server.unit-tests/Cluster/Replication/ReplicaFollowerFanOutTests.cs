using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>The fan-out observes every distinct follower append once and marks a follower lagging when the pipeline hands it an append already started for another.</summary>
[Immutable]
public sealed class ReplicaFollowerFanOutTests : ServerUnitTestBase
{
    /// <summary>The second follower that receives the same task is marked lagging and is not observed again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SharedTaskMarksFollowerLagging(CancellationToken cancellationToken)
    {
        var mutation = ReplicaMutationTestKit.CreateMutation();
        var pipeline = new SharedTaskPipeline();
        List<Task<FollowerCompletion>> pending = [];

        ReplicaFollowerFanOut.StartFollowers(new ReplicaCommitQuorum(3), 0, pipeline, [mutation], pending, cancellationToken);

        _ = await Assert.That(pending.Count).IsEqualTo(1);
        _ = await Assert.That(pipeline.Lagging.Count).IsEqualTo(1);
        _ = await Assert.That(pipeline.Lagging[0]).IsEqualTo((2, mutation.LogIndex));
    }

    private sealed class SharedTaskPipeline : IReplicaCommitPipeline
    {
        private readonly Task<ReplicaDurableAcknowledgement> _shared = new TaskCompletionSource<ReplicaDurableAcknowledgement>(TaskCreationOptions.RunContinuationsAsynchronously).Task;

        internal List<(int ReplicaIndex, ulong LogIndex)> Lagging { get; } = [];

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
            new(_shared);

        public ValueTask AppendLocalAsync(IReadOnlyList<PreparedReplicaMutation> mutations, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex) => Lagging.Add((replicaIndex, logIndex));
    }
}
