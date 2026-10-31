using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Cost of the leader commit path that starts the follower appends of one entry and waits for the majority, against followers that
/// acknowledge at once: the collections it keeps per commit are the subject.
/// </summary>
[MemoryDiagnoser]
public class ReplicaFollowerFanOutBenchmarks
{
    private const int LeaderReplicaIndex = 0;
    private const int OperationsPerInvoke = 10_000;
    private const int ReplicaCount = 3;
    private static readonly PreparedReplicaMutation[] Run = new PreparedReplicaMutation[1];
    private static readonly ReplicaOperationIdentity Identity = new("group-a", "client", "fedcba9876543210fedcba9876543210", new byte[] { 1 });
    private static readonly ReplicaMutationPayload Payload = new(new byte[] { 2 }, new byte[] { 3 }, 4);
    private AcknowledgingPipeline? _pipeline;
    private ReplicaCommitQuorum? _quorum;

    /// <summary>Starts the follower appends of one entry and awaits its majority, once per entry.</summary>
    /// <returns>A task that completes when all entries are committed.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task CommitSingleEntryAsync()
    {
        var quorum = _quorum!;
        var pipeline = _pipeline!;
        var run = Run;
        var commitIndex = 0UL;
        for (ulong logIndex = 1; logIndex <= OperationsPerInvoke; logIndex++)
        {
            run[0] = new PreparedReplicaMutation(Identity, 1, logIndex, Payload);
            quorum.Admit(LeaderReplicaIndex, logIndex);
            var pending = new List<Task<FollowerCompletion>>(ReplicaCount - 1);
            ReplicaFollowerFanOut.StartFollowers(quorum, LeaderReplicaIndex, pipeline, run, pending, CancellationToken.None);
            await ReplicaFollowerFanOut.AwaitMajorityAsync(quorum, pipeline, pending, run, commitIndex, CancellationToken.None).ConfigureAwait(false);
            commitIndex = logIndex;
        }
    }

    /// <summary>Creates a fresh quorum for the invocation.</summary>
    [IterationSetup]
    public void Setup()
    {
        _quorum = new ReplicaCommitQuorum(ReplicaCount);
        _pipeline = new AcknowledgingPipeline();
    }

    private sealed class AcknowledgingPipeline : IReplicaCommitPipeline
    {
        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true));

        public ValueTask AppendLocalAsync(IReadOnlyList<PreparedReplicaMutation> mutations, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }
    }
}
