using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Majority of a commit whose follower acknowledgement is recorded by the observation of the commit before it.</summary>
public sealed class ReplicaAckOrderTests
{
    private const int Rounds = 25;

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(10);

    /// <summary>
    /// A follower's acknowledgement of entry 2 that arrives before its acknowledgement of entry 1 is recorded, because the background
    /// observation of the first commit records that one later, still completes the majority of entry 2 without a second follower.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LatePrefixAckCompletesLaterMajority(CancellationToken cancellationToken)
    {
        for (var round = 0; round < Rounds; round++)
        {
            var pipeline = new AckPipeline();
            await using var coordinator = new ReplicaCommitCoordinator(
                new ReplicaCommitCoordinatorOptions(3, 0, 0, 2),
                pipeline,
                ReplicaFaultHooks.CreateNoOp(),
                new GroupIdempotencyState(4, TimeSpan.MaxValue));
            var first = Mutation(1);
            var second = Mutation(2);
            try
            {
                var firstCommit = coordinator.CommitAsync(first, HangGuard, cancellationToken);
                pipeline.Answer(2, first);
                _ = await firstCommit.AsTask().WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

                // The first follower still owes entry 1; its entry 2 comes first, and the second follower never answers.
                var secondCommit = coordinator.CommitAsync(second, HangGuard, cancellationToken);
                pipeline.Answer(1, second);
                for (var spin = 0; spin < 20; spin++)
                    await Task.Yield();

                pipeline.Answer(1, first);

                _ = await secondCommit.AsTask().WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            }
            finally
            {
                pipeline.AnswerAll();
            }
        }
    }

    private static PreparedReplicaMutation Mutation(ulong index)
    {
        var identity = new ReplicaOperationIdentity("group-a", "client", $"op-{index}", new byte[] { 1 });
        return new PreparedReplicaMutation(identity, 1, index, new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 3 }, 4));
    }

    /// <summary>Pipeline double whose follower acknowledgements wait until the test answers them, per slot and entry.</summary>
    [ThreadSafe]
    private sealed class AckPipeline : IReplicaCommitPipeline
    {
        private readonly Dictionary<(int Replica, ulong Index), TaskCompletionSource<ReplicaDurableAcknowledgement>> _acks = [];
        private readonly Lock _sync = new();

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
            new(AckFor(replicaIndex, mutation.LogIndex).Task);

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }

        internal void Answer(int replicaIndex, PreparedReplicaMutation mutation) =>
            _ = AckFor(replicaIndex, mutation.LogIndex).TrySetResult(
                new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true));

        /// <summary>Fails every acknowledgement still waiting, so no work parked by the test outlives it.</summary>
        internal void AnswerAll()
        {
            TaskCompletionSource<ReplicaDurableAcknowledgement>[] all;
            lock (_sync)
                all = [.. _acks.Values];

            foreach (var ack in all)
                _ = ack.TrySetException(new TimeoutException("follower did not answer"));
        }

        private TaskCompletionSource<ReplicaDurableAcknowledgement> AckFor(int replicaIndex, ulong index)
        {
            lock (_sync)
            {
                if (!_acks.TryGetValue((replicaIndex, index), out var ack))
                {
                    ack = new TaskCompletionSource<ReplicaDurableAcknowledgement>(TaskCreationOptions.RunContinuationsAsynchronously);
                    _acks[(replicaIndex, index)] = ack;
                }

                return ack;
            }
        }
    }
}
