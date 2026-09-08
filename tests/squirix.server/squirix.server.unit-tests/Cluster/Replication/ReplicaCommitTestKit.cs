using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Focused majority-pipeline doubles shared by contract-named tests.</summary>
internal static class ReplicaCommitTestKit
{
    internal static ReplicaCommitCoordinator CreateCoordinator(Pipeline pipeline, GroupIdempotencyState? idempotency = null)
    {
        var expectations = new IReplicaCommitFaultHooksCreateExpectations();
        _ = expectations.Setups.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        var options = new ReplicaCommitCoordinatorOptions(3, 0, 0, 2);
        return new ReplicaCommitCoordinator(options, pipeline, expectations.Instance(), idempotency ?? new GroupIdempotencyState(4, TimeSpan.MaxValue));
    }

    internal static PreparedReplicaMutation CreateMutation()
    {
        var identity = new ReplicaOperationIdentity("group-a", "client", "fedcba9876543210fedcba9876543210", new byte[] { 1 });
        return new PreparedReplicaMutation(identity, 1, 1, new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 3 }, 4), 0);
    }

    internal sealed class Pipeline : IReplicaCommitPipeline
    {
        private readonly bool _blockFollowers;
        private readonly Dictionary<int, TaskCompletionSource<ReplicaDurableAcknowledgement>> _blockedFollowers = [];
        private readonly Lock _blockedSync = new();
        private readonly bool _failFollowers;
        private readonly TaskCompletionSource _localAppended = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Pipeline(bool failFollowers = false, bool blockFollowers = false)
        {
            _failFollowers = failFollowers;
            _blockFollowers = blockFollowers;
        }

        internal int LocalAppendCount { get; private set; }

        internal Task LocalAppended => _localAppended.Task;

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            if (_blockFollowers)
            {
                lock (_blockedSync)
                {
                    if (_blockedFollowers.TryGetValue(replicaIndex, out var source))
                        return new ValueTask<ReplicaDurableAcknowledgement>(source.Task);
                    source = new TaskCompletionSource<ReplicaDurableAcknowledgement>(TaskCreationOptions.RunContinuationsAsynchronously);
                    _blockedFollowers[replicaIndex] = source;
                    return new ValueTask<ReplicaDurableAcknowledgement>(source.Task);
                }
            }

            if (_failFollowers)
                return ValueTask.FromException<ReplicaDurableAcknowledgement>(new TimeoutException("follower timeout"));
            var result = new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true);
            return ValueTask.FromResult(result);
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            LocalAppendCount++;
            _ = _localAppended.TrySetResult();
            return ValueTask.CompletedTask;
        }

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }

        internal void FailBlockedFollowers()
        {
            List<TaskCompletionSource<ReplicaDurableAcknowledgement>> sources;
            lock (_blockedSync)
                sources = [.. _blockedFollowers.Values];

            foreach (var source in CollectionsMarshal.AsSpan(sources))
                _ = source.TrySetException(new TimeoutException("follower timeout"));
        }
    }
}
