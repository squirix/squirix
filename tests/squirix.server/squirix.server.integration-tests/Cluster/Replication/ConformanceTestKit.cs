using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.ProtocolModel;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Test-only production trace capture and independent safety projection.</summary>
internal static class ConformanceTestKit
{
    internal static void AssertModelAccepted(IReadOnlyList<TracePoint> trace)
    {
        Assert.NotEmpty(trace);
        var modelTrace = new ModelCommitTracePoint[trace.Count];
        for (var index = 0; index < trace.Count; index++)
        {
            var current = trace[index];
            modelTrace[index] = new ModelCommitTracePoint(
                Convert.ToInt32(current.Term),
                Convert.ToInt32(current.LogIndex),
                Convert.ToInt32(current.CommitIndex),
                Convert.ToInt32(current.AppliedIndex));
        }

        Assert.True(ExploreRunner.AcceptsCommitTrace(modelTrace), "The production trace is not accepted by the protocol model transition system.");
    }

    internal static ReplicaCommitCoordinator CreateCoordinator(Pipeline pipeline, int maxInFlight = 4) => new(
        new ReplicaCommitCoordinatorOptions(3, 0, 0, maxInFlight),
        pipeline,
        NoOpHooks.Instance,
        new GroupIdempotencyState(maxInFlight + 2, TimeSpan.MaxValue));

    internal static PreparedReplicaMutation CreateMutation(ulong index) => new(
        new ReplicaOperationIdentity("group-a", "client", index.ToString("x32", CultureInfo.InvariantCulture), new[] { Convert.ToByte(index) }),
        1,
        index,
        new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 7 }, Convert.ToUInt32(index)),
        0);

    internal sealed record TracePoint(ulong Term, ulong LogIndex, ulong CommitIndex, ulong AppliedIndex);

    internal sealed class Pipeline : IReplicaCommitPipeline
    {
        private readonly bool _blockFirstLocalAppend;
        private readonly TaskCompletionSource<bool> _firstLocalAppendRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _firstLocalAppendStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<ReplicaDurableAcknowledgement> _lagging = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly int _laggingReplica;
        private int _localAppendCalls;

        internal Pipeline(int laggingReplica = -1, bool blockFirstLocalAppend = false)
        {
            _laggingReplica = laggingReplica;
            _blockFirstLocalAppend = blockFirstLocalAppend;
        }

        internal ulong AppliedIndex { get; private set; }

        internal ulong CommitIndex { get; private set; }

        internal Task FirstLocalAppendStarted => _firstLocalAppendStarted.Task;

        internal int FollowerCalls { get; private set; }

        internal List<ulong> LocalIndexes { get; } = [];

        internal List<TracePoint> Trace { get; } = [];

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            CommitIndex = commitIndex;
            Trace.Add(new TracePoint(1, LocalIndexes[^1], CommitIndex, AppliedIndex));
            return ValueTask.CompletedTask;
        }

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            FollowerCalls++;
            return replicaIndex == _laggingReplica ? new ValueTask<ReplicaDurableAcknowledgement>(_lagging.Task) : ValueTask.FromResult(Acknowledge(mutation));
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            LocalIndexes.Add(mutation.LogIndex);
            Trace.Add(new TracePoint(mutation.Term, mutation.LogIndex, CommitIndex, AppliedIndex));
            if (!_blockFirstLocalAppend || Interlocked.Increment(ref _localAppendCalls) != 1)
                return ValueTask.CompletedTask;
            _ = _firstLocalAppendStarted.TrySetResult(true);
            return new ValueTask(_firstLocalAppendRelease.Task.WaitAsync(cancellationToken));
        }

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            AppliedIndex = mutation.LogIndex;
            Trace.Add(new TracePoint(mutation.Term, mutation.LogIndex, CommitIndex, AppliedIndex));
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
            _ = replicaIndex;
            _ = logIndex;
        }

        internal void ReleaseFirstLocalAppend() => _ = _firstLocalAppendRelease.TrySetResult(true);

        internal void ReleaseLagging(PreparedReplicaMutation mutation) => _ = _lagging.TrySetResult(Acknowledge(mutation));

        private static ReplicaDurableAcknowledgement Acknowledge(PreparedReplicaMutation mutation) => new(
            mutation.GroupId,
            mutation.Term,
            mutation.LogIndex,
            mutation.OperationFingerprint,
            mutation.PayloadChecksum,
            true,
            true);
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
