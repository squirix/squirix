using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
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

    internal static ReplicaCommitCoordinator CreateCoordinator(Pipeline pipeline, int maxInFlight = 4, int replicaCount = 3)
    {
        var expectations = new IReplicaCommitFaultHooksCreateExpectations();
        _ = expectations.Setups.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        var options = new ReplicaCommitCoordinatorOptions(replicaCount, 0, 0, maxInFlight);
        return new ReplicaCommitCoordinator(options, pipeline, expectations.Instance(), new GroupIdempotencyState(maxInFlight + 2, TimeSpan.MaxValue));
    }

    internal static PreparedReplicaMutation CreateMutation(ulong index)
    {
        var identity = new ReplicaOperationIdentity("group-a", "client", index.ToString("x32", CultureInfo.InvariantCulture), new[] { Convert.ToByte(index) });
        return new PreparedReplicaMutation(identity, 1, index, new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 7 }, Convert.ToUInt32(index)));
    }

    internal sealed record TracePoint(ulong Term, ulong LogIndex, ulong CommitIndex, ulong AppliedIndex);

    internal sealed class Pipeline : IReplicaCommitPipeline
    {
        private readonly bool _blockFirstLocalAppend;
        private readonly TaskCompletionSource<bool> _firstLocalAppendRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _firstLocalAppendStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<ReplicaDurableAcknowledgement> _lagging = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly int _laggingReplica;
        private readonly int _unavailableReplica;
        private int _localAppendCalls;

        internal Pipeline(int laggingReplica = -1, bool blockFirstLocalAppend = false, int unavailableReplica = -1)
        {
            _laggingReplica = laggingReplica;
            _blockFirstLocalAppend = blockFirstLocalAppend;
            _unavailableReplica = unavailableReplica;
        }

        internal ulong AppliedIndex { get; private set; }

        internal ulong CommitIndex { get; private set; }

        internal Task FirstLocalAppendStarted => _firstLocalAppendStarted.Task;

        internal int FollowerCalls { get; private set; }

        internal List<ulong> LocalIndexes { get; } = [];

        internal List<TracePoint> Trace { get; } = [];

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken)
        {
            CommitIndex = commitIndex;
            Trace.Add(new TracePoint(1, LocalIndexes[^1], CommitIndex, AppliedIndex));
            return ValueTask.CompletedTask;
        }

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            FollowerCalls++;

            // A faulted task, not a synchronous throw: production gateways are async, so transport
            // failures surface as faulted follower tasks that the coordinator records as lagging.
            if (replicaIndex == _unavailableReplica)
                return ValueTask.FromException<ReplicaDurableAcknowledgement>(new IOException("Simulated replica unavailable."));

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
            AppliedIndex = mutation.LogIndex;
            Trace.Add(new TracePoint(mutation.Term, mutation.LogIndex, CommitIndex, AppliedIndex));
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
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
}
