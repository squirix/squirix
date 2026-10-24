using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>
/// Majority pipeline whose follower acknowledges the entries up to a limit at once and the others once released, and which records the
/// local runs, the commit indexes, and the applied entries.
/// </summary>
[ThreadSafe]
internal sealed class RunPipeline : IReplicaCommitPipeline
{
    private readonly TaskCompletionSource _applyEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _applyReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly List<(int Count, TaskCompletionSource Done)> _appliedWaiters = [];
    private readonly Lock _sync = new();
    private readonly TaskCompletionSource _lateReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _failedApply;

    internal Exception? LocalFailure { get; set; }

    internal ulong AckThrough { get; init; } = ulong.MaxValue;

    internal ulong FailApplyAt { get; init; }

    internal bool StallApply { get; init; }

    internal ConcurrentQueue<ulong> Applied { get; } = new();

    internal Task ApplyEntered => _applyEntered.Task;

    internal ConcurrentQueue<ulong> CommitIndexes { get; } = new();

    internal ConcurrentQueue<(ulong First, int Count)> LocalRuns { get; } = new();

    public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken)
    {
        CommitIndexes.Enqueue(commitIndex);
        return ValueTask.CompletedTask;
    }

    public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
        mutation.LogIndex <= AckThrough ? ValueTask.FromResult(Acknowledge(mutation)) : new ValueTask<ReplicaDurableAcknowledgement>(LateAsync(mutation));

    public ValueTask AppendLocalAsync(IReadOnlyList<PreparedReplicaMutation> mutations, CancellationToken cancellationToken)
    {
        if (LocalFailure is { } failure)
            return ValueTask.FromException(failure);

        LocalRuns.Enqueue((mutations[0].LogIndex, mutations.Count));
        return ValueTask.CompletedTask;
    }

    public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
    {
        if (mutation.LogIndex == FailApplyAt && Interlocked.Exchange(ref _failedApply, 1) == 0)
            return ValueTask.FromException(new InvalidOperationException("Injected memory apply failure after the majority."));
        if (StallApply)
        {
            _ = _applyEntered.TrySetResult();
            return new ValueTask(StallThenApplyAsync(mutation));
        }

        RecordApplied(mutation);
        return ValueTask.CompletedTask;
    }

    public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
    {
    }

    internal Task AppliedAsync(int count)
    {
        lock (_sync)
        {
            if (Applied.Count >= count)
                return Task.CompletedTask;

            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _appliedWaiters.Add((count, done));
            return done.Task;
        }
    }

    internal void ReleaseApply() => _ = _applyReleased.TrySetResult();

    internal void ReleaseLate() => _ = _lateReleased.TrySetResult();

    private static ReplicaDurableAcknowledgement Acknowledge(PreparedReplicaMutation mutation) =>
        new(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true);

    private async Task<ReplicaDurableAcknowledgement> LateAsync(PreparedReplicaMutation mutation)
    {
        await _lateReleased.Task.ConfigureAwait(false);
        return Acknowledge(mutation);
    }

    private void RecordApplied(PreparedReplicaMutation mutation)
    {
        lock (_sync)
        {
            Applied.Enqueue(mutation.LogIndex);
            for (var i = _appliedWaiters.Count - 1; i >= 0; i--)
            {
                if (Applied.Count < _appliedWaiters[i].Count)
                    continue;

                _ = _appliedWaiters[i].Done.TrySetResult();
                _appliedWaiters.RemoveAt(i);
            }
        }
    }

    private async Task StallThenApplyAsync(PreparedReplicaMutation mutation)
    {
        await _applyReleased.Task.ConfigureAwait(false);
        RecordApplied(mutation);
    }
}
