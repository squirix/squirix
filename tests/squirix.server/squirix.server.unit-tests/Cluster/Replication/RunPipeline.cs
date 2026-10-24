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
    private readonly TaskCompletionSource _heldReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly List<(int Count, TaskCompletionSource Done)> _appliedWaiters = [];
    private readonly List<(int Count, TaskCompletionSource Done)> _failedApplyWaiters = [];
    private readonly Lock _sync = new();
    private readonly TaskCompletionSource _lateReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _failApplyStopped;
    private int _failedApplies;

    internal Exception? LocalFailure { get; set; }

    internal Exception? AdvanceFailure { get; set; }

    internal ulong AckThrough { get; init; } = ulong.MaxValue;

    /// <summary>Gets the highest index each listed follower acknowledges at once; a follower not listed uses <see cref="AckThrough" />.</summary>
    internal Dictionary<int, ulong> ReplicaAckThrough { get; init; } = [];

    /// <summary>Gets the indexes whose follower append fails at once, on every follower.</summary>
    internal HashSet<ulong> FaultFollowerAt { get; init; } = [];

    /// <summary>Gets the indexes whose follower append is acknowledged only once <see cref="ReleaseHeld" /> runs, on every follower.</summary>
    internal HashSet<ulong> HoldFollowerAt { get; init; } = [];

    internal ulong FailApplyAt { get; init; }

    /// <summary>Gets the ambient value seen by each apply, in apply order.</summary>
    internal ConcurrentQueue<string?> AppliedScopes { get; } = new();

    /// <summary>Gets the ambient value a test sets before it commits; <see cref="AppliedScopes" /> shows which applies still see it.</summary>
    internal AsyncLocal<string?> AmbientScope { get; } = new();

    /// <summary>Gets the replica slot and log index of every follower reported lagging, in report order.</summary>
    internal ConcurrentQueue<(int ReplicaIndex, ulong LogIndex)> LaggingReplicas { get; } = new();

    internal bool StallApply { get; init; }

    internal ConcurrentQueue<ulong> Applied { get; } = new();

    internal Task ApplyEntered => _applyEntered.Task;

    internal ConcurrentQueue<ulong> CommitIndexes { get; } = new();

    internal ConcurrentQueue<(ulong First, int Count)> LocalRuns { get; } = new();

    public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken)
    {
        if (AdvanceFailure is { } failure)
            return ValueTask.FromException(failure);

        CommitIndexes.Enqueue(commitIndex);
        return ValueTask.CompletedTask;
    }

    public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
    {
        if (FaultFollowerAt.Contains(mutation.LogIndex))
            return ValueTask.FromException<ReplicaDurableAcknowledgement>(new InvalidOperationException("Injected follower append failure."));
        if (HoldFollowerAt.Contains(mutation.LogIndex))
            return new ValueTask<ReplicaDurableAcknowledgement>(HeldAsync(mutation));

        var through = ReplicaAckThrough.TryGetValue(replicaIndex, out var limit) ? limit : AckThrough;
        return mutation.LogIndex <= through ? ValueTask.FromResult(Acknowledge(mutation)) : new ValueTask<ReplicaDurableAcknowledgement>(LateAsync(mutation));
    }

    public ValueTask AppendLocalAsync(IReadOnlyList<PreparedReplicaMutation> mutations, CancellationToken cancellationToken)
    {
        if (LocalFailure is { } failure)
            return ValueTask.FromException(failure);

        LocalRuns.Enqueue((mutations[0].LogIndex, mutations.Count));
        return ValueTask.CompletedTask;
    }

    public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
    {
        if (mutation.LogIndex == FailApplyAt && Volatile.Read(ref _failApplyStopped) == 0)
        {
            RecordFailedApply();
            return ValueTask.FromException(new InvalidOperationException("Injected memory apply failure after the majority."));
        }

        if (StallApply)
        {
            _ = _applyEntered.TrySetResult();
            return new ValueTask(StallThenApplyAsync(mutation));
        }

        RecordApplied(mutation);
        return ValueTask.CompletedTask;
    }

    public void RecordLaggingReplica(int replicaIndex, ulong logIndex) => LaggingReplicas.Enqueue((replicaIndex, logIndex));

    /// <summary>Completes once the apply of <see cref="FailApplyAt" /> failed the given number of times.</summary>
    /// <param name="count">The number of failed attempts.</param>
    /// <returns>A task that completes when the failures happened.</returns>
    /// <remarks>
    /// The apply fails on every attempt until <see cref="StopFailingApply" /> runs, so the background drive that follows a failed commit fails
    /// too and leaves the state a test inspects; waiting for its failure makes that state final.
    /// </remarks>
    internal Task FailedApplyAsync(int count)
    {
        lock (_sync)
        {
            if (_failedApplies >= count)
                return Task.CompletedTask;

            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _failedApplyWaiters.Add((count, done));
            return done.Task;
        }
    }

    internal void StopFailingApply() => Volatile.Write(ref _failApplyStopped, 1);

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

    internal void ReleaseHeld() => _ = _heldReleased.TrySetResult();

    internal void ReleaseLate() => _ = _lateReleased.TrySetResult();

    private static ReplicaDurableAcknowledgement Acknowledge(PreparedReplicaMutation mutation) =>
        new(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true);

    private async Task<ReplicaDurableAcknowledgement> HeldAsync(PreparedReplicaMutation mutation)
    {
        await _heldReleased.Task.ConfigureAwait(false);
        return Acknowledge(mutation);
    }

    private async Task<ReplicaDurableAcknowledgement> LateAsync(PreparedReplicaMutation mutation)
    {
        await _lateReleased.Task.ConfigureAwait(false);
        return Acknowledge(mutation);
    }

    private void RecordFailedApply()
    {
        lock (_sync)
        {
            _failedApplies++;
            for (var i = _failedApplyWaiters.Count - 1; i >= 0; i--)
            {
                if (_failedApplies < _failedApplyWaiters[i].Count)
                    continue;

                _ = _failedApplyWaiters[i].Done.TrySetResult();
                _failedApplyWaiters.RemoveAt(i);
            }
        }
    }

    private void RecordApplied(PreparedReplicaMutation mutation)
    {
        lock (_sync)
        {
            Applied.Enqueue(mutation.LogIndex);
            AppliedScopes.Enqueue(AmbientScope.Value);
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
