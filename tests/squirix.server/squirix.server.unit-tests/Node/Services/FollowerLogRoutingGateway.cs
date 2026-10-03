using System;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Follower transport double that routes leader batches to a real follower log per node, as the follower RPC handler does, and can park
/// the next batch with entries of one node until the test releases it. A parked call observes cancellation, as the real transport does.
/// </summary>
[ThreadSafe]
internal sealed class FollowerLogRoutingGateway : IReplicaRpcGateway
{
    private readonly List<(string Node, ulong Index, TaskCompletionSource Done)> _appendWaiters = [];
    private readonly FrozenDictionary<string, FollowerLog> _logs;
    private readonly TaskCompletionSource _parkCanceled = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _parked = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Lock _sync = new();
    private readonly ConcurrentDictionary<string, ulong> _through = new(StringComparer.Ordinal);
    private string? _parkNode;

    internal FollowerLogRoutingGateway(ConcurrentDictionary<string, FollowerLog> logs)
    {
        _logs = logs.ToFrozenDictionary(StringComparer.Ordinal);
    }

    /// <summary>Gets every batch with entries that reached the transport, as the node, first and last index, and predecessor index, in arrival order.</summary>
    internal ConcurrentQueue<(string Node, ulong FirstIndex, ulong LastIndex, ulong PrevLogIndex)> Arrivals { get; } = new();

    /// <summary>Gets the node and refusal code of every batch a follower log refused.</summary>
    internal ConcurrentQueue<(string Node, string Refusal)> Refusals { get; } = new();

    /// <summary>Gets a value indicating whether canceling a parked call throws from a cancellation callback; <see langword="false" /> unless set.</summary>
    internal bool FailsOnCancel { get; init; }

    /// <summary>Gets a task that completes when the parked call observed its cancellation.</summary>
    internal Task ParkCanceled => _parkCanceled.Task;

    /// <summary>Gets a task that completes when a call parked.</summary>
    internal Task Parked => _parked.Task;

    public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
    {
        var records = batch.Records;
        if (records.Count > 0)
            Arrivals.Enqueue((nodeId, records[0].LogIndex, records[^1].LogIndex, batch.PrevLogIndex));

        if (records.Count > 0 && TakePark(nodeId))
        {
            _ = _parked.TrySetResult();
            await using var failing = FailsOnCancel ? cancellationToken.Register(static () => throw new InvalidOperationException("Injected cancellation callback failure.")) : default;
            try
            {
                await _release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                _ = _parkCanceled.TrySetResult();
                throw;
            }
        }

        var entries = new FollowerLogEntry[records.Count];
        for (var i = 0; i < entries.Length; i++)
        {
            var record = records[i];
            entries[i] = new FollowerLogEntry(record.LogIndex, record.Term, ReplicaLogCodec.Encode(in record));
        }

        var request = new FollowerLogAppendRequest(batch.LeaderNodeId, batch.LeaderTerm, batch.PrevLogIndex, batch.PrevLogTerm, batch.LeaderCommitIndex, entries);
        var result = await _logs[nodeId].AppendAsync(request, cancellationToken).ConfigureAwait(false);
        if (result.Success)
            RecordAppended(nodeId, result.LastLogIndex);
        else
            Refusals.Enqueue((nodeId, result.RefusalCode));

        return result;
    }

    /// <summary>Waits until a batch accepted by a follower log leaves it holding the log through <paramref name="index" />.</summary>
    /// <param name="node">The follower node.</param>
    /// <param name="index">The log index the follower must hold.</param>
    /// <returns>A task that completes once the follower holds the index.</returns>
    internal Task AppendedAsync(string node, ulong index)
    {
        lock (_sync)
        {
            if (_through.TryGetValue(node, out var held) && held >= index)
                return Task.CompletedTask;

            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _appendWaiters.Add((node, index, done));
            return done.Task;
        }
    }

    /// <summary>Makes the next batch with entries sent to <paramref name="node" /> park before it reaches the follower log.</summary>
    /// <param name="node">The follower node.</param>
    internal void ParkNext(string node)
    {
        lock (_sync)
            _parkNode = node;
    }

    /// <summary>Lets the parked call, and every later one, reach the follower log.</summary>
    internal void Release() => _ = _release.TrySetResult();

    private void RecordAppended(string node, ulong last)
    {
        List<TaskCompletionSource> ready = [];
        lock (_sync)
        {
            _through[node] = _through.TryGetValue(node, out var held) ? Math.Max(held, last) : last;
            for (var i = _appendWaiters.Count - 1; i >= 0; i--)
            {
                var waiter = _appendWaiters[i];
                if (!string.Equals(waiter.Node, node, StringComparison.Ordinal) || waiter.Index > _through[node])
                    continue;

                ready.Add(waiter.Done);
                _appendWaiters.RemoveAt(i);
            }
        }

        foreach (var done in CollectionsMarshal.AsSpan(ready))
            _ = done.TrySetResult();
    }

    private bool TakePark(string node)
    {
        lock (_sync)
        {
            if (!string.Equals(_parkNode, node, StringComparison.Ordinal))
                return false;

            _parkNode = null;
            return true;
        }
    }
}
