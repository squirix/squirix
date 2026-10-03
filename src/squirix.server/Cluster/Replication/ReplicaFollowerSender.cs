using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Sends the appends of one follower slot one at a time, in log order, batching what accumulates while a send is in flight.</summary>
/// <remarks>
/// A follower refuses an append whose predecessor it does not hold, so an append must never overtake an earlier one to the same
/// follower. The sender keeps at most one append request in flight: entries enqueued meanwhile wait and then go out together, as one
/// request, while they stay contiguous in index and term. There are no retries and no repair: a failed or refused request fails the
/// entries it carried, and later entries are still sent once. <see cref="EnqueueAsync" /> never waits, so a slow follower cannot hold up
/// its caller.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaFollowerSender : IAsyncDisposable
{
    private const string BacklogFullMessage = "follower append backlog full";

    private readonly TimeSpan _appendTimeout;
    private readonly CancellationTokenSource _closing = new();
    private readonly ReplicaRpcHeader _header;
    private readonly string _nodeId;
    private readonly Queue<PendingAppend> _pending = new();
    private readonly IReplicaRpcGateway _rpc;
    private readonly Lock _sync = new();
    private bool _closed;
    private TaskCompletionSource? _loopDone;
    private long _pendingBytes;
    private ulong _lastEnqueuedIndex;
    private ulong _lastEnqueuedTerm;

    /// <summary>Initializes a new instance of the <see cref="ReplicaFollowerSender" /> class.</summary>
    /// <param name="rpc">Follower replication RPCs.</param>
    /// <param name="nodeId">Identifier of the follower this sender serves.</param>
    /// <param name="header">Replication envelope identity for follower calls.</param>
    /// <param name="lastIndex">Log index of the last entry the leader log held when the sender was created; later entries must follow it.</param>
    /// <param name="lastTerm">Term of the entry at <paramref name="lastIndex" />.</param>
    /// <param name="appendTimeout">Longest time one append request may take before it is canceled and failed.</param>
    internal ReplicaFollowerSender(IReplicaRpcGateway rpc, string nodeId, in ReplicaRpcHeader header, ulong lastIndex, ulong lastTerm, TimeSpan appendTimeout)
    {
        ArgumentNullException.ThrowIfNull(rpc);
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(appendTimeout, TimeSpan.Zero);

        _rpc = rpc;
        _nodeId = nodeId;
        _header = header;
        _lastEnqueuedIndex = lastIndex;
        _lastEnqueuedTerm = lastTerm;
        _appendTimeout = appendTimeout;
        ShutdownBudget = TimeSpan.FromSeconds(30);
        MaxPendingBytes = 64L * 1024 * 1024;
        MaxPendingEntries = 1024;
        MaxBatchBytes = 4 * 1024 * 1024;
        MaxBatchEntries = 64;
    }

    /// <summary>Gets the most entries one request carries; 64 unless set.</summary>
    internal int MaxBatchEntries
    {
        private get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1);

            field = value;
        }
    }

    /// <summary>Gets the most canonical payload bytes one request carries; 4 MiB unless set. A single larger entry still goes out alone.</summary>
    internal long MaxBatchBytes
    {
        private get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1L);

            field = value;
        }
    }

    /// <summary>Gets the most entries waiting to be sent; 1024 unless set.</summary>
    internal int MaxPendingEntries
    {
        private get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1);

            field = value;
        }
    }

    /// <summary>Gets the most canonical payload bytes waiting to be sent; 64 MiB unless set. A single larger entry is still accepted while nothing else waits.</summary>
    internal long MaxPendingBytes
    {
        private get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1L);

            field = value;
        }
    }

    /// <summary>Gets the longest wait on dispose for the request in flight; 30 seconds unless set.</summary>
    internal TimeSpan ShutdownBudget
    {
        private get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero);

            field = value;
        }
    }

    /// <summary>Gets the time source of the append timeout; the system clock unless set.</summary>
    internal TimeProvider TimeProvider { private get; init; } = TimeProvider.System;

    /// <inheritdoc />
    /// <remarks>
    /// Waiting entries fail with <see cref="ObjectDisposedException" /> and the request in flight is canceled. A gateway that ignores
    /// cancellation is given up on after <see cref="ShutdownBudget" />; the dispose then returns without throwing.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        PendingAppend[] abandoned;
        Task? loop;
        lock (_sync)
        {
            if (_closed)
                return;

            _closed = true;
            abandoned = [.. _pending];
            _pending.Clear();
            _pendingBytes = 0;
            loop = _loopDone?.Task;
        }

        for (var i = 0; i < abandoned.Length; i++)
            _ = abandoned[i].Completion.TrySetException(new ObjectDisposedException(nameof(ReplicaFollowerSender)));

        await _closing.CancelAsync().ConfigureAwait(false);
        if (loop == null)
        {
            _closing.Dispose();
            return;
        }

        try
        {
            await loop.WaitAsync(ShutdownBudget, TimeProvider.System, CancellationToken.None).ConfigureAwait(false);
            _closing.Dispose();
        }
        catch (TimeoutException)
        {
            // The gateway ignored cancellation: the loop is abandoned and ends on its own once the call returns.
        }
    }

    /// <summary>Queues one entry for the follower and returns the task of its acknowledgement; never waits.</summary>
    /// <param name="mutation">Prepared mutation the entry carries.</param>
    /// <param name="record">The decoded entry.</param>
    /// <param name="prevLogIndex">Index of the entry that precedes the entry in the leader log.</param>
    /// <param name="prevLogTerm">Term of the entry at <paramref name="prevLogIndex" />.</param>
    /// <param name="leaderCommitIndex">Leader commit index to carry with the entry.</param>
    /// <returns>
    /// The task of the follower's acknowledgement. It fails, instead of this call throwing, when the sender is disposed, the backlog is
    /// full, the entry does not follow the entries enqueued before it, the follower refuses the request, or the request fails.
    /// </returns>
    [SuppressMessage("Usage", "VSTHRD003", Justification = "The completion source is created by this call and completed only by the send loop or by the dispose drain.")]
    internal Task<ReplicaDurableAcknowledgement> EnqueueAsync(PreparedReplicaMutation mutation, in ReplicaLogRecord record, ulong prevLogIndex, ulong prevLogTerm, ulong leaderCommitIndex)
    {
        ArgumentNullException.ThrowIfNull(mutation);

        var item = new PendingAppend(mutation, in record, prevLogIndex, prevLogTerm, leaderCommitIndex);
        TaskCompletionSource? started = null;
        lock (_sync)
        {
            if (_closed)
                return Task.FromException<ReplicaDurableAcknowledgement>(new ObjectDisposedException(nameof(ReplicaFollowerSender)));

            if (record.LogIndex <= _lastEnqueuedIndex || record.Term < _lastEnqueuedTerm)
                return Task.FromException<ReplicaDurableAcknowledgement>(new InvalidOperationException("follower append out of order"));

            if (_pending.Count >= MaxPendingEntries || (_pending.Count > 0 && _pendingBytes + item.Bytes > MaxPendingBytes))
                return Task.FromException<ReplicaDurableAcknowledgement>(new InvalidOperationException(BacklogFullMessage));

            _pending.Enqueue(item);
            _pendingBytes += item.Bytes;
            _lastEnqueuedIndex = record.LogIndex;
            _lastEnqueuedTerm = record.Term;
            if (_loopDone == null)
            {
                started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _loopDone = started;
            }
        }

        if (started != null)
            StartLoop(started);

        return item.Completion.Task;
    }

    private static void Complete(List<PendingAppend> batch, in FollowerLogAppendResult result, string nodeId)
    {
        for (var i = 0; i < batch.Count; i++)
        {
            var mutation = batch[i].Mutation;
            _ = result.Success
                ? batch[i].Completion.TrySetResult(new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true))
                : batch[i].Completion.TrySetException(new InvalidOperationException($"Follower '{nodeId}' refused append: {result.RefusalCode}."));
        }
    }

    private static void Fail(List<PendingAppend> batch, Exception error)
    {
        for (var i = 0; i < batch.Count; i++)
            _ = batch[i].Completion.TrySetException(error);
    }

    /// <summary>Takes the next request from the waiting entries, or ends the loop when none wait.</summary>
    /// <returns>The entries of the next request, or <see langword="null" /> when the loop ended.</returns>
    /// <remarks>The loop ends, and a later enqueue starts a new one, under the same lock hold that saw the entries run out.</remarks>
    private List<PendingAppend>? TakeBatch()
    {
        lock (_sync)
        {
            if (_pending.Count == 0)
            {
                _loopDone = null;
                return null;
            }

            var first = _pending.Dequeue();
            _pendingBytes -= first.Bytes;
            var batch = new List<PendingAppend>(Math.Min(_pending.Count + 1, MaxBatchEntries)) { first };
            var bytes = first.Bytes;
            var last = first;
            while (batch.Count < MaxBatchEntries && _pending.Count > 0)
            {
                var next = _pending.Peek();
                if (!next.Follows(last) || bytes + next.Bytes > MaxBatchBytes)
                    break;

                _ = _pending.Dequeue();
                _pendingBytes -= next.Bytes;
                bytes += next.Bytes;
                batch.Add(next);
                last = next;
            }

            return batch;
        }
    }

    [SuppressMessage("Design", "CA1031", Justification = "The loop must never fault: every failure of a request is delivered to the entries it carried.")]
    private async Task RunAsync(TaskCompletionSource done)
    {
        // A pending-free exit is decided under the lock in TakeBatch, so an enqueue racing the exit starts a loop of its own.
        while (TakeBatch() is { } batch)
        {
            try
            {
                var records = new ReplicaLogRecord[batch.Count];
                for (var i = 0; i < records.Length; i++)
                    records[i] = batch[i].Record;

                var first = batch[0];
                var request = new FollowerBatch(records, _header.LeaderNodeId, first.Record.Term, first.PrevLogIndex, first.PrevLogTerm, batch[^1].LeaderCommitIndex);
                using var timeout = new CancellationTokenSource(_appendTimeout, TimeProvider);
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(_closing.Token, timeout.Token);
                var result = await _rpc.AppendEntriesAsync(_nodeId, _header, request, linked.Token).ConfigureAwait(false);
                Complete(batch, in result, _nodeId);
            }
            catch (Exception error)
            {
                Fail(batch, error);
            }
        }

        _ = done.TrySetResult();
    }

    /// <summary>Starts the send loop on the calling thread, up to its first wait, without the caller's execution context.</summary>
    /// <param name="done">The completion source the loop completes when it ends.</param>
    /// <remarks>
    /// The first send runs inline so an idle slot sends at once. The flow suppression covers only the start, and the loop's
    /// continuations then run without the ambient scope of whoever enqueued first.
    /// </remarks>
    private void StartLoop(TaskCompletionSource done)
    {
        if (ExecutionContext.IsFlowSuppressed())
        {
            _ = RunAsync(done);
            return;
        }

        using (ExecutionContext.SuppressFlow())
            _ = RunAsync(done);
    }

    /// <summary>One entry waiting for its request, with the positions it is sent at.</summary>
    [Immutable]
    private sealed class PendingAppend
    {
        internal PendingAppend(PreparedReplicaMutation mutation, in ReplicaLogRecord record, ulong prevLogIndex, ulong prevLogTerm, ulong leaderCommitIndex)
        {
            Mutation = mutation;
            Record = record;
            PrevLogIndex = prevLogIndex;
            PrevLogTerm = prevLogTerm;
            LeaderCommitIndex = leaderCommitIndex;
            Bytes = mutation.CanonicalPayload.Length;
        }

        internal long Bytes { get; }

        internal TaskCompletionSource<ReplicaDurableAcknowledgement> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal ulong LeaderCommitIndex { get; }

        internal PreparedReplicaMutation Mutation { get; }

        internal ulong PrevLogIndex { get; }

        internal ulong PrevLogTerm { get; }

        internal ReplicaLogRecord Record { get; }

        /// <summary>Checks whether this entry directly continues <paramref name="last" /> in index and term, as a request needs.</summary>
        /// <param name="last">The entry that precedes this one in the request.</param>
        /// <returns><see langword="true" /> when both can go out in one request.</returns>
        internal bool Follows(PendingAppend last) =>
            Record.LogIndex == last.Record.LogIndex + 1 && PrevLogIndex == last.Record.LogIndex && PrevLogTerm == last.Record.Term && Record.Term == last.Record.Term;
    }
}
