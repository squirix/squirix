using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Threading;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Sends the appends of one follower slot one at a time, in log order, batching what accumulates while a send is in flight.</summary>
/// <remarks>
/// A follower refuses an append whose predecessor it does not hold, so an append must never overtake an earlier one to the same
/// follower. The sender keeps at most one append request in flight: entries enqueued meanwhile wait and then go out together, as one
/// request, while they stay contiguous in index and term. There are no retries and no repair: a failed or refused request fails the
/// entries it carried, and later entries are still sent once. <see cref="EnqueueAsync" /> never waits, so a slow follower cannot hold up
/// its caller. A catch-up pauses the live sends through <see cref="BeginCatchUpAsync" /> and sends in their place, so the follower never
/// sees two senders at once. A heartbeat goes out only while the slot is idle: an empty append at the last enqueued entry, so it never
/// overtakes an entry; every follower reply, to an append or a heartbeat, is handed to <see cref="ReplyObserver" />.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaFollowerSender : IAsyncDisposable
{
    private const string BacklogFullMessage = "follower append backlog full";

    private readonly TimeSpan _appendTimeout;
    private readonly Backlog _backlog = new();
    private readonly CancellationTokenSource _closing = new();
    private readonly CancellationTokenSource _drainStarted = new();
    private readonly ReplicaRpcHeader _header;
    private readonly IReplicaRpcGateway _rpc;
    private readonly Lock _sync = new();
    private TaskCompletionSource? _catchUpRequest;
    private bool _closed;
    private ulong? _deferredHeartbeat;
    private ulong _lastEnqueuedIndex;
    private ulong _lastEnqueuedTerm;
    private TaskCompletionSource? _leaseDone;
    private TaskCompletionSource? _loopDone;

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
        NodeId = nodeId;
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

    /// <summary>Gets a token canceled once the sender starts draining or closes.</summary>
    /// <remarks>
    /// A drain runs under the commit gate and waits for an active catch-up lease, so a lease holder waiting for that gate links this
    /// token into its wait and gives up instead of holding the drain for its whole budget.
    /// </remarks>
    internal CancellationToken DrainStarted
    {
        get
        {
            // The source is disposed only after the close, so a closed sender hands out an already canceled token instead.
            lock (_sync)
                return _closed ? new CancellationToken(true) : _drainStarted.Token;
        }
    }

    /// <summary>Gets a value indicating whether the sender is closed.</summary>
    internal bool IsClosed
    {
        get
        {
            lock (_sync)
                return _closed;
        }
    }

    /// <summary>Initializes the most canonical payload bytes one request carries; 4 MiB unless set. A single larger entry still goes out alone.</summary>
    internal long MaxBatchBytes
    {
        private get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1L);

            field = value;
        }
    }

    /// <summary>Initializes the most entries one request carries; 64 unless set.</summary>
    internal int MaxBatchEntries
    {
        private get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1);

            field = value;
        }
    }

    /// <summary>Initializes the most canonical payload bytes waiting to be sent; 64 MiB unless set. A single larger entry is still accepted while nothing else waits.</summary>
    internal long MaxPendingBytes
    {
        private get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1L);

            field = value;
        }
    }

    /// <summary>Initializes the most entries waiting to be sent; 1024 unless set.</summary>
    internal int MaxPendingEntries
    {
        private get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1);

            field = value;
        }
    }

    /// <summary>Gets the identifier of the follower this sender appends to.</summary>
    internal string NodeId { get; }

    /// <summary>Initializes the owner callback that receives, with <see cref="ReplicaIndex" />, every follower reply to a live append, a heartbeat, or a catch-up request; it must not throw or wait.</summary>
    internal Action<int, FollowerLogAppendResult>? ReplyObserver { private get; init; }

    /// <summary>Initializes the slot of the follower in its group, handed to <see cref="ReplyObserver" />; zero unless set.</summary>
    internal int ReplicaIndex { private get; init; }

    /// <summary>Initializes the longest wait on dispose for the request in flight; 30 seconds unless set.</summary>
    internal TimeSpan ShutdownBudget
    {
        private get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero);

            field = value;
        }
    }

    /// <summary>Initializes the owner callback that reports, with the shutdown budget, a dispose that gave up on a request that ignored cancellation.</summary>
    internal Action<TimeSpan>? ShutdownLeakReporter { private get; init; }

    /// <summary>Initializes the time source of the append timeout; the system clock unless set.</summary>
    internal TimeProvider TimeProvider { private get; init; } = TimeProvider.System;

    /// <summary>Gets a value indicating whether the sender stopped admitting entries for a drain.</summary>
    private bool Draining => _drainStarted.IsCancellationRequested;

    /// <inheritdoc />
    /// <remarks>
    /// Waiting entries fail with <see cref="ObjectDisposedException" /> and the request in flight, live or catch-up, is canceled. A
    /// gateway that ignores cancellation is given up on after <see cref="ShutdownBudget" />; the dispose then returns without throwing.
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
            abandoned = _backlog.Clear();
            loop = _loopDone?.Task ?? _catchUpRequest?.Task;
        }

        for (var i = 0; i < abandoned.Length; i++)
            _ = abandoned[i].Completion.TrySetException(new ObjectDisposedException(nameof(ReplicaFollowerSender)));

        await _closing.CancelAsync().ConfigureAwait(false);
        await _drainStarted.CancelAsync().ConfigureAwait(false);
        if (loop == null)
        {
            _closing.Dispose();
            _drainStarted.Dispose();
            return;
        }

        try
        {
            await loop.WaitAsync(ShutdownBudget, TimeProvider.System, CancellationToken.None).ConfigureAwait(false);
            _closing.Dispose();
            _drainStarted.Dispose();
        }
        catch (TimeoutException)
        {
            // The gateway ignored cancellation: the loop is abandoned and ends on its own once the call returns.
            ShutdownLeakReporter?.Invoke(ShutdownBudget);
        }
    }

    /// <summary>Suspends the live sends of this slot once the request in flight is answered, so a catch-up may send in its place.</summary>
    /// <param name="cancellationToken">Cancellation token; a cancellation while the request in flight is waited for resumes the live sends.</param>
    /// <returns>The lease; disposing it resumes the live sends.</returns>
    /// <exception cref="InvalidOperationException">A lease is already active.</exception>
    /// <exception cref="ObjectDisposedException">The sender is closed or draining.</exception>
    /// <remarks>Entries enqueued while the lease is held wait; they still count against the backlog limits.</remarks>
    internal async ValueTask<ReplicaFollowerCatchUp> BeginCatchUpAsync(CancellationToken cancellationToken)
    {
        Task? loop;
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_closed || Draining, this);
            if (_leaseDone != null)
                throw new InvalidOperationException($"Follower '{NodeId}' catch-up lease is already active.");

            _leaseDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            loop = _loopDone?.Task;
        }

        if (loop == null)
            return new ReplicaFollowerCatchUp(this, NodeId);
        try
        {
            await loop.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            EndCatchUp(0);
            throw;
        }

        return new ReplicaFollowerCatchUp(this, NodeId);
    }

    /// <summary>Stops admitting entries and waits until the ones already queued, and the request in flight, have been answered.</summary>
    /// <param name="budget">The longest wait; entries still queued after it are left for <see cref="DisposeAsync" /> to fail.</param>
    /// <returns>A task that completes when the sender is idle or the budget elapsed.</returns>
    /// <remarks>
    /// Used where a pipeline is replaced but its follower must still receive every entry the leader already appended. An active catch-up
    /// lease is waited for first: its next request is refused, so the catch-up ends and resumes the loop that sends the rest.
    /// </remarks>
    internal async ValueTask DrainAsync(TimeSpan budget)
    {
        // Draining is the canceled state of the token, set before the lease is read: an enqueue, a lease, or a catch-up send that takes
        // the lock afterwards is refused. A closed sender has nothing to drain; the pipeline drains before it closes, never concurrently.
        lock (_sync)
        {
            if (_closed)
                return;
        }

        await _drainStarted.CancelAsync().ConfigureAwait(false);
        Task? lease;
        lock (_sync)
            lease = _leaseDone?.Task;

        var started = TimeProvider.System.GetTimestamp();
        if (lease != null && !await WaitWithinAsync(lease, budget).ConfigureAwait(false))
            return;

        Task? loop;
        lock (_sync)
            loop = _loopDone?.Task;

        var remaining = budget - TimeProvider.System.GetElapsedTime(started);
        if (loop != null && remaining > TimeSpan.Zero)
            _ = await WaitWithinAsync(loop, remaining).ConfigureAwait(false);
    }

    /// <summary>Resumes the live sends after a catch-up lease ended; idempotent.</summary>
    /// <param name="heldThrough">The highest log index the follower accepted from the catch-up; waiting entries through it are acknowledged without a request.</param>
    /// <remarks>After the sender closed, only the lease signal is completed: the waiting entries were failed by the close.</remarks>
    internal void EndCatchUp(ulong heldThrough)
    {
        List<PendingAppend>? held = null;
        TaskCompletionSource? leaseDone;
        TaskCompletionSource? started = null;
        lock (_sync)
        {
            leaseDone = _leaseDone;
            if (leaseDone == null)
                return;

            _leaseDone = null;
            if (!_closed)
                held = _backlog.TakeThrough(heldThrough);

            if (!_closed && _backlog.Count > 0 && _loopDone == null)
                started = ClaimLoop();
        }

        if (held != null)
            Backlog.Acknowledge(held);

        if (started != null)
            StartLoop(started);

        _ = leaseDone.TrySetResult();
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
    internal Task<ReplicaDurableAcknowledgement> EnqueueAsync(
        PreparedReplicaMutation mutation,
        in ReplicaLogRecord record,
        ulong prevLogIndex,
        ulong prevLogTerm,
        ulong leaderCommitIndex)
    {
        ArgumentNullException.ThrowIfNull(mutation);

        var completion = new TaskCompletionSource<ReplicaDurableAcknowledgement>(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = new PendingAppend(mutation, in record, prevLogIndex, prevLogTerm, leaderCommitIndex, completion);
        TaskCompletionSource? started = null;
        lock (_sync)
        {
            if (_closed || Draining)
                return Task.FromException<ReplicaDurableAcknowledgement>(new ObjectDisposedException(nameof(ReplicaFollowerSender)));

            if (record.LogIndex <= _lastEnqueuedIndex || record.Term < _lastEnqueuedTerm)
            {
                return Task.FromException<ReplicaDurableAcknowledgement>(
                    new InvalidOperationException($"Follower '{NodeId}' append out of order: index {record.LogIndex} after {_lastEnqueuedIndex}."));
            }

            if (_backlog.IsFullFor(item, MaxPendingEntries, MaxPendingBytes))
                return Task.FromException<ReplicaDurableAcknowledgement>(new InvalidOperationException(BacklogFullMessage));

            _backlog.Enqueue(item);
            _lastEnqueuedIndex = record.LogIndex;
            _lastEnqueuedTerm = record.Term;
            if (_loopDone == null && _leaseDone == null)
                started = ClaimLoop();
        }

        if (started != null)
            StartLoop(started);

        return completion.Task;
    }

    /// <summary>Sends one heartbeat when the slot is idle: an empty append at the last enqueued entry, carrying the leader commit index.</summary>
    /// <param name="leaderCommitIndex">Leader commit index to carry.</param>
    /// <param name="deferWhileBusy">
    /// Whether a busy slot sends the heartbeat once the send loop runs out of entries, unless a later request of the loop goes out first; a
    /// read-index round needs a request sent after it started, and the one in flight was sent before.
    /// </param>
    /// <returns><see langword="true" /> when the heartbeat went out; <see langword="false" /> when an append, a catch-up, or a drain is under way, or the sender is closed.</returns>
    /// <remarks>
    /// Never waits. Entries enqueued while the heartbeat is in flight are sent after it. A busy slot needs no heartbeat to keep the follower
    /// in contact: its live appends reach the follower and their replies are observed the same way.
    /// </remarks>
    internal bool TryEnqueueHeartbeat(ulong leaderCommitIndex, bool deferWhileBusy = false)
    {
        TaskCompletionSource started;
        FollowerBatch heartbeat;
        lock (_sync)
        {
            if (_closed || Draining)
                return false;

            if (_backlog.Count > 0 || _loopDone != null || _leaseDone != null)
            {
                if (deferWhileBusy)
                    _deferredHeartbeat = leaderCommitIndex;

                return false;
            }

            heartbeat = new FollowerBatch([], _header.LeaderNodeId, _header.Term, _lastEnqueuedIndex, _lastEnqueuedTerm, leaderCommitIndex);
            started = ClaimLoop();
        }

        StartLoop(started, heartbeat);
        return true;
    }

    /// <summary>Sends one catch-up request with the sender's identity, per-request timeout, and closing token.</summary>
    /// <param name="batch">The request.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// The follower's answer. The task fails with <see cref="ObjectDisposedException" /> when the sender is closed or draining and with
    /// <see cref="InvalidOperationException" /> when a catch-up request is already in flight.
    /// </returns>
    internal Task<FollowerLogAppendResult> SendCatchUpAsync(in FollowerBatch batch, CancellationToken cancellationToken)
    {
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_sync)
        {
            if (_closed || Draining)
                return Task.FromException<FollowerLogAppendResult>(new ObjectDisposedException(nameof(ReplicaFollowerSender)));

            if (_catchUpRequest != null)
                return Task.FromException<FollowerLogAppendResult>(new InvalidOperationException($"Follower '{NodeId}' catch-up request is already in flight."));

            _catchUpRequest = done;
        }

        return SendCatchUpCoreAsync(batch, done, cancellationToken);
    }

    private static async ValueTask<bool> WaitWithinAsync(Task task, TimeSpan budget)
    {
        try
        {
            await task.WaitAsync(budget, TimeProvider.System, CancellationToken.None).ConfigureAwait(false);
            return true;
        }
        catch (TimeoutException)
        {
            // The rest is failed by the dispose that follows.
            return false;
        }
    }

    private async Task RunAsync(TaskCompletionSource done, FollowerBatch? heartbeat)
    {
        // A failed heartbeat fails nothing: the follower simply does not count as heard from.
        if (heartbeat is { } empty)
            _ = await SendHeartbeatAsync(empty).CaptureFailureAsync().ConfigureAwait(false);

        // A pending-free exit is decided under the lock in TakeBatch, so an enqueue racing the exit starts a loop of its own.
        while (true)
        {
            if (TakeBatch(out var deferred) is { } batch)
            {
                if (await SendBatchAsync(batch).CaptureFailureAsync().ConfigureAwait(false) is { } error)
                    Backlog.Fail(batch, error);
            }
            else if (deferred is { } beat)
            {
                _ = await SendHeartbeatAsync(beat).CaptureFailureAsync().ConfigureAwait(false);
            }
            else
            {
                break;
            }
        }

        _ = done.TrySetResult();
    }

    /// <summary>Sends one request and completes the entries it carried with the follower's answer.</summary>
    /// <param name="batch">The entries of the request, in log order.</param>
    /// <returns>A task that faults when the request fails or times out; the caller fails the entries then.</returns>
    private async Task SendBatchAsync(List<PendingAppend> batch)
    {
        var request = Backlog.ToRequest(batch, _header.LeaderNodeId);
        using var timeout = new CancellationTokenSource(_appendTimeout, TimeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_closing.Token, timeout.Token);
        var result = await _rpc.AppendEntriesAsync(NodeId, _header, request, linked.Token).ConfigureAwait(false);
        Backlog.Complete(batch, in result, NodeId);

        // After the entries are answered: an observer that throws faults only this request task, never an acknowledged entry.
        ReplyObserver?.Invoke(ReplicaIndex, result);
    }

    private async Task SendHeartbeatAsync(FollowerBatch heartbeat)
    {
        using var timeout = new CancellationTokenSource(_appendTimeout, TimeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_closing.Token, timeout.Token);
        var result = await _rpc.AppendEntriesAsync(NodeId, _header, heartbeat, linked.Token).ConfigureAwait(false);
        ReplyObserver?.Invoke(ReplicaIndex, result);
    }

    private async Task<FollowerLogAppendResult> SendCatchUpCoreAsync(FollowerBatch batch, TaskCompletionSource done, CancellationToken cancellationToken)
    {
        try
        {
            using var timeout = new CancellationTokenSource(_appendTimeout, TimeProvider);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(_closing.Token, timeout.Token, cancellationToken);
            var result = await _rpc.AppendEntriesAsync(NodeId, _header, batch, linked.Token).ConfigureAwait(false);

            // A follower under catch-up gets no heartbeat: its catch-up replies are its contact with the leader.
            ReplyObserver?.Invoke(ReplicaIndex, result);
            return result;
        }
        finally
        {
            lock (_sync)
                _catchUpRequest = null;

            _ = done.TrySetResult();
        }
    }

    /// <summary>Records a new send loop as running; the caller holds the lock and starts the loop once it released it.</summary>
    /// <returns>The completion source the loop completes when it ends.</returns>
    private TaskCompletionSource ClaimLoop()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _loopDone = started;
        return started;
    }

    /// <summary>Starts the send loop on the calling thread, up to its first wait, without the caller's execution context.</summary>
    /// <param name="done">The completion source the loop completes when it ends.</param>
    /// <param name="heartbeat">An empty append the loop sends before any waiting entry, or <see langword="null" />.</param>
    /// <remarks>
    /// The first send runs inline so an idle slot sends at once. The flow suppression covers only the start, and the loop's
    /// continuations then run without the ambient scope of whoever enqueued first.
    /// </remarks>
    private void StartLoop(TaskCompletionSource done, FollowerBatch? heartbeat = null)
    {
        if (ExecutionContext.IsFlowSuppressed())
        {
            _ = RunAsync(done, heartbeat);
            return;
        }

        using (ExecutionContext.SuppressFlow())
            _ = RunAsync(done, heartbeat);
    }

    /// <summary>Takes the next request from the waiting entries, or ends the loop when none wait or a catch-up lease paused the sends.</summary>
    /// <param name="heartbeat">The heartbeat deferred while the slot was busy, to send next when no entry waits; otherwise <see langword="null" />.</param>
    /// <returns>The entries of the next request, or <see langword="null" /> when the loop sends the deferred heartbeat or ended.</returns>
    /// <remarks>The loop ends, and a later enqueue or lease end starts a new one, under the same lock hold that saw the entries run out.</remarks>
    private List<PendingAppend>? TakeBatch(out FollowerBatch? heartbeat)
    {
        heartbeat = null;
        lock (_sync)
        {
            if (_backlog.Count == 0 || _leaseDone != null)
            {
                if (_deferredHeartbeat is { } commitIndex && _leaseDone == null && !_closed && !Draining)
                {
                    _deferredHeartbeat = null;
                    heartbeat = new FollowerBatch([], _header.LeaderNodeId, _header.Term, _lastEnqueuedIndex, _lastEnqueuedTerm, commitIndex);
                    return null;
                }

                _loopDone = null;
                return null;
            }

            // The request taken now is sent after the deferral, so it stands in for the deferred heartbeat.
            _deferredHeartbeat = null;
            return _backlog.TakeBatch(MaxBatchEntries, MaxBatchBytes);
        }
    }

    /// <summary>The entries of one follower slot waiting for their request, in log order, with the canonical payload bytes they hold.</summary>
    /// <remarks>Not thread-safe: the sender reads and changes it under its own lock only.</remarks>
    private sealed class Backlog
    {
        private readonly Queue<PendingAppend> _pending = new();
        private long _pendingBytes;

        /// <summary>Gets the number of waiting entries.</summary>
        internal int Count => _pending.Count;

        /// <summary>Acknowledges entries the follower already holds, without a request.</summary>
        /// <param name="entries">The entries.</param>
        internal static void Acknowledge(List<PendingAppend> entries)
        {
            for (var i = 0; i < entries.Count; i++)
                _ = entries[i].Completion.TrySetResult(AcknowledgementOf(entries[i].Mutation));
        }

        /// <summary>Completes the entries of a request with the follower's answer.</summary>
        /// <param name="batch">The entries of the request.</param>
        /// <param name="result">The follower's answer.</param>
        /// <param name="nodeId">The follower, named in the refusal.</param>
        internal static void Complete(List<PendingAppend> batch, in FollowerLogAppendResult result, string nodeId)
        {
            for (var i = 0; i < batch.Count; i++)
            {
                _ = result.Success ? batch[i].Completion.TrySetResult(AcknowledgementOf(batch[i].Mutation))
                    : batch[i].Completion.TrySetException(new InvalidOperationException($"Follower '{nodeId}' refused append: {result.RefusalCode}."));
            }
        }

        /// <summary>Fails the entries of a request that failed or timed out.</summary>
        /// <param name="batch">The entries of the request.</param>
        /// <param name="error">The failure.</param>
        internal static void Fail(List<PendingAppend> batch, Exception error)
        {
            for (var i = 0; i < batch.Count; i++)
                _ = batch[i].Completion.TrySetException(error);
        }

        /// <summary>Builds the request that carries the entries of a batch.</summary>
        /// <param name="batch">The entries, in log order; the first one names the term and the predecessor of the request.</param>
        /// <param name="leaderNodeId">The leader that sends the request.</param>
        /// <returns>The request, carrying the leader commit index of the last entry.</returns>
        internal static FollowerBatch ToRequest(List<PendingAppend> batch, string leaderNodeId)
        {
            var records = new ReplicaLogRecord[batch.Count];
            for (var i = 0; i < records.Length; i++)
                records[i] = batch[i].Record;

            var first = batch[0];
            return new FollowerBatch(records, leaderNodeId, first.Record.Term, first.PrevLogIndex, first.PrevLogTerm, batch[^1].LeaderCommitIndex);
        }

        /// <summary>Takes every waiting entry out, as when the sender closes.</summary>
        /// <returns>The entries that were waiting, in log order.</returns>
        internal PendingAppend[] Clear()
        {
            PendingAppend[] abandoned = [.. _pending];
            _pending.Clear();
            _pendingBytes = 0;
            return abandoned;
        }

        /// <summary>Queues one entry after the waiting ones.</summary>
        /// <param name="item">The entry.</param>
        internal void Enqueue(PendingAppend item)
        {
            _pending.Enqueue(item);
            _pendingBytes += item.Bytes;
        }

        /// <summary>Tells whether the backlog refuses one more entry: it is at its entry limit, or the entry would pass its byte limit while others wait.</summary>
        /// <param name="item">The entry to queue.</param>
        /// <param name="maxEntries">The most entries waiting.</param>
        /// <param name="maxBytes">The most canonical payload bytes waiting; a single larger entry is still accepted while nothing else waits.</param>
        /// <returns><see langword="true" /> when the entry does not fit.</returns>
        internal bool IsFullFor(PendingAppend item, int maxEntries, long maxBytes) => _pending.Count >= maxEntries || (_pending.Count > 0 && _pendingBytes + item.Bytes > maxBytes);

        /// <summary>Takes the next request: the first waiting entry and those after it that directly continue it, within the batch limits.</summary>
        /// <param name="maxEntries">The most entries one request carries.</param>
        /// <param name="maxBytes">The most canonical payload bytes one request carries; a single larger entry still goes out alone.</param>
        /// <returns>The entries of the request, in log order.</returns>
        /// <remarks>Called only while some entry waits.</remarks>
        internal List<PendingAppend> TakeBatch(int maxEntries, long maxBytes)
        {
            var first = Take();
            var batch = new List<PendingAppend>(Math.Min(_pending.Count + 1, maxEntries)) { first };
            var bytes = first.Bytes;
            var last = first;
            while (batch.Count < maxEntries && _pending.Count > 0)
            {
                var next = _pending.Peek();
                if (!next.Follows(last) || bytes + next.Bytes > maxBytes)
                    break;

                _ = Take();
                bytes += next.Bytes;
                batch.Add(next);
                last = next;
            }

            return batch;
        }

        /// <summary>Takes the waiting entries through a log index out, in log order.</summary>
        /// <param name="heldThrough">The highest log index to take.</param>
        /// <returns>The entries taken, or <see langword="null" /> when none waits at or below <paramref name="heldThrough" />.</returns>
        internal List<PendingAppend>? TakeThrough(ulong heldThrough)
        {
            List<PendingAppend>? held = null;
            while (_pending.Count > 0 && _pending.Peek().Record.LogIndex <= heldThrough)
                (held ??= []).Add(Take());

            return held;
        }

        private static ReplicaDurableAcknowledgement AcknowledgementOf(PreparedReplicaMutation mutation) => new(
            mutation.GroupId,
            mutation.Term,
            mutation.LogIndex,
            mutation.OperationFingerprint,
            mutation.PayloadChecksum,
            true,
            true);

        private PendingAppend Take()
        {
            var item = _pending.Dequeue();
            _pendingBytes -= item.Bytes;
            return item;
        }
    }

    /// <summary>One entry waiting for its request, with the positions it is sent at.</summary>
    [Immutable]
    private sealed class PendingAppend
    {
        internal PendingAppend(
            PreparedReplicaMutation mutation,
            in ReplicaLogRecord record,
            ulong prevLogIndex,
            ulong prevLogTerm,
            ulong leaderCommitIndex,
            TaskCompletionSource<ReplicaDurableAcknowledgement> completion)
        {
            Mutation = mutation;
            Record = record;
            PrevLogIndex = prevLogIndex;
            PrevLogTerm = prevLogTerm;
            LeaderCommitIndex = leaderCommitIndex;
            Bytes = mutation.CanonicalPayload.Length;
            Completion = completion;
        }

        internal long Bytes { get; }

        internal TaskCompletionSource<ReplicaDurableAcknowledgement> Completion { get; }

        internal ulong LeaderCommitIndex { get; }

        internal PreparedReplicaMutation Mutation { get; }

        internal ulong PrevLogIndex { get; }

        internal ulong PrevLogTerm { get; }

        internal ReplicaLogRecord Record { get; }

        /// <summary>Checks whether this entry directly continues <paramref name="last" /> in index and term, as a request needs.</summary>
        /// <param name="last">The entry that precedes this one in the request.</param>
        /// <returns><see langword="true" /> when both can go out in one request.</returns>
        internal bool Follows(PendingAppend last) => Record.LogIndex == last.Record.LogIndex + 1 && PrevLogIndex == last.Record.LogIndex && PrevLogTerm == last.Record.Term &&
                                                     Record.Term == last.Record.Term;
    }
}
