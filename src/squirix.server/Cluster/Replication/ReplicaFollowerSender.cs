using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Threading;
using Squirix.Transport.Grpc;

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
    /// <summary>The number of entries a sender queues before it refuses more, unless set.</summary>
    internal const int DefaultMaxPendingEntries = 1024;

    /// <summary>
    /// How long a static leader waits after the last commit before it sends the commit index to a follower that no append carried it to. It is
    /// far longer than the gap between writes of a busy writer, whose next append carries the index, and short enough that a follower holds
    /// the tail of a finished burst well before an election timeout.
    /// </summary>
    internal static readonly TimeSpan CommitIndexDebounce = TimeSpan.FromMilliseconds(50);

    /// <summary>The longest wait before a request that carried a commit index is tried again after it failed or was refused; the wait doubles from <see cref="CommitIndexDebounce" /> up to it.</summary>
    internal static readonly TimeSpan CommitIndexRetryCap = TimeSpan.FromSeconds(2);

    private const string BacklogFullMessage = "follower append backlog full";

    private readonly Backlog _backlog = new();
    private readonly CancellationTokenSource _closing = new();
    private readonly CommitIndexDelivery _commit;
    private readonly CancellationTokenSource _drainStarted = new();
    private readonly ReplicaRpcHeader _header;
    private readonly IReplicaRpcGateway _rpc;
    private readonly FollowerRequests _requests;
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
        _commit = new CommitIndexDelivery(this);
        _requests = new FollowerRequests(this, appendTimeout);
        ShutdownBudget = TimeSpan.FromSeconds(30);
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

    /// <summary>Gets the number of requests in a row that were not accepted, and whether the debounce timer is armed, so a test can wait until a reply was processed.</summary>
    internal (int Failures, bool Armed) CommitRetryState
    {
        get
        {
            lock (_sync)
                return (_commit.Failures, _commit.Armed);
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

    /// <summary>Initializes the backlog and request size limits; <see cref="ReplicaFollowerSenderLimits" /> defaults unless set.</summary>
    internal ReplicaFollowerSenderLimits Limits { private get; init; } = new();

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

    /// <summary>Initializes the time source of the append timeout.</summary>
    internal required TimeProvider TimeProvider { private get; init; }

    /// <summary>Gets a value indicating whether the sender stopped admitting entries for a drain.</summary>
    private bool Draining => _drainStarted.IsCancellationRequested;

    /// <summary>Gets a value indicating whether entries wait, a send loop runs, or a catch-up lease is held; the caller holds the lock.</summary>
    private bool IsBusy => _backlog.Count > 0 || _loopDone != null || _leaseDone != null;

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
            _commit.StopTimer();
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
        TaskCompletionSource? flushing = null;
        FollowerBatch flush = default;
        lock (_sync)
        {
            if (_closed)
                return;

            // The quiet period ends here: the sender that replaces this one starts without the index, so it goes out before the drain ends.
            if (_commit.TryFlush(out flush))
                flushing = ClaimLoop();
        }

        if (flushing != null)
            StartLoop(flushing, flush);

        await _drainStarted.CancelAsync().ConfigureAwait(false);
        Task? lease;
        lock (_sync)
            lease = _leaseDone?.Task;

        var started = TimeProvider.System.GetTimestamp();

        // A wait that runs out of budget leaves the rest to the dispose that follows.
        if (lease != null && await lease.WaitAsync(budget, TimeProvider.System, CancellationToken.None).CaptureFailureAsync(static ex => ex is TimeoutException).ConfigureAwait(false) != null)
            return;

        Task? loop;
        lock (_sync)
            loop = _loopDone?.Task;

        var remaining = budget - TimeProvider.System.GetElapsedTime(started);
        if (loop != null && remaining > TimeSpan.Zero)
            _ = await loop.WaitAsync(remaining, TimeProvider.System, CancellationToken.None).CaptureFailureAsync(static ex => ex is TimeoutException).ConfigureAwait(false);
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

            if (!_closed && (_backlog.Count > 0 || _commit.Pending) && _loopDone == null)
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
    /// <param name="startLoop">
    /// Whether an idle sender starts sending at once. A caller that queues a run of entries passes <see langword="false" /> and calls
    /// <see cref="StartPending" /> after the last one, so the run goes out in as few requests as the batch limits allow instead of
    /// one request for the first entry.
    /// </param>
    /// <returns>
    /// The task of the follower's acknowledgement. It fails, instead of this call throwing, when the sender is disposed, the backlog is
    /// full, the entry does not follow the entries enqueued before it, the follower refuses the request, or the request fails.
    /// </returns>
    internal Task<ReplicaDurableAcknowledgement> EnqueueAsync(
        PreparedReplicaMutation mutation,
        in ReplicaLogRecord record,
        ulong prevLogIndex,
        ulong prevLogTerm,
        ulong leaderCommitIndex,
        bool startLoop = true)
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

            if (_backlog.IsFullFor(item, Limits.MaxPendingEntries, Limits.MaxPendingBytes))
                return Task.FromException<ReplicaDurableAcknowledgement>(new InvalidOperationException(BacklogFullMessage));

            _backlog.Enqueue(item);
            _lastEnqueuedIndex = record.LogIndex;
            _lastEnqueuedTerm = record.Term;
            if (startLoop && _loopDone == null && _leaseDone == null)
                started = ClaimLoop();
        }

        if (started != null)
            StartLoop(started);

        return completion.Task;
    }

    /// <summary>Starts sending the entries queued with the loop start deferred, when the sender is idle; never waits.</summary>
    /// <remarks>It also starts while draining: the entries were admitted before the drain began, so they must still be sent.</remarks>
    internal void StartPending()
    {
        TaskCompletionSource? started = null;
        lock (_sync)
        {
            if (!_closed && _backlog.Count > 0 && _loopDone == null && _leaseDone == null)
                started = ClaimLoop();
        }

        if (started != null)
            StartLoop(started);
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

            if (IsBusy)
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

    /// <summary>Records a commit index the follower must learn, which an append carries or, failing that, one empty append after a quiet period.</summary>
    /// <param name="leaderCommitIndex">The commit index to deliver.</param>
    /// <remarks>
    /// Never waits and never sends by itself. The quiet period counts from the latest call, so a writer that commits faster than
    /// <see cref="CommitIndexDebounce" /> causes no empty append: its appends carry the index. The index counts as delivered only when
    /// the follower accepted a request that carried it; whenever the sender runs out of work, or a catch-up lease ends, and an index is still
    /// undelivered after the quiet period, one empty append carries it. A failed or refused one is tried again after the quiet period. This is
    /// independent of the heartbeat a read-index round defers. A closed or draining sender arms nothing.
    /// </remarks>
    internal void ScheduleCommitIndex(ulong leaderCommitIndex)
    {
        lock (_sync)
            _commit.Schedule(leaderCommitIndex);
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

        return _requests.SendCatchUpCoreAsync(batch, done, cancellationToken);
    }

    private async Task RunAsync(TaskCompletionSource done, FollowerBatch? heartbeat)
    {
        // A failed heartbeat fails nothing: the follower simply does not count as heard from.
        if (heartbeat is { } empty)
            _ = await _requests.SendHeartbeatAsync(empty).CaptureFailureAsync().ConfigureAwait(false);

        // A pending-free exit is decided under the lock in TakeBatch, so an enqueue racing the exit starts a loop of its own.
        while (true)
        {
            if (TakeBatch(out var deferred) is { } batch)
            {
                if (await _requests.SendBatchAsync(batch).CaptureFailureAsync().ConfigureAwait(false) is { } error)
                    Backlog.Fail(batch, error);
            }
            else if (deferred is { } beat)
            {
                _ = await _requests.SendHeartbeatAsync(beat).CaptureFailureAsync().ConfigureAwait(false);
            }
            else
            {
                break;
            }
        }

        _ = done.TrySetResult();
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

                // Out of work: a commit index no accepted request carried yet goes out once the quiet period passed. A draining sender sends it
                // too, as its drain ended the quiet period.
                if (_commit.TryTakeDue(out var due))
                {
                    heartbeat = due;
                    return null;
                }

                _loopDone = null;
                return null;
            }

            // The request taken now is sent after the deferral, so it stands in for the deferred heartbeat.
            _deferredHeartbeat = null;
            return _backlog.TakeBatch(Limits.MaxBatchEntries, Limits.MaxBatchBytes);
        }
    }

    /// <summary>The commit index delivery of the sender: what must reach the follower, what did, and the debounce that delivers the rest.</summary>
    /// <remarks>Not thread-safe: the sender reads and changes it under its own lock only; the timer callback takes that lock.</remarks>
    private sealed class CommitIndexDelivery
    {
        private const int MaxBackoffDoublings = 20;

        private readonly ReplicaFollowerSender _owner;
        private ulong _delivered;
        private bool _flushed;
        private long _lastCommit;
        private ulong _target;
        private ITimer? _timer;

        internal CommitIndexDelivery(ReplicaFollowerSender owner)
        {
            _owner = owner;
        }

        /// <summary>Gets a value indicating whether the debounce timer is armed.</summary>
        internal bool Armed { get; private set; }

        /// <summary>Gets the number of requests in a row that were not accepted; zero after an accepted one.</summary>
        internal int Failures { get; private set; }

        /// <summary>Gets a value indicating whether the follower still has to learn the target.</summary>
        internal bool Pending => _target > _delivered;

        /// <summary>Records how a request that carried a commit index ended; one that was not accepted delivered nothing, and the next try waits out the quiet period again.</summary>
        /// <param name="commitIndex">The commit index the request carried.</param>
        /// <param name="accepted">Whether the follower accepted the request.</param>
        internal void Note(ulong commitIndex, bool accepted)
        {
            if (accepted)
            {
                _delivered = Math.Max(_delivered, commitIndex);
                Failures = 0;
                return;
            }

            // A drain tries its flush once: a refused one leaves the quiet period to the backoff, which a draining sender never waits out.
            _flushed = false;
            Failures = Math.Min(Failures + 1, MaxBackoffDoublings + 1);
            _lastCommit = _owner.TimeProvider.GetTimestamp();
        }

        /// <summary>Records a commit index to deliver and restarts the quiet period; a closed or draining sender records nothing.</summary>
        /// <param name="leaderCommitIndex">The commit index to deliver.</param>
        internal void Schedule(ulong leaderCommitIndex)
        {
            if (_owner._closed || _owner.Draining || leaderCommitIndex <= _delivered)
                return;

            _target = Math.Max(_target, leaderCommitIndex);
            _lastCommit = _owner.TimeProvider.GetTimestamp();
            Arm(CommitIndexDebounce);
        }

        internal void StopTimer()
        {
            _timer?.Dispose();
            _timer = null;
            Armed = false;
        }

        /// <summary>Ends the quiet period for a drain and returns the empty append to send now when the slot is idle.</summary>
        /// <param name="heartbeat">The empty append carrying the target, when one is to be sent.</param>
        /// <returns><see langword="true" /> when an index is undelivered and the slot is idle.</returns>
        internal bool TryFlush(out FollowerBatch heartbeat)
        {
            StopTimer();
            heartbeat = default;
            if (!Pending)
                return false;

            _flushed = true;
            if (_owner.IsBusy)
                return false;

            heartbeat = Heartbeat();
            return true;
        }

        /// <summary>For a send loop out of work: returns the empty append to send when an undelivered index waited out the quiet period, otherwise re-arms the timer.</summary>
        /// <param name="heartbeat">The empty append carrying the target, when one is due.</param>
        /// <returns><see langword="true" /> when the empty append is due now.</returns>
        internal bool TryTakeDue(out FollowerBatch heartbeat)
        {
            heartbeat = default;
            if (_owner._leaseDone != null || _owner._closed || !Pending)
                return false;

            var remaining = Remaining();
            if (remaining > TimeSpan.Zero)
            {
                Arm(remaining);
                return false;
            }

            heartbeat = Heartbeat();
            return true;
        }

        /// <summary>Arms the one-shot debounce timer unless it is armed or the sender is closed or draining.</summary>
        /// <param name="delay">The time until the timer fires.</param>
        private void Arm(TimeSpan delay)
        {
            if (Armed || _owner._closed || _owner.Draining)
                return;

            if (_timer == null)
            {
                // The timer would capture the ambient scope of whoever commits first, and every firing would run in it.
                if (ExecutionContext.IsFlowSuppressed())
                {
                    _timer = _owner.TimeProvider.CreateTimer(static state => (state as CommitIndexDelivery)?.OnTimer(), this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                }
                else
                {
                    using (ExecutionContext.SuppressFlow())
                        _timer = _owner.TimeProvider.CreateTimer(static state => (state as CommitIndexDelivery)?.OnTimer(), this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                }
            }

            Armed = true;
            _ = _timer.Change(delay, Timeout.InfiniteTimeSpan);
        }

        /// <summary>Builds the empty append that carries the commit index still to deliver.</summary>
        /// <returns>The heartbeat.</returns>
        private FollowerBatch Heartbeat() =>
            new([], _owner._header.LeaderNodeId, _owner._header.Term, _owner._lastEnqueuedIndex, _owner._lastEnqueuedTerm, _target);

        private void OnTimer()
        {
            TaskCompletionSource started;
            FollowerBatch heartbeat;
            lock (_owner._sync)
            {
                Armed = false;
                if (_owner._closed || _owner.Draining || !Pending)
                    return;

                var remaining = Remaining();
                if (remaining > TimeSpan.Zero)
                {
                    Arm(remaining);
                    return;
                }

                // A busy sender or a catch-up lease hands over: the loop that runs out of work, and the end of the lease, send it.
                if (_owner.IsBusy)
                    return;

                heartbeat = Heartbeat();
                started = _owner.ClaimLoop();
            }

            _owner.StartLoop(started, heartbeat);
        }

        /// <summary>Gets the time left of the quiet period since the latest commit; after failed tries the quiet period doubles up to the cap.</summary>
        /// <returns>The time left, or zero or less once the quiet period passed or a drain ended it.</returns>
        private TimeSpan Remaining() => _flushed ? TimeSpan.Zero : RetryDelay() - _owner.TimeProvider.GetElapsedTime(_lastCommit);

        private TimeSpan RetryDelay() => Failures == 0 ? CommitIndexDebounce : TimeSpan.FromTicks(Math.Min(CommitIndexDebounce.Ticks << (Failures - 1), CommitIndexRetryCap.Ticks));
    }

    /// <summary>The append requests of the sender: the timeout and closing bounds of a request and what follows its answer.</summary>
    /// <remarks>Reads and changes the sender state under the sender lock only.</remarks>
    [Immutable]
    private sealed class FollowerRequests
    {
        private readonly TimeSpan _appendTimeout;
        private readonly ReplicaFollowerSender _owner;

        internal FollowerRequests(ReplicaFollowerSender owner, TimeSpan appendTimeout)
        {
            _owner = owner;
            _appendTimeout = appendTimeout;
        }

        /// <summary>Sends one request and completes the entries it carried with the follower's answer.</summary>
        /// <param name="batch">The entries of the request, in log order.</param>
        /// <returns>A task that faults when the request fails or times out; the caller fails the entries then.</returns>
        internal async Task SendBatchAsync(List<PendingAppend> batch)
        {
            var request = Backlog.ToRequest(batch, _owner._header.LeaderNodeId);
            var accepted = false;
            try
            {
                var result = await AppendAsync(request, CancellationToken.None).ConfigureAwait(false);
                accepted = result.Success;
                Backlog.Complete(batch, in result, _owner.NodeId);

                // After the entries are answered: an observer that throws faults only this request task, never an acknowledged entry.
                _owner.ReplyObserver?.Invoke(_owner.ReplicaIndex, result);
            }
            finally
            {
                lock (_owner._sync)
                    _owner._commit.Note(request.LeaderCommitIndex, accepted);
            }
        }

        /// <summary>Sends one empty append; a failure is the caller's to ignore.</summary>
        /// <param name="heartbeat">The empty append.</param>
        /// <returns>A task that faults when the request fails or times out.</returns>
        internal async Task SendHeartbeatAsync(FollowerBatch heartbeat)
        {
            var accepted = false;
            try
            {
                var result = await AppendAsync(heartbeat, CancellationToken.None).ConfigureAwait(false);
                accepted = result.Success;
                _owner.ReplyObserver?.Invoke(_owner.ReplicaIndex, result);
            }
            finally
            {
                lock (_owner._sync)
                    _owner._commit.Note(heartbeat.LeaderCommitIndex, accepted);
            }
        }

        /// <summary>Sends one catch-up request and releases the catch-up request slot when it ends.</summary>
        /// <param name="batch">The request.</param>
        /// <param name="done">The completion source of the catch-up request slot.</param>
        /// <param name="cancellationToken">Cancellation token of the caller.</param>
        /// <returns>The follower's answer.</returns>
        internal async Task<FollowerLogAppendResult> SendCatchUpCoreAsync(FollowerBatch batch, TaskCompletionSource done, CancellationToken cancellationToken)
        {
            try
            {
                var result = await AppendAsync(batch, cancellationToken).ConfigureAwait(false);

                // A follower under catch-up gets no heartbeat: its catch-up replies are its contact with the leader. An accepted one also delivered
                // the commit index it carried and ends the backoff of the refusals before it.
                if (result.Success)
                {
                    lock (_owner._sync)
                        _owner._commit.Note(batch.LeaderCommitIndex, true);
                }

                _owner.ReplyObserver?.Invoke(_owner.ReplicaIndex, result);
                return result;
            }
            finally
            {
                lock (_owner._sync)
                    _owner._catchUpRequest = null;

                _ = done.TrySetResult();
            }
        }

        /// <summary>Sends one append request bounded by the append timeout and canceled when the sender closes.</summary>
        /// <param name="request">The request.</param>
        /// <param name="cancellationToken">Cancellation token of the caller.</param>
        /// <returns>The follower's answer.</returns>
        private async Task<FollowerLogAppendResult> AppendAsync(FollowerBatch request, CancellationToken cancellationToken)
        {
            if (!cancellationToken.CanBeCanceled)
            {
                using var bound = new TimedCancellation(_appendTimeout, _owner.TimeProvider, _owner._closing.Token);
                return await _owner._rpc.AppendEntriesAsync(_owner.NodeId, _owner._header, request, bound.Token).ConfigureAwait(false);
            }

            using var timeout = new CancellationTokenSource(_appendTimeout, _owner.TimeProvider);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(_owner._closing.Token, timeout.Token, cancellationToken);
            return await _owner._rpc.AppendEntriesAsync(_owner.NodeId, _owner._header, request, linked.Token).ConfigureAwait(false);
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
                _ = result.Success
                    ? batch[i].Completion.TrySetResult(AcknowledgementOf(batch[i].Mutation))
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
