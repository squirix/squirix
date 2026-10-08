using System;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>The read-index rounds of one leader pipeline: a leader read is served only after a majority answered the leader in its term.</summary>
/// <remarks>
/// <para>
/// A round takes the commit index as its read index, sends a heartbeat to every idle follower, and completes once followers that, with the
/// leader, form a majority answered from their logs in the pipeline term to a request sent after the round started. A reply to a request
/// sent before the round started proves nothing about the time the read arrived, so it never counts, even when it arrives later; a busy
/// follower is counted by its next request, an append or the next heartbeat of the leader.
/// </para>
/// <para>
/// One round is in flight at a time. A reader that arrives while one is in flight waits for it to end and joins the next round, which
/// takes its read index after the reader arrived. A reply in a higher term fails every round for good: the pipeline term is over. No lease
/// and no local timer ever completes a round. Cancellation ends the wait of its reader only; the round goes on for the others.
/// </para>
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaReadIndexRound
{
    private readonly int _leaderSlot;
    private readonly int _majority;
    private readonly Lock _sync = new();
    private readonly ulong _term;
    private Round? _current;
    private Exception? _failure;
    private TaskCompletionSource? _idle;
    private long _started;

    /// <summary>Initializes a new instance of the <see cref="ReplicaReadIndexRound" /> class.</summary>
    /// <param name="term">The leader term of the pipeline.</param>
    /// <param name="replicaCount">The number of replicas of the group, the leader included; at most 64.</param>
    /// <param name="leaderSlot">The slot of the leader, which counts toward every majority without answering.</param>
    internal ReplicaReadIndexRound(ulong term, int replicaCount, int leaderSlot)
    {
        ArgumentOutOfRangeException.ThrowIfZero(term);
        ArgumentOutOfRangeException.ThrowIfLessThan(replicaCount, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(replicaCount, 64);
        ArgumentOutOfRangeException.ThrowIfNegative(leaderSlot);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(leaderSlot, replicaCount);
        _term = term;
        _majority = (replicaCount / 2) + 1;
        _leaderSlot = leaderSlot;
    }

    /// <summary>Waits until a round that started after this call confirms the leader term, and returns its read index.</summary>
    /// <typeparam name="TState">The type of the state handed to the callbacks.</typeparam>
    /// <param name="state">The state handed to the callbacks.</param>
    /// <param name="commitIndex">Reads the leader commit index; called under the round lock, so it must not block.</param>
    /// <param name="heartbeat">Sends a heartbeat to every idle follower; called outside the round lock.</param>
    /// <param name="cancellationToken">Cancellation token; it ends this wait only.</param>
    /// <returns>The read index: the commit index taken after this call started, confirmed by a majority in the leader term.</returns>
    /// <exception cref="Grpc.Core.RpcException">
    /// A follower answered in a higher term (no leader authority), or the pipeline closed (the read quorum is unconfirmed): Unavailable.
    /// </exception>
    internal async ValueTask<ulong> ConfirmAsync<TState>(TState state, Func<TState, ulong> commitIndex, Action<TState> heartbeat, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(commitIndex);
        ArgumentNullException.ThrowIfNull(heartbeat);
        var arrival = -1L;
        while (true)
        {
            var (round, idle, started) = Join(ref arrival, state, commitIndex);
            if (started)
                heartbeat(state);

            if (round != null)
                return await round.WaitAsync(cancellationToken).ConfigureAwait(false);

            await idle!.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Wraps the follower transport of the pipeline senders, so every reply counts toward the rounds started before its request.</summary>
    /// <param name="inner">The follower transport.</param>
    /// <param name="members">The group members in slot order.</param>
    /// <returns>The observing transport.</returns>
    internal IReplicaRpcGateway Observing(IReplicaRpcGateway inner, string[] members)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(members);
        return new ObservingGateway(this, inner, members);
    }

    /// <summary>Gets the ticket of a request about to be sent: the sequence number of the last round started.</summary>
    /// <returns>The ticket; a reply counts only toward rounds whose sequence number is at most the ticket of its request.</returns>
    internal long Ticket() => Volatile.Read(ref _started);

    /// <summary>Counts a follower reply toward the round in flight, or fails every round on a higher term.</summary>
    /// <param name="ticket">The ticket taken before the request was sent.</param>
    /// <param name="slot">The slot of the follower.</param>
    /// <param name="reply">The reply.</param>
    /// <remarks>
    /// Only an answer from the follower's log is a contact, as the election state counts it: accepted, a log mismatch, or a log not ready
    /// yet. Never throws.
    /// </remarks>
    internal void Observe(long ticket, int slot, in FollowerLogAppendResult reply)
    {
        if (reply.CurrentTerm > _term)
        {
            Fail(ServerOpContract.NoLeaderAuthority());
            return;
        }

        if (reply.CurrentTerm != _term || slot == _leaderSlot || slot is < 0 or >= 64 || !IsContact(in reply))
            return;

        Round confirmed;
        TaskCompletionSource? idle;
        lock (_sync)
        {
            if (_current is not { } round || ticket < round.Sequence)
                return;

            round.Acknowledged |= 1UL << slot;
            if (BitOperations.PopCount(round.Acknowledged) + 1 < _majority)
                return;

            confirmed = round;
            idle = _idle;
            _current = null;
            _idle = null;
        }

        _ = confirmed.Done.TrySetResult(confirmed.ReadIndex);
        _ = idle?.TrySetResult();
    }

    /// <summary>Fails the round in flight and every later one: the pipeline closed, so its reads are unconfirmed.</summary>
    internal void Close() => Fail(ServerOpContract.ReadQuorumUnconfirmed());

    private static bool IsContact(in FollowerLogAppendResult reply) =>
        reply.Success || string.Equals(reply.RefusalCode, FollowerLogRefusal.LogMismatch, StringComparison.Ordinal) ||
        string.Equals(reply.RefusalCode, FollowerLogRefusal.NotReady, StringComparison.Ordinal);

    /// <summary>Joins a round that started after the reader arrived, starts one when none is in flight, or waits for the one in flight.</summary>
    /// <typeparam name="TState">The type of the state handed to <paramref name="commitIndex" />.</typeparam>
    /// <param name="arrival">The sequence number of the last round started when the reader arrived; set on the first call.</param>
    /// <param name="state">The state handed to <paramref name="commitIndex" />.</param>
    /// <param name="commitIndex">Reads the leader commit index.</param>
    /// <returns>The round to wait for, or the end of the round in flight to wait for; and whether this call started the round.</returns>
    private (Task<ulong>? Round, Task? Idle, bool Started) Join<TState>(ref long arrival, TState state, Func<TState, ulong> commitIndex)
    {
        lock (_sync)
        {
            if (_failure != null)
                return (Task.FromException<ulong>(_failure), null, false);

            if (arrival < 0)
                arrival = _started;

            if (_current is { } current)
            {
                if (current.Sequence > arrival)
                    return (current.Done.Task, null, false);

                _idle ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                return (null, _idle.Task, false);
            }

            // The read index is taken before the sequence number is published, so a request that carries the new ticket is sent after it.
            var readIndex = commitIndex(state);
            var sequence = _started + 1;
            Volatile.Write(ref _started, sequence);
            if (_majority <= 1)
                return (Task.FromResult(readIndex), null, false);

            _current = new Round(sequence, readIndex);
            return (_current.Done.Task, null, true);
        }
    }

    private void Fail(Exception failure)
    {
        Round? current;
        TaskCompletionSource? idle;
        lock (_sync)
        {
            if (_failure != null)
                return;

            _failure = failure;
            current = _current;
            idle = _idle;
            _current = null;
            _idle = null;
        }

        if (current != null)
        {
            _ = current.Done.TrySetException(failure);

            // A round whose readers all gave up is never awaited; its failure is observed here.
            _ = current.Done.Task.Exception;
        }

        // The readers waiting for the round in flight retry and find the failure.
        _ = idle?.TrySetResult();
    }

    /// <summary>One read-index round: its sequence number, read index, the follower slots that answered, and its completion.</summary>
    private sealed class Round
    {
        internal Round(long sequence, ulong readIndex)
        {
            Sequence = sequence;
            ReadIndex = readIndex;
        }

        /// <summary>Gets or sets the bit mask of the follower slots that answered in the term to a request sent after the round started.</summary>
        /// <remarks>Read and written under the lock of the owning rounds.</remarks>
        internal ulong Acknowledged { get; set; }

        internal TaskCompletionSource<ulong> Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal ulong ReadIndex { get; }

        internal long Sequence { get; }
    }

    /// <summary>Follower transport that hands every reply, with the ticket taken before its request, to the rounds.</summary>
    private sealed class ObservingGateway : IReplicaRpcGateway
    {
        private readonly IReplicaRpcGateway _inner;
        private readonly string[] _members;
        private readonly ReplicaReadIndexRound _rounds;

        internal ObservingGateway(ReplicaReadIndexRound rounds, IReplicaRpcGateway inner, string[] members)
        {
            _rounds = rounds;
            _inner = inner;
            _members = members;
        }

        public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            var ticket = _rounds.Ticket();
            var result = await _inner.AppendEntriesAsync(nodeId, header, batch, cancellationToken).ConfigureAwait(false);
            var slot = Array.IndexOf(_members, nodeId);
            if (slot >= 0)
                _rounds.Observe(ticket, slot, in result);

            return result;
        }
    }
}
