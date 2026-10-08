using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// The read-index rounds of a leader pipeline in term 2 of a group of three, led from slot 0: a round completes once one follower answered
/// in the term to a request sent after the round started, and fails for good on a higher term or when the pipeline closes.
/// </summary>
public sealed class ReplicaReadIndexRoundTests : ServerUnitTestBase
{
    private const ulong Term = 2UL;

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>The round takes the commit index, sends one heartbeat, and completes with that index once a follower answers in the term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MajorityCompletesRound(CancellationToken cancellationToken)
    {
        var leader = new Leader(7UL);

        var confirming = leader.ConfirmAsync(cancellationToken);
        var ticket = await leader.NextBeatAsync(cancellationToken);
        var waited = !confirming.IsCompleted;
        leader.Rounds.Observe(ticket, 1, Accepted(Term));

        _ = await Assert.That((waited, await confirming.WaitAsync(HangGuard, TimeProvider.System, cancellationToken))).IsEqualTo((true, 7UL));
    }

    /// <summary>A reply to a request sent before the round started does not confirm it, even when it arrives later; a reply to a later request does.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreRoundReplyDoesNotCount(CancellationToken cancellationToken)
    {
        var leader = new Leader(4UL);
        var before = leader.Rounds.Ticket();

        var confirming = leader.ConfirmAsync(cancellationToken);
        var ticket = await leader.NextBeatAsync(cancellationToken);
        leader.Rounds.Observe(before, 1, Accepted(Term));
        leader.Rounds.Observe(before, 2, Accepted(Term));
        var heldBack = !confirming.IsCompleted;
        leader.Rounds.Observe(ticket, 2, Rejected(FollowerLogRefusal.LogMismatch));

        _ = await Assert.That((heldBack, await confirming.WaitAsync(HangGuard, TimeProvider.System, cancellationToken))).IsEqualTo((true, 4UL));
    }

    /// <summary>
    /// Through the observing transport, the reply to a request sent before the round started does not confirm it; the sender, busy when the
    /// round asked for a heartbeat, sends one as soon as its send loop ran out of work, and that reply does.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BusySenderBeatsOnceIdle(CancellationToken cancellationToken)
    {
        var gateway = new HeldGateway();
        var rounds = new ReplicaReadIndexRound(Term, 3, 0);
        var header = new ReplicaRpcHeader("n1", new byte[] { 9, 8, 7 }, 1, Term, "n1", "n1");
        var sender = new ReplicaFollowerSender(rounds.Observing(gateway, ["n1", "n2", "n3"]), "n2", in header, 0, 0, HangGuard);
        try
        {
            var idle = sender.TryEnqueueHeartbeat(0UL);
            var inFlight = await gateway.NextAsync(cancellationToken);
            var confirming = rounds.ConfirmAsync(sender, static _ => 1UL, static busy => _ = busy.TryEnqueueHeartbeat(1UL, true), cancellationToken).AsTask();
            _ = inFlight.TrySetResult(Accepted(Term));
            var deferred = await gateway.NextAsync(cancellationToken);
            var heldBack = !confirming.IsCompleted;
            _ = deferred.TrySetResult(Accepted(Term));

            _ = await Assert.That((idle, heldBack, await confirming.WaitAsync(HangGuard, TimeProvider.System, cancellationToken))).IsEqualTo((true, true, 1UL));
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A reply in a higher term fails the round in flight and every later one as stale-term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherTermFaultsRound(CancellationToken cancellationToken)
    {
        var leader = new Leader(3UL);

        var confirming = leader.ConfirmAsync(cancellationToken);
        var ticket = await leader.NextBeatAsync(cancellationToken);
        leader.Rounds.Observe(ticket, 1, Accepted(Term + 1));
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(confirming.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));
        var later = await NodeAsyncAssert.ThrowsAsync<RpcException>(leader.ConfirmAsync(cancellationToken));

        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.FailedPrecondition, "stale-term"));
        _ = await Assert.That((later.StatusCode, later.Status.Detail)).IsEqualTo((StatusCode.FailedPrecondition, "stale-term"));
    }

    /// <summary>A reader that arrives while a round is in flight does not share it: it starts the next round, at the commit index of its arrival.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LateReaderJoinsNextRound(CancellationToken cancellationToken)
    {
        var leader = new Leader(5UL);
        var first = leader.ConfirmAsync(cancellationToken);
        var firstTicket = await leader.NextBeatAsync(cancellationToken);
        leader.Commit = 6UL;

        var late = leader.ConfirmAsync(cancellationToken);
        leader.Rounds.Observe(firstTicket, 1, Accepted(Term));
        var firstIndex = await first.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        var lateTicket = await leader.NextBeatAsync(cancellationToken);
        var lateWaited = !late.IsCompleted;
        leader.Rounds.Observe(lateTicket, 2, Accepted(Term));

        _ = await Assert.That((firstIndex, lateWaited, await late.WaitAsync(HangGuard, TimeProvider.System, cancellationToken))).IsEqualTo((5UL, true, 6UL));
        _ = await Assert.That(lateTicket).IsGreaterThan(firstTicket);
    }

    /// <summary>Closing the pipeline fails the round in flight as an unconfirmed read quorum, and every later round as well.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CloseFaultsOpenRounds(CancellationToken cancellationToken)
    {
        var leader = new Leader(2UL);

        var confirming = leader.ConfirmAsync(cancellationToken);
        _ = await leader.NextBeatAsync(cancellationToken);
        leader.Rounds.Close();
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(confirming.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));
        var later = await NodeAsyncAssert.ThrowsAsync<RpcException>(leader.ConfirmAsync(cancellationToken));

        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.ReadQuorumUnconfirmedDetail));
        _ = await Assert.That(later.Status.Detail).IsEqualTo(ServerOpContract.ReadQuorumUnconfirmedDetail);
        _ = await Assert.That(later).IsNotSameReferenceAs(refused);
    }

    /// <summary>A reader that gives up leaves its round in flight: a reader waiting behind it still waits for that round, then gets its own.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelledReaderLeavesRound(CancellationToken cancellationToken)
    {
        var leader = new Leader(8UL);
        using var giveUp = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var cancelled = leader.ConfirmAsync(giveUp.Token);
        var firstTicket = await leader.NextBeatAsync(cancellationToken);
        var waiting = leader.ConfirmAsync(cancellationToken);

        await giveUp.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(cancelled.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));
        var heldBack = !waiting.IsCompleted;
        leader.Commit = 9UL;
        leader.Rounds.Observe(firstTicket, 1, Accepted(Term));
        var nextTicket = await leader.NextBeatAsync(cancellationToken);
        leader.Rounds.Observe(nextTicket, 1, Accepted(Term));

        _ = await Assert.That((heldBack, await waiting.WaitAsync(HangGuard, TimeProvider.System, cancellationToken))).IsEqualTo((true, 9UL));
    }

    private static FollowerLogAppendResult Accepted(ulong term) => new(true, string.Empty, term, 0UL);

    private static FollowerLogAppendResult Rejected(string refusal) => new(false, refusal, Term, 0UL);

    /// <summary>Follower transport that holds every request until the test answers it, handing the requests out in arrival order.</summary>
    private sealed class HeldGateway : IReplicaRpcGateway
    {
        private readonly Channel<TaskCompletionSource<FollowerLogAppendResult>> _arrivals = Channel.CreateUnbounded<TaskCompletionSource<FollowerLogAppendResult>>();
        private readonly ConcurrentQueue<TaskCompletionSource<FollowerLogAppendResult>> _held = new();

        public Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            var answer = new TaskCompletionSource<FollowerLogAppendResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            _held.Enqueue(answer);
            _ = _arrivals.Writer.TryWrite(answer);
            return answer.Task;
        }

        /// <summary>Waits for the next request.</summary>
        /// <param name="cancellationToken">The test cancellation token.</param>
        /// <returns>The answer of the request.</returns>
        internal Task<TaskCompletionSource<FollowerLogAppendResult>> NextAsync(CancellationToken cancellationToken) =>
            _arrivals.Reader.ReadAsync(cancellationToken).AsTask().WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        /// <summary>Answers every held request, so no send loop outlives the test.</summary>
        internal void ReleaseAll()
        {
            foreach (var answer in _held)
                _ = answer.TrySetResult(Accepted(Term));
        }
    }

    /// <summary>The leader side of the rounds: a commit index the test moves, and a heartbeat that reports the ticket of the requests it sends.</summary>
    private sealed class Leader
    {
        private readonly Channel<long> _beats = Channel.CreateUnbounded<long>();

        internal Leader(ulong commit)
        {
            Commit = commit;
        }

        internal ulong Commit { get; set; }

        internal ReplicaReadIndexRound Rounds { get; } = new(Term, 3, 0);

        internal Task<ulong> ConfirmAsync(CancellationToken cancellationToken) =>
            Rounds.ConfirmAsync(this, static leader => leader.Commit, static leader => _ = leader._beats.Writer.TryWrite(leader.Rounds.Ticket()), cancellationToken).AsTask();

        /// <summary>Waits for the next heartbeat a round sent and returns the ticket of its requests.</summary>
        /// <param name="cancellationToken">The test cancellation token.</param>
        /// <returns>The ticket.</returns>
        internal Task<long> NextBeatAsync(CancellationToken cancellationToken) =>
            _beats.Reader.ReadAsync(cancellationToken).AsTask().WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
    }
}
