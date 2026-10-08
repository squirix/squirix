using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Grpc.Core;
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

    /// <summary>A reply in a higher term fails the round in flight and every later one as the loss of leader authority.</summary>
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

        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.NoLeaderAuthorityDetail));
        _ = await Assert.That(later.StatusCode).IsEqualTo(StatusCode.Unavailable);
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
