using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ElectionTestDoubles;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>The election driver under failing logs, racing terms, and voters that never answer.</summary>
[Immutable]
public sealed class ReplicaGroupElectionEdgeTests : ServerUnitTestBase
{
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);
    private static readonly string[] Three = ["n2", "n1", "n3"];

    /// <summary>A higher term the log cannot make durable is retried after a full election timeout, never in a busy loop.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedTermPersistRearms(CancellationToken cancellationToken)
    {
        var log = new IFollowerLogCreateExpectations();
        _ = log.Setups.GetStatusAsync(Arg.Any<CancellationToken>()).ReturnValue(new ValueTask<FollowerLogStatus>(Status("n2", FollowerLogReadiness.Ready)));
        _ = log.Setups.ObserveTermAsync(Arg.Any<ulong>(), Arg.Any<CancellationToken>()).ReturnValue(Task.FromResult(0UL));
        var state = new ReplicaGroupState(3, Options, new FakeTimeProvider());
        var election = Create(state, log.Instance(), "n2", Three);
        _ = await election.StepAsync(cancellationToken);
        state.ObserveHigherTerm(5UL);

        var outcome = await election.StepAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.TermNotDurable, 5UL));
        _ = await Assert.That(election.NextDelay()).IsEqualTo(Options.ElectionTimeout);
    }

    /// <summary>
    /// The own group waits for a ready log before it decides its provisional term, and a log that moved past term one meanwhile is
    /// followed instead of led.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnGroupStartWaitsForReadyLog(CancellationToken cancellationToken)
    {
        var reads = 0;
        var log = new IFollowerLogCreateExpectations();
        _ = log.Setups.GetStatusAsync(Arg.Any<CancellationToken>())
               .Callback(_ => new ValueTask<FollowerLogStatus>(Status("n1", Interlocked.Increment(ref reads) == 1 ? FollowerLogReadiness.Failed : FollowerLogReadiness.Ready)));
        _ = log.Setups.ObserveTermAsync(Arg.Any<ulong>(), Arg.Any<CancellationToken>()).ReturnValue(Task.FromResult(2UL));
        var state = new ReplicaGroupState(3, Options, new FakeTimeProvider());
        var leadership = new RecordingLeadership(state);
        var election = new ReplicaGroupElection(state, log.Instance(), new ScriptedVotes(Grant), leadership, ["n1", "n2", "n3"], Header("n1"));

        var waiting = await election.StepAsync(cancellationToken);
        var waitingDelay = election.NextDelay();
        var started = await election.StepAsync(cancellationToken);

        _ = await Assert.That((waiting.Event, waitingDelay)).IsEqualTo((ElectionEvent.None, Options.HeartbeatInterval));
        _ = await Assert.That(started).IsEqualTo(new ElectionOutcome(ElectionEvent.TermObserved, 2UL));
        _ = await Assert.That((state.Role, leadership.Calls)).IsEqualTo((ReplicaGroupRole.Follower, string.Empty));
    }

    /// <summary>A vote for another candidate in the proposed term, cast while the pre-vote ran, ends the round without leading.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteForOtherCandidateEndsRound(CancellationToken cancellationToken)
    {
        var votes = new RacingVotes();
        await using var scope = await OpenAsync("n2", 3, votes);
        votes.Log = scope.Log;
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);

        var outcome = await election.StepAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.VoteLost, 2UL));
        var status = await scope.Log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.VotedFor, scope.Leadership.Calls)).IsEqualTo((2UL, "n3", string.Empty));
    }

    /// <summary>A higher term posted while the vote round ran beats the grants: the candidate follows it and promotes nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherTermDuringRoundWins(CancellationToken cancellationToken)
    {
        ReplicaGroupState? state = null;
        var votes = new ScriptedVotes(call =>
        {
            if (!call.PreVote)
                state?.ObserveHigherTerm(5UL);

            return Grant(call);
        });
        await using var scope = await OpenAsync("n2", 3, votes);
        state = scope.State;
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);

        var outcome = await election.StepAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.TermObserved, 5UL));
        _ = await Assert.That((scope.State.Role, scope.Leadership.Calls)).IsEqualTo((ReplicaGroupRole.Follower, string.Empty));
    }

    /// <summary>A voter that never answers does not delay an election the others decide, in either round.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SilentVoterDoesNotDelayElection(CancellationToken cancellationToken)
    {
        var votes = new SilentVoter("n3", false);
        await using var scope = await OpenAsync("n2", 3, votes);
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);

        var outcome = await election.StepAsync(cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.Authorized, 2UL));
        _ = await Assert.That(votes.Canceled).IsEqualTo(2);
    }

    /// <summary>A silent voter the outcome depends on is given up on at the vote timeout.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NeededSilentVoterTimesOut(CancellationToken cancellationToken)
    {
        var votes = new SilentVoter("n3", true);
        await using var scope = await OpenAsync("n2", 3, votes);
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);

        var step = election.StepAsync(cancellationToken);
        var arrived = await votes.Arrivals.WaitAsync(HangGuard, cancellationToken);
        var pending = !step.IsCompleted;
        scope.Time.Advance(Options.VoteRpcTimeout);
        var outcome = await step.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That((arrived, pending)).IsEqualTo((true, true));
        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.PreVoteLost, 0UL));
    }

    /// <summary>
    /// A term the log adopted from a new leader is followed at the start of the next step, so a later contact in that term no longer wakes
    /// the driver.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NewerDurableTermIsFollowed(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        _ = await scope.Log.AppendAsync(new FollowerLogAppendRequest("n2", 3UL, 0UL, 0UL, 0UL, ReadOnlyMemory<FollowerLogEntry>.Empty), cancellationToken);
        scope.State.ObserveLeaderContact("n2", 3UL);
        _ = await scope.State.WaitAsync(TimeSpan.Zero, cancellationToken);

        var outcome = await election.StepAsync(cancellationToken);
        scope.State.ObserveLeaderContact("n2", 3UL);

        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.None, 3UL));
        _ = await Assert.That((scope.State.Term, scope.State.Role)).IsEqualTo((3UL, ReplicaGroupRole.Follower));
        _ = await Assert.That(await scope.State.WaitAsync(TimeSpan.Zero, cancellationToken)).IsFalse();
    }

    private static ReplicaGroupElection Create(ReplicaGroupState state, IFollowerLog log, string groupId, string[] members) =>
        new(state, log, new ScriptedVotes(Grant), new RecordingLeadership(state), members, Header(groupId));

    private static FollowerLogVoteResult? Grant(VoteCall call) => new(true, string.Empty, call.PreVote ? 0UL : call.Term);

    private static ReplicaRpcHeader Header(string groupId) => new(groupId, ReadOnlyMemory<byte>.Empty, 1UL, 0UL, string.Empty, "n1");

    private static FollowerLogStatus Status(string groupId, FollowerLogReadiness readiness) =>
        new(groupId, ReadOnlyMemory<byte>.Empty, 1UL, 0UL, string.Empty, 0UL, 0UL, 0UL, 0UL, readiness);

    /// <summary>Voters whose first pre-vote makes the candidate log vote for another candidate in the proposed term, then grant.</summary>
    [ThreadSafe]
    private sealed class RacingVotes : IReplicaVoteGateway
    {
        private int _raced;

        internal FollowerLog? Log { get; set; }

        public async Task<FollowerLogVoteResult> PreVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken)
        {
            if (Interlocked.Exchange(ref _raced, 1) == 0 && Log is { } log)
                _ = await log.RequestVoteAsync(new ElectionVoteRequest("n3", header.Term, 0UL, 0UL), CancellationToken.None);

            return new FollowerLogVoteResult(true, string.Empty, 0UL);
        }

        public Task<FollowerLogVoteResult> RequestVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken) =>
            Task.FromResult(new FollowerLogVoteResult(true, string.Empty, header.Term));
    }

    /// <summary>Voters of which one never answers until its call is canceled; every call to it is signaled and canceled calls are counted.</summary>
    [ThreadSafe]
    private sealed class SilentVoter : IReplicaVoteGateway
    {
        private readonly bool _refuseOthers;
        private readonly string _silent;
        private int _canceled;

        internal SilentVoter(string silent, bool refuseOthers)
        {
            _silent = silent;
            _refuseOthers = refuseOthers;
        }

        internal SemaphoreSlim Arrivals { get; } = new(0);

        internal int Canceled => Volatile.Read(ref _canceled);

        public Task<FollowerLogVoteResult> PreVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken) =>
            AnswerAsync(nodeId, 0UL, cancellationToken);

        public Task<FollowerLogVoteResult> RequestVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken) =>
            AnswerAsync(nodeId, header.Term, cancellationToken);

        private async Task<FollowerLogVoteResult> AnswerAsync(string nodeId, ulong term, CancellationToken cancellationToken)
        {
            if (!string.Equals(nodeId, _silent, StringComparison.Ordinal))
                return new FollowerLogVoteResult(!_refuseOthers, _refuseOthers ? "refused" : string.Empty, term);

            _ = Arrivals.Release();
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                _ = Interlocked.Increment(ref _canceled);
                throw;
            }

            throw new InvalidOperationException("A silent voter never answers.");
        }
    }
}
