using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The election driver of one replica group on this node: it campaigns when the leader goes silent, leads a won term, and steps down.</summary>
/// <remarks>
/// <para>
/// One caller runs <see cref="StepAsync" /> at a time, after <see cref="NextDelay" /> or when the group state wakes it; the driver is
/// the only writer of the role, term, and authority of the group state, and every round it starts ends inside the step, with its
/// unanswered calls canceled and awaited, so no late reply is ever counted. Terms are made durable in the group log before the driver
/// acts on them: a pre-vote changes nothing, a candidate persists its term and its own vote through the same vote path a peer's request
/// takes, and a higher term seen anywhere is persisted before the driver follows it.
/// </para>
/// <para>
/// A follower campaigns once no leader contact or granted vote happened for the election timeout plus a seeded jitter: a pre-vote round
/// first, then the vote round in the next term. A won term goes to the leadership, which commits a leader-term entry; authority is
/// granted only once that entry is committed by a majority. A leader heartbeats its followers, retries the promotion until it is
/// authorized, and steps down on a higher term or once a majority stayed silent for an election timeout, revoking its authority before
/// it retires. A leader that stays without authority past its deadline steps down the same way and, once its retirement completes,
/// does not campaign for a little longer than the longest normal timer, so another member can take the group; see
/// <see cref="AuthorityDeadline" />. A leader with authority that applies no committed entry for the apply timeout, as with a full node
/// journal, does the same: it reads the apply progress from the leadership on every tick, without the commit gate that a blocked apply
/// holds, and it reports the timeout and revokes its authority before the retirement, which waits for that gate, starts in a later step.
/// A leader with authority whose appends to its own group log keep failing or hang does the same after the append timeout, but only while
/// the followers without it form a majority that answered lately: with no such majority nobody could take the group, so the leader keeps
/// it and serves its reads. It reads the append progress from the leadership without any gate, as it does the apply progress.
/// A static leader, which no election hands the group to, has no such deadline, and nobody else can take its group.
/// </para>
/// <para>
/// The provisional term one of the group this node owns needs no election: only the owner may lead it, so a complete log that never
/// left it is led at term one at once; a copy that is not complete neither leads nor campaigns nor votes. The leadership reports a
/// failure to promote or retire as <see langword="false" />; anything it throws faults the step.
/// </para>
/// </remarks>
internal sealed class ReplicaGroupElection
{
    private readonly AuthorityDeadline _deadline;
    private readonly ReplicaRpcHeader _header;
    private readonly ElectionJitter _jitter;
    private readonly IReplicaLeadership _leadership;
    private readonly IFollowerLog _log;
    private readonly int _replicaCount;
    private readonly ReplicaVoteRound _round;
    private readonly int _selfSlot;
    private readonly ReplicaGroupState _state;
    private long _armedAt;
    private bool _retirePending;
    private bool _started;
    private TimeSpan _timeout;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupElection" /> class.</summary>
    /// <param name="state">The election state of the group.</param>
    /// <param name="log">The group log, which holds the term and the vote durably.</param>
    /// <param name="votes">The vote RPCs to the other members.</param>
    /// <param name="leadership">The leader side the driver hands a won term to.</param>
    /// <param name="members">The members of the group in slot order, this node among them.</param>
    /// <param name="header">The envelope of the vote RPCs: group, topology, and this node as sender; its term is replaced per round.</param>
    /// <exception cref="ArgumentException">This node is not a member of the group, or the state counts another number of replicas.</exception>
    internal ReplicaGroupElection(
        ReplicaGroupState state,
        IFollowerLog log,
        IReplicaVoteGateway votes,
        IReplicaLeadership leadership,
        string[] members,
        in ReplicaRpcHeader header)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(votes);
        ArgumentNullException.ThrowIfNull(leadership);
        ArgumentNullException.ThrowIfNull(members);
        ArgumentException.ThrowIfNullOrWhiteSpace(header.GroupId);
        ArgumentException.ThrowIfNullOrWhiteSpace(header.SenderNodeId);
        if (members.Length != state.ReplicaCount)
            throw new ArgumentException("The members do not match the replica count of the group state.", nameof(members));

        _selfSlot = Array.IndexOf(members, header.SenderNodeId);
        if (_selfSlot < 0)
            throw new ArgumentException($"Node '{header.SenderNodeId}' is not a member of replica group '{header.GroupId}'.", nameof(members));

        _state = state;
        _log = log;
        _leadership = leadership;
        _replicaCount = members.Length;
        _round = new ReplicaVoteRound(votes, [.. members], _selfSlot, state);
        _header = header with { Term = 0, LeaderNodeId = string.Empty };
        _jitter = new ElectionJitter(state.Options.JitterSeed, header.GroupId);
        _deadline = new AuthorityDeadline(state.Clock, state.Options);
        Arm();
    }

    /// <summary>Why the leader of a term leaves it.</summary>
    private enum LeaderExit
    {
        /// <summary>It stays.</summary>
        None = 0,

        /// <summary>A higher term was observed, or no majority of followers answered within the election timeout.</summary>
        Deposed = 1,

        /// <summary>It got no authority within its deadline.</summary>
        AuthorityTimedOut = 2,

        /// <summary>It has authority but applied no committed entry within the apply deadline.</summary>
        ApplyTimedOut = 3,

        /// <summary>It has authority but its appends to its own group log failed within the append deadline, and the followers can elect another leader.</summary>
        AppendTimedOut = 4,
    }

    /// <summary>Gets the replica group identifier.</summary>
    internal string GroupId => _header.GroupId;

    /// <summary>Gets how long the caller waits before the next step unless the group state wakes it first.</summary>
    /// <returns>The heartbeat interval while leading, retiring, or not started; otherwise the time left until this node may campaign.</returns>
    internal TimeSpan NextDelay() =>
        _retirePending || _state.Role == ReplicaGroupRole.Leader || !_started ? _state.Options.HeartbeatInterval : TimeLeft();

    /// <summary>Runs one step of the driver.</summary>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the step by throwing.</param>
    /// <returns>What the step did.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    internal async Task<ElectionOutcome> StepAsync(CancellationToken cancellationToken)
    {
        if (!_started)
            return await StartAsync(cancellationToken).ConfigureAwait(false);
        if (_state.Role == ReplicaGroupRole.Leader)
            return await LeadStepAsync(cancellationToken).ConfigureAwait(false);
        if (_retirePending)
            return await RetireAsync(_state.Term, cancellationToken).ConfigureAwait(false);

        // The log may have adopted a newer term on a follower path (an append or a vote of another node): the state follows it, so
        // later contacts in that term are not mistaken for a higher term that wakes the driver.
        var status = await _log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        if (status.CurrentTerm > _state.Term)
            _state.BecomeFollower(status.CurrentTerm, false);

        return true switch
        {
            _ when _state.HighestObservedTerm > status.CurrentTerm => await FollowAsync(_state.HighestObservedTerm, cancellationToken).ConfigureAwait(false),
            _ when TimeLeft() > TimeSpan.Zero => new ElectionOutcome(ElectionEvent.None, status.CurrentTerm),
            _ => await CampaignAsync(status, cancellationToken).ConfigureAwait(false),
        };
    }

    /// <summary>Stops driving the group: it stays a follower without authority, and votes for the own group are refused again.</summary>
    internal void Stop() => _state.SetElectionDriven(false);

    /// <summary>Restarts the election timeout with a fresh jitter.</summary>
    private void Arm()
    {
        _armedAt = _state.Clock.GetTimestamp();
        _timeout = _state.Options.ElectionTimeout + _jitter.Next(_state.Options.MaxJitter);
    }

    /// <summary>Gets the timestamp the election timeout runs from: the later of the arming and the last leader contact or granted vote.</summary>
    /// <returns>The timestamp.</returns>
    private long Baseline() => _state.ElectionResetTimestamp() is { } reset && reset > _armedAt ? reset : _armedAt;

    /// <summary>Campaigns for the next term once the election timeout fired: a pre-vote round, then the vote round.</summary>
    /// <param name="status">The log status read at the start of the step.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome of the campaign.</returns>
    private async Task<ElectionOutcome> CampaignAsync(FollowerLogStatus status, CancellationToken cancellationToken)
    {
        _state.BecomeFollower(status.CurrentTerm, true);
        var verdict = FailoverActivationGate.CheckElection(
            _replicaCount,
            true,
            true,
            status.MemberState == GroupMemberState.Complete,
            status.Readiness == FollowerLogReadiness.Ready,
            status.CurrentTerm,
            _state.HighestObservedTerm);
        if (!verdict.Eligible)
        {
            Arm();
            return new ElectionOutcome(ElectionEvent.Denied, status.CurrentTerm, verdict.Denial);
        }

        // The pre-vote changes no term anywhere: a node that cannot win, or that a live leader still serves, gives up here. Term one
        // belongs to the owner of the group alone, so the first election is for term two.
        var term = Math.Max(status.CurrentTerm + 1, 2UL);
        _state.BecomePreCandidate();
        var (granted, highest) = await _round
            .RunAsync(true, _header with { Term = term }, status.CurrentTerm, (status.LastLogIndex, status.LastLogTerm), cancellationToken)
            .ConfigureAwait(false);
        return true switch
        {
            _ when highest > status.CurrentTerm => await FollowAsync(highest, cancellationToken).ConfigureAwait(false),
            _ when IsMajority(granted) => await VoteAsync(term, cancellationToken).ConfigureAwait(false),
            _ => Lose(ElectionEvent.PreVoteLost, status.CurrentTerm),
        };
    }

    /// <summary>Makes a higher term durable and follows it, re-arming the election with a fresh jitter.</summary>
    /// <param name="term">The higher term.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome: the term followed, or the term that could not be made durable.</returns>
    /// <remarks>
    /// Re-arming keeps a follower of a new term from campaigning at once, and a log that cannot make the term durable from being retried
    /// in a busy loop: the next attempt waits for the election timeout.
    /// </remarks>
    private async Task<ElectionOutcome> FollowAsync(ulong term, CancellationToken cancellationToken)
    {
        var durable = await PersistTermAsync(term, cancellationToken).ConfigureAwait(false);
        _state.BecomeFollower(durable == 0 ? _state.Term : durable, false);
        Arm();
        return durable == 0 ? new ElectionOutcome(ElectionEvent.TermNotDurable, term) : new ElectionOutcome(ElectionEvent.TermObserved, durable);
    }

    private bool IsMajority(int granted) => granted * 2 > _replicaCount;

    /// <summary>Leads a won term: the leadership commits its leader-term entry, and authority follows once it is committed.</summary>
    /// <param name="term">The won term.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see cref="ElectionEvent.Authorized" /> when the entry is already committed; otherwise <see cref="ElectionEvent.Elected" />.</returns>
    private async Task<ElectionOutcome> LeadAsync(ulong term, CancellationToken cancellationToken)
    {
        // A driver stopped meanwhile leaves the group a follower: nothing is promoted.
        if (!_state.BecomeLeader(term))
            return new ElectionOutcome(ElectionEvent.None, term);

        _deadline.Begin();
        return await PromoteAsync(term, cancellationToken).ConfigureAwait(false)
            ? new ElectionOutcome(ElectionEvent.Authorized, term)
            : new ElectionOutcome(ElectionEvent.Elected, term);
    }

    /// <summary>One leader tick: leave on a higher term, a silent majority, or a missed authority, apply, or append deadline, else heartbeat and promote.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome of the tick.</returns>
    private async Task<ElectionOutcome> LeadStepAsync(CancellationToken cancellationToken)
    {
        var term = _state.Term;
        var exit = LeaderVerdict(term);
        if (exit != LeaderExit.None)
            return await LeaveAsync(term, exit, cancellationToken).ConfigureAwait(false);

        await _leadership.HeartbeatAsync(GroupId, cancellationToken).ConfigureAwait(false);
        return true switch
        {
            _ when _state.HasAuthority => new ElectionOutcome(ElectionEvent.None, term),
            _ when await PromoteAsync(term, cancellationToken).ConfigureAwait(false) => new ElectionOutcome(ElectionEvent.Authorized, term),
            _ => new ElectionOutcome(ElectionEvent.PromotionPending, term),
        };
    }

    /// <summary>Decides whether the leader of a term must leave it.</summary>
    /// <param name="term">The term led.</param>
    /// <returns>
    /// <see cref="LeaderExit.Deposed" /> on a higher term or a silent majority; otherwise <see cref="LeaderExit.AuthorityTimedOut" />
    /// when the leader got no authority within its deadline; otherwise <see cref="LeaderExit.ApplyTimedOut" /> when the leader with authority
    /// applied no committed entry within its apply deadline, or otherwise <see cref="LeaderExit.AppendTimedOut" /> when the leader with authority
    /// failed to append to its own log within its append deadline while the followers form a majority; both checked after the first;
    /// otherwise <see cref="LeaderExit.None" />.
    /// </returns>
    /// <remarks>The apply and append progress are read only by a leader with authority that no higher term or silent majority deposes.</remarks>
    private LeaderExit LeaderVerdict(ulong term)
    {
        var deposed = _state.HighestObservedTerm > term || !_state.HasQuorumContact(_selfSlot, _state.Options.ElectionTimeout);

        // The apply and append windows are judged only in the tick that observed the progress: a higher term seen meanwhile revokes the
        // authority, and the next tick then deposes the leader instead of timing it out.
        var observed = !deposed && _state.HasAuthority;
        if (observed)
        {
            _deadline.ObserveApply(_leadership.ApplyProgress(GroupId));
            _deadline.ObserveAppend(_leadership.AppendProgress(GroupId));
        }

        return true switch
        {
            _ when deposed => LeaderExit.Deposed,
            _ when _deadline.IsOverdue(_state.TransferProgress.IsRecent(_state.Options.ElectionTimeout)) => LeaderExit.AuthorityTimedOut,
            _ when observed && _deadline.IsApplyOverdue() => LeaderExit.ApplyTimedOut,
            _ when observed && _deadline.IsAppendOverdue() && _state.ElectableContacts.HasMajority(_selfSlot, _state.Options.ElectionTimeout) => LeaderExit.AppendTimedOut,
            _ => LeaderExit.None,
        };
    }

    /// <summary>Leaves the led term; a leader that missed its authority, apply, or append deadline then does not campaign for a while.</summary>
    /// <param name="term">The term led.</param>
    /// <param name="exit">Why the leader leaves.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// The outcome of the step-down; for a missed deadline it also carries how long the leader waited, and a missed apply or append deadline
    /// reports <see cref="ElectionEvent.ApplyTimedOut" /> or <see cref="ElectionEvent.AppendTimedOut" />.
    /// </returns>
    /// <remarks>
    /// The authority is revoked before the retirement, as for every step-down. A missed apply or append deadline returns at once, with the
    /// retirement left to the next step: the retirement waits for the commit gate, which a hung apply or append may hold, and the report
    /// must not wait for it.
    /// The retirement stays pending while a committed entry is unapplied and keeps the node from campaigning until it completes; the group
    /// has no heartbeat meanwhile, so another member takes it.
    /// </remarks>
    private async Task<ElectionOutcome> LeaveAsync(ulong term, LeaderExit exit, CancellationToken cancellationToken)
    {
        if (exit is not (LeaderExit.AuthorityTimedOut or LeaderExit.ApplyTimedOut or LeaderExit.AppendTimedOut))
            return await StepDownAsync(term, true, cancellationToken).ConfigureAwait(false);

        // The abstain starts when the retirement inside the step-down completes, possibly in a later step.
        if (exit is LeaderExit.ApplyTimedOut or LeaderExit.AppendTimedOut)
        {
            var stalled = exit == LeaderExit.ApplyTimedOut ? _deadline.GiveUpApply() : _deadline.GiveUpAppend();
            _ = await StepDownAsync(term, false, cancellationToken).ConfigureAwait(false);
            return new ElectionOutcome(exit == LeaderExit.ApplyTimedOut ? ElectionEvent.ApplyTimedOut : ElectionEvent.AppendTimedOut, term, FailoverDenial.None, stalled);
        }

        var waited = _deadline.GiveUp();
        _ = await StepDownAsync(term, true, cancellationToken).ConfigureAwait(false);
        return new ElectionOutcome(ElectionEvent.SteppedDown, term, FailoverDenial.None, waited);
    }

    private ElectionOutcome Lose(ElectionEvent lost, ulong term)
    {
        _state.BecomeFollower(term, false);
        Arm();
        return new ElectionOutcome(lost, term);
    }

    /// <summary>Makes a term durable in the group log.</summary>
    /// <param name="term">The term.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The durable term, at least <paramref name="term" />; zero when the log could not make it durable.</returns>
    /// <remarks>A log that is not ready reports its old term instead of adopting a higher one, which counts as a failure here.</remarks>
    private async Task<ulong> PersistTermAsync(ulong term, CancellationToken cancellationToken)
    {
        try
        {
            var durable = await _log.ObserveTermAsync(term, cancellationToken).ConfigureAwait(false);
            return durable >= term ? durable : 0UL;
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            // The log failed its readiness with the write: nothing acts on the term, and the step is retried.
            return 0UL;
        }
    }

    /// <summary>Promotes the led term once and grants authority when its leader-term entry is committed.</summary>
    /// <param name="term">The led term.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true" /> when this node now has authority in <paramref name="term" />.</returns>
    /// <remarks>
    /// A promotion that held the driver for a heartbeat interval or more, such as a start whose probes waited for followers that gave no
    /// answer from their logs, sent no heartbeat meanwhile: without authority, the quorum check then restarts its grace, so the leader is
    /// not deposed for followers it could not ask. A quick promotion that stays pending restarts nothing, so a leader no majority answers
    /// still steps down.
    /// </remarks>
    private async Task<bool> PromoteAsync(ulong term, CancellationToken cancellationToken)
    {
        var started = _state.Clock.GetTimestamp();
        if (await _leadership.PromoteAsync(GroupId, term, cancellationToken).ConfigureAwait(false) && _state.GrantAuthority(term))
        {
            _deadline.Granted();
            return true;
        }

        _deadline.AttemptFailed();
        if (_state.Clock.GetElapsedTime(started) >= _state.Options.HeartbeatInterval)
            _state.RestartQuorumGrace(term);

        return false;
    }

    private async Task<ElectionOutcome> RetireAsync(ulong term, CancellationToken cancellationToken)
    {
        var retired = await _leadership.RetireAsync(GroupId, cancellationToken).ConfigureAwait(false);
        _retirePending = !retired;
        _deadline.RetireAnswered(retired);
        return new ElectionOutcome(retired ? ElectionEvent.SteppedDown : ElectionEvent.RetirePending, term);
    }

    /// <summary>Starts driving the group: the own group still at the provisional term is led at once, every other group is followed.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome of the start.</returns>
    private async Task<ElectionOutcome> StartAsync(CancellationToken cancellationToken)
    {
        // Driven first, so a vote that raises the own group's term from here on is answered by the log and seen below.
        _state.SetElectionDriven(true);
        var status = await _log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        var own = string.Equals(GroupId, _header.SenderNodeId, StringComparison.Ordinal);

        // The own group decides its provisional term only from a ready, complete log; until then the start is retried, not skipped.
        if (own && ElectionHold.OnStart(in status) is { } held)
            return held;

        if (!own || status.CurrentTerm > 1)
        {
            _started = true;
            _state.BecomeFollower(status.CurrentTerm, false);
            Arm();
            return new ElectionOutcome(ElectionEvent.None, status.CurrentTerm);
        }

        // Term one belongs to the owner alone: no vote is ever granted for it, so leading it needs none. A log that moved past it
        // meanwhile reports the higher term, and the group is then followed like any other.
        var durable = await PersistTermAsync(1UL, cancellationToken).ConfigureAwait(false);
        if (durable == 0)
            return new ElectionOutcome(ElectionEvent.TermNotDurable, 1UL);

        _started = true;
        if (durable == 1)
            return await LeadAsync(1UL, cancellationToken).ConfigureAwait(false);

        _state.BecomeFollower(durable, false);
        Arm();
        return new ElectionOutcome(ElectionEvent.TermObserved, durable);
    }

    /// <summary>Steps down from a term: authority is revoked first, a higher term is made durable, then the group is retired.</summary>
    /// <param name="term">The term led.</param>
    /// <param name="retireNow">Whether to retire the group in this step; otherwise the next step does, as it waits for the commit gate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome of the step-down; <see cref="ElectionEvent.RetirePending" /> when the retirement is left to the next step.</returns>
    private async Task<ElectionOutcome> StepDownAsync(ulong term, bool retireNow, CancellationToken cancellationToken)
    {
        _state.BecomeFollower(term, false);
        _retirePending = true;
        Arm();
        var observed = _state.HighestObservedTerm;
        if (observed > term && await PersistTermAsync(observed, cancellationToken).ConfigureAwait(false) is var durable and > 0UL)
            _state.BecomeFollower(durable, false);

        return retireNow
            ? await RetireAsync(_state.Term, cancellationToken).ConfigureAwait(false)
            : new ElectionOutcome(ElectionEvent.RetirePending, _state.Term);
    }

    /// <summary>Gets the time until this node may campaign: the later of the election timeout and the end of the abstain after a given-up leadership.</summary>
    /// <returns>The time left; zero once this node may campaign.</returns>
    private TimeSpan TimeLeft()
    {
        var timer = _timeout - _state.Clock.GetElapsedTime(Baseline());
        var abstain = _deadline.AbstainLeft();
        return timer > abstain ? timer : abstain;
    }

    /// <summary>Runs the vote round in a term the pre-vote round found winnable.</summary>
    /// <param name="term">The candidate term.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome of the round.</returns>
    private async Task<ElectionOutcome> VoteAsync(ulong term, CancellationToken cancellationToken)
    {
        // The log may have moved during the pre-vote: the ballot carries its last entry now, and a term reached meanwhile ends the round.
        var current = await _log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        if (current.BlocksVote(term))
            return Lose(ElectionEvent.VoteLost, current.CurrentTerm);

        // The own vote takes the path a peer's request takes: the term and the vote are durable before they count.
        FollowerLogVoteResult self;
        try
        {
            self = await _log.RequestVoteAsync(new ElectionVoteRequest(_header.SenderNodeId, term, current.LastLogIndex, current.LastLogTerm), cancellationToken)
                             .ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            return Lose(ElectionEvent.TermNotDurable, current.CurrentTerm);
        }

        if (!await ElectionHold.SelfVoteStandsAsync(_log, self, term, cancellationToken).ConfigureAwait(false))
        {
            _state.ObserveHigherTerm(self.CurrentTerm);
            return Lose(ElectionEvent.VoteLost, Math.Max(current.CurrentTerm, self.CurrentTerm));
        }

        _state.BecomeCandidate(term);
        var (granted, highest) = await _round
            .RunAsync(false, _header with { Term = term }, term, (current.LastLogIndex, current.LastLogTerm), cancellationToken)
            .ConfigureAwait(false);
        return true switch
        {
            _ when highest > term => await FollowAsync(highest, cancellationToken).ConfigureAwait(false),

            // A higher term posted while the round ran (a vote or an append from another node) wins over the grants counted here.
            _ when _state.HighestObservedTerm > term => await FollowAsync(_state.HighestObservedTerm, cancellationToken).ConfigureAwait(false),
            _ when IsMajority(granted) => await LeadAsync(term, cancellationToken).ConfigureAwait(false),
            _ => Lose(ElectionEvent.VoteLost, term),
        };
    }
}
