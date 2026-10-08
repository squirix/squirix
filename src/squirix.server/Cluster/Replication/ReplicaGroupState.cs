using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The election state of this node in one replica group: its role, its term, its authority, and the contacts that keep it.</summary>
/// <remarks>
/// The election driver of the group is the only writer of the role, the term, and the authority; the replication RPC paths only post
/// what they saw (a leader contact, a higher term, a follower reply), and a higher term than the driver's wakes it. The term here mirrors
/// the durable term of the group log the driver acted on; the log stays the source of truth. While no driver runs for the group (automatic
/// failover off, or fewer than three replicas) the state stays a follower without authority and only records contacts.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaGroupState
{
    /// <summary>Marks a follower slot that never answered the current leader.</summary>
    private const long NoContact = long.MinValue;

    private readonly long[] _followerContact;
    private readonly Lock _sync = new();
    private readonly ReplicaApplySignal _wake = new();
    private bool _driven;
    private bool _hasAuthority;
    private ulong _highestObservedTerm;
    private string _knownLeader = string.Empty;
    private ulong _knownLeaderTerm;
    private long _lastElectionReset = NoContact;
    private long _lastLeaderContact = NoContact;
    private long _leaderSince;
    private ReplicaGroupRole _role;
    private ulong _term;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupState" /> class.</summary>
    /// <param name="replicaCount">The number of replicas of the group, this node included.</param>
    /// <param name="options">The election timing of the group.</param>
    /// <param name="clock">The time source of every contact and timeout; only its monotonic timestamp is read.</param>
    internal ReplicaGroupState(int replicaCount, ElectionTimerOptions options, TimeProvider clock)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(replicaCount, 1);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(clock);
        _followerContact = new long[replicaCount];
        _followerContact.AsSpan().Fill(NoContact);
        Options = options;
        Clock = clock;
    }

    /// <summary>Gets the time source of every contact and timeout.</summary>
    internal TimeProvider Clock { get; }

    /// <summary>Gets a value indicating whether this node is the leader of the group and its leader-term entry is committed.</summary>
    internal bool HasAuthority
    {
        get
        {
            lock (_sync)
                return _hasAuthority;
        }
    }

    /// <summary>Gets the highest term this node saw for the group, from its own log or from any peer.</summary>
    internal ulong HighestObservedTerm
    {
        get
        {
            lock (_sync)
                return _highestObservedTerm;
        }
    }

    /// <summary>Gets a value indicating whether an election driver runs for the group, so its leader term comes from an election.</summary>
    internal bool IsElectionDriven
    {
        get
        {
            lock (_sync)
                return _driven;
        }
    }

    /// <summary>Gets the election timing of the group.</summary>
    internal ElectionTimerOptions Options { get; }

    /// <summary>Gets the number of replicas of the group, this node included.</summary>
    internal int ReplicaCount => _followerContact.Length;

    /// <summary>Gets the election role of this node in the group.</summary>
    internal ReplicaGroupRole Role
    {
        get
        {
            lock (_sync)
                return _role;
        }
    }

    /// <summary>Gets the signal published whenever the authority or the known leader of the group may have changed.</summary>
    internal ReplicaRouteSignal RouteChanged { get; } = new();

    /// <summary>Gets the term the driver acts in: the term it follows, campaigns in, or leads.</summary>
    internal ulong Term
    {
        get
        {
            lock (_sync)
                return _term;
        }
    }

    /// <summary>Gets the monotonic timestamp from which the follower election timeout runs: the last leader contact or granted vote.</summary>
    /// <returns>The timestamp, or <see langword="null" /> when neither happened yet.</returns>
    internal long? ElectionResetTimestamp()
    {
        lock (_sync)
            return _lastElectionReset == NoContact ? null : _lastElectionReset;
    }

    /// <summary>Tells whether the group has a live leader as far as this node knows: itself with authority, or a recent contact.</summary>
    /// <param name="window">How recent the leader contact must be.</param>
    /// <returns><see langword="true" /> when a pre-vote must be refused, so a node cut off for a while cannot disrupt a working leader.</returns>
    internal bool HasRecentLeaderContact(TimeSpan window)
    {
        lock (_sync)
        {
            return (_role == ReplicaGroupRole.Leader && _hasAuthority) ||
                   (_lastLeaderContact != NoContact && Clock.GetElapsedTime(_lastLeaderContact) < window);
        }
    }

    /// <summary>Records a granted vote, which postpones this node's own election like a leader contact does.</summary>
    internal void ObserveGrantedVote()
    {
        lock (_sync)
            _lastElectionReset = Clock.GetTimestamp();
    }

    /// <summary>Records a term seen anywhere for the group; a term above the driver's wakes the driver, which steps down to it.</summary>
    /// <param name="term">The term seen.</param>
    internal void ObserveHigherTerm(ulong term)
    {
        bool wake;
        lock (_sync)
            wake = RaiseLocked(term);

        if (wake)
            _wake.Notify();
    }

    /// <summary>Records an append, commit, or snapshot this node accepted from a leader, which postpones its own election.</summary>
    /// <param name="leaderId">The leader that sent it, or <see langword="null" /> when the call does not name it.</param>
    /// <param name="term">The durable term of the log after the call, the term of the leader.</param>
    internal void ObserveLeaderContact(string? leaderId, ulong term)
    {
        bool wake;
        lock (_sync)
        {
            wake = RaiseLocked(term);
            if (_role != ReplicaGroupRole.Leader || term > _term)
            {
                var now = Clock.GetTimestamp();
                _lastLeaderContact = now;
                _lastElectionReset = now;
                if (!string.IsNullOrEmpty(leaderId) && term >= _knownLeaderTerm &&
                    (term != _knownLeaderTerm || !string.Equals(leaderId, _knownLeader, StringComparison.Ordinal)))
                {
                    _knownLeader = leaderId;
                    _knownLeaderTerm = term;
                    RouteChanged.Publish();
                }
            }
        }

        if (wake)
            _wake.Notify();
    }

    /// <summary>Records the reply of a follower to an append or heartbeat of this leader.</summary>
    /// <param name="replicaIndex">The slot of the follower.</param>
    /// <param name="replyTerm">The durable term of the follower; above the leader term it is a higher term, never a contact.</param>
    internal void RecordFollowerContact(int replicaIndex, ulong replyTerm)
    {
        bool wake;
        lock (_sync)
        {
            wake = RaiseLocked(replyTerm);
            if (!wake && _role == ReplicaGroupRole.Leader && replyTerm <= _term && replicaIndex >= 0 && replicaIndex < _followerContact.Length)
                _followerContact[replicaIndex] = Clock.GetTimestamp();
        }

        if (wake)
            _wake.Notify();
    }

    /// <summary>Tells whether this leader heard from enough followers lately to form a majority with itself.</summary>
    /// <param name="leaderReplicaIndex">The slot of this node, which counts for itself.</param>
    /// <param name="window">How recent a follower reply must be.</param>
    /// <returns>
    /// <see langword="true" /> while a majority answered within the window, and for one window after this node became leader, so a new
    /// leader is not deposed before its first heartbeats could be answered.
    /// </returns>
    internal bool HasQuorumContact(int leaderReplicaIndex, TimeSpan window)
    {
        lock (_sync)
        {
            if (Clock.GetElapsedTime(_leaderSince) < window)
                return true;

            var recent = 1;
            for (var i = 0; i < _followerContact.Length; i++)
            {
                if (i != leaderReplicaIndex && _followerContact[i] != NoContact && Clock.GetElapsedTime(_followerContact[i]) < window)
                    recent++;
            }

            return recent * 2 > _followerContact.Length;
        }
    }

    /// <summary>Reads, in one consistent view, what the replica status reports of this node in the group.</summary>
    /// <returns>
    /// The role; whether this node leads with authority; whether it is in contact with a majority as far as its role can tell (a leader:
    /// a majority answered within the election timeout, the quorum check of the driver; any other role: a leader contacted it within the
    /// election timeout); and the highest term it saw.
    /// </returns>
    internal (ReplicaGroupRole Role, bool HasAuthority, bool HasMajorityContact, ulong ObservedTerm) ObserveStatus()
    {
        lock (_sync)
        {
            // The leader's own slot never records a contact: only its followers answer it. The lock is reentrant.
            var contact = _role == ReplicaGroupRole.Leader
                ? HasQuorumContact(-1, Options.ElectionTimeout)
                : _lastLeaderContact != NoContact && Clock.GetElapsedTime(_lastLeaderContact) < Options.ElectionTimeout;
            return (_role, _hasAuthority, contact, _highestObservedTerm);
        }
    }

    /// <summary>Reads, in one consistent view, the authority of this node and the leader it last accepted contact from.</summary>
    /// <returns>
    /// A served view; its known leader is the last accepted contact while this node is a follower, and <see langword="default" /> in any
    /// other role, so a candidate or a leader names no other node. The state does not know this node's identifier, so authority does not
    /// name it either.
    /// </returns>
    internal GroupLeaderView ReadRoute()
    {
        lock (_sync)
        {
            var known = _role == ReplicaGroupRole.Follower && _knownLeader.Length != 0 ? new LeaderRoute(_knownLeader, _knownLeaderTerm) : default;
            return new GroupLeaderView(true, _hasAuthority, _role == ReplicaGroupRole.Leader && !_hasAuthority, _term, _highestObservedTerm, known);
        }
    }

    /// <summary>Waits until a higher term wakes the driver or <paramref name="delay" /> elapses.</summary>
    /// <param name="delay">The longest wait.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the wait by throwing.</param>
    /// <returns><see langword="true" /> when a wake-up was pending.</returns>
    internal Task<bool> WaitAsync(TimeSpan delay, CancellationToken cancellationToken) => _wake.WaitAsync(delay, Clock, cancellationToken);

    /// <summary>Starts a pre-vote round.</summary>
    internal void BecomePreCandidate()
    {
        lock (_sync)
        {
            SetRoleLocked(ReplicaGroupRole.PreCandidate);
            RouteChanged.Publish();
        }
    }

    /// <summary>Starts a vote round in a term the log already holds durably with this node's own vote.</summary>
    /// <param name="term">The candidate term.</param>
    internal void BecomeCandidate(ulong term)
    {
        lock (_sync)
        {
            SetRoleLocked(ReplicaGroupRole.Candidate);
            _term = term;
            _ = RaiseLocked(term);
            RouteChanged.Publish();
        }
    }

    /// <summary>Follows a term; authority is cleared before anything else changes.</summary>
    /// <param name="term">The durable term of the log.</param>
    /// <param name="forgetLeader">Whether the known leader is forgotten, as when the election timeout fired without its contact.</param>
    internal void BecomeFollower(ulong term, bool forgetLeader)
    {
        lock (_sync)
        {
            SetRoleLocked(ReplicaGroupRole.Follower);
            _term = term;
            _ = RaiseLocked(term);
            if (forgetLeader)
            {
                _knownLeader = string.Empty;
                _knownLeaderTerm = 0;
                _lastLeaderContact = NoContact;
            }

            RouteChanged.Publish();
        }
    }

    /// <summary>Leads a won term, without authority until its leader-term entry is committed.</summary>
    /// <param name="term">The won term.</param>
    /// <returns><see langword="false" /> when no driver runs any more, so the group stays a follower.</returns>
    internal bool BecomeLeader(ulong term)
    {
        lock (_sync)
        {
            if (!_driven)
            {
                // No driver runs any more: the role stays as it is, and only the authority is cleared.
                SetRoleLocked(_role);
                return false;
            }

            SetRoleLocked(ReplicaGroupRole.Leader);
            _term = term;
            _ = RaiseLocked(term);
            _leaderSince = Clock.GetTimestamp();
            _followerContact.AsSpan().Fill(NoContact);
            _knownLeader = string.Empty;
            _knownLeaderTerm = 0;
            _lastLeaderContact = NoContact;
            RouteChanged.Publish();
            return true;
        }
    }

    /// <summary>Grants the leader authority once its leader-term entry is committed, unless a higher term was seen meanwhile.</summary>
    /// <param name="term">The term whose leader-term entry is committed.</param>
    /// <returns><see langword="true" /> when this node now has authority in <paramref name="term" />; never once the driver stopped.</returns>
    internal bool GrantAuthority(ulong term)
    {
        lock (_sync)
        {
            if (!_driven || _role != ReplicaGroupRole.Leader || _term != term || _highestObservedTerm > term)
                return false;

            _hasAuthority = true;
            RouteChanged.Publish();
            return true;
        }
    }

    /// <summary>Restarts the quorum grace of a leader still without authority, after a promotion attempt that held its driver.</summary>
    /// <param name="term">The led term.</param>
    /// <remarks>
    /// A promotion that starts the leadership probes every follower before any heartbeat, and a dead follower holds that probe for its
    /// whole timeout; the grace then restarts when the heartbeats can, so the leader is not deposed for followers it could not ask. A
    /// leader without authority serves nothing, so the longer tenure admits nothing; a leader already authorized keeps its grace.
    /// </remarks>
    internal void RestartQuorumGrace(ulong term)
    {
        lock (_sync)
        {
            if (_role == ReplicaGroupRole.Leader && _term == term && !_hasAuthority)
                _leaderSince = Clock.GetTimestamp();
        }
    }

    /// <summary>Marks whether an election driver runs for the group.</summary>
    /// <param name="driven">Whether the driver runs.</param>
    /// <remarks>A driver that stops leaves the group a follower without authority.</remarks>
    internal void SetElectionDriven(bool driven)
    {
        lock (_sync)
        {
            _driven = driven;
            if (driven)
                return;

            SetRoleLocked(ReplicaGroupRole.Follower);
            RouteChanged.Publish();
        }
    }

    /// <summary>Moves this node to a role without authority; authority comes only with <see cref="GrantAuthority" />.</summary>
    /// <param name="role">The new role.</param>
    /// <remarks>Called under <see cref="_sync" />.</remarks>
    private void SetRoleLocked(ReplicaGroupRole role)
    {
        _hasAuthority = false;
        _role = role;
    }

    /// <summary>Raises the highest observed term; a leader loses its authority at once to a term above its own.</summary>
    /// <param name="term">The term seen.</param>
    /// <returns><see langword="true" /> when the term is above the driver's term while a driver runs, so the driver must be woken.</returns>
    /// <remarks>
    /// The driver still steps down and retires; the authority is gone before it even wakes, so no write is admitted meanwhile. A revoked
    /// authority is published as a route change.
    /// </remarks>
    private bool RaiseLocked(ulong term)
    {
        if (term > _highestObservedTerm)
            _highestObservedTerm = term;

        if (term > _term && _role == ReplicaGroupRole.Leader && _hasAuthority)
        {
            _hasAuthority = false;
            RouteChanged.Publish();
        }

        return _driven && term > _term;
    }
}
