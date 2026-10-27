using System;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>When each follower of a led group last answered as one that can vote for another leader, and as one that can stand for election.</summary>
/// <remarks>
/// A follower whose log is ready votes and can stand. An incomplete copy still votes but never stands. A follower whose log is not ready,
/// or latched a failure, answers a heartbeat but refuses every ballot, so it counts as neither. Another leader can win only when the voters
/// form a majority of the group without the leader and at least one of them can stand. Unlike the quorum contact of the leader, this one
/// has no grace for a new leader and never counts the leader itself.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaElectableContacts
{
    /// <summary>Marks a follower slot that never answered in that role.</summary>
    private const long NoContact = long.MinValue;

    private readonly TimeProvider _clock;
    private readonly long[] _voter;
    private readonly long[] _candidate;

    /// <summary>Initializes a new instance of the <see cref="ReplicaElectableContacts" /> class.</summary>
    /// <param name="replicaCount">The number of replicas of the group, the leader included.</param>
    /// <param name="clock">The time source of the group.</param>
    internal ReplicaElectableContacts(int replicaCount, TimeProvider clock)
    {
        _clock = clock;
        _voter = new long[replicaCount];
        _candidate = new long[replicaCount];
        Reset();
    }

    /// <summary>Records that a follower answered as a voter now, and as a candidate only when its log is ready, so the latest answer decides.</summary>
    /// <param name="replicaIndex">The slot of the follower.</param>
    /// <param name="canStand"><see langword="true" /> when the follower answered from a ready log, so it can also stand for election.</param>
    internal void Record(int replicaIndex, bool canStand)
    {
        if (replicaIndex < 0 || replicaIndex >= _voter.Length)
            return;

        var now = _clock.GetTimestamp();
        Volatile.Write(ref _voter[replicaIndex], now);
        Volatile.Write(ref _candidate[replicaIndex], canStand ? now : NoContact);
    }

    /// <summary>Forgets every contact, as a new leadership starts.</summary>
    internal void Reset()
    {
        for (var i = 0; i < _voter.Length; i++)
        {
            Volatile.Write(ref _voter[i], NoContact);
            Volatile.Write(ref _candidate[i], NoContact);
        }
    }

    /// <summary>Tells whether the followers that answered lately as voters form a majority of the group without the leader, with a candidate among them.</summary>
    /// <param name="leaderReplicaIndex">The slot of this node, which does not count.</param>
    /// <param name="window">How recent an answer must be.</param>
    /// <returns><see langword="true" /> when more than half of all replicas are such voters and at least one answered from a ready log.</returns>
    internal bool HasMajority(int leaderReplicaIndex, TimeSpan window)
    {
        var voters = 0;
        var hasCandidate = false;
        for (var i = 0; i < _voter.Length; i++)
        {
            if (i == leaderReplicaIndex)
                continue;

            var at = Volatile.Read(ref _voter[i]);
            if (at == NoContact || _clock.GetElapsedTime(at) >= window)
                continue;

            voters++;
            var stood = Volatile.Read(ref _candidate[i]);
            if (stood != NoContact && _clock.GetElapsedTime(stood) < window)
                hasCandidate = true;
        }

        return hasCandidate && voters * 2 > _voter.Length;
    }
}
