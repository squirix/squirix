using System;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>When each follower of a led group last answered from a ready log, which is the contact that lets it vote for another leader.</summary>
/// <remarks>
/// A follower whose log latched a failure still answers a heartbeat, as not ready, but refuses every ballot, so it cannot help elect a new
/// leader. Only an answer that accepted the append or reported a log mismatch counts here. Unlike the quorum contact of the leader, this one
/// has no grace for a new leader and never counts the leader itself.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaElectableContacts
{
    /// <summary>Marks a follower slot that never answered from a ready log.</summary>
    private const long NoContact = long.MinValue;

    private readonly TimeProvider _clock;
    private readonly long[] _contact;

    /// <summary>Initializes a new instance of the <see cref="ReplicaElectableContacts" /> class.</summary>
    /// <param name="replicaCount">The number of replicas of the group, the leader included.</param>
    /// <param name="clock">The time source of the group.</param>
    internal ReplicaElectableContacts(int replicaCount, TimeProvider clock)
    {
        _clock = clock;
        _contact = new long[replicaCount];
        Reset();
    }

    /// <summary>Records that a follower answered from a ready log now.</summary>
    /// <param name="replicaIndex">The slot of the follower.</param>
    internal void Record(int replicaIndex)
    {
        if (replicaIndex >= 0 && replicaIndex < _contact.Length)
            Volatile.Write(ref _contact[replicaIndex], _clock.GetTimestamp());
    }

    /// <summary>Forgets every contact, as a new leadership starts.</summary>
    internal void Reset()
    {
        for (var i = 0; i < _contact.Length; i++)
            Volatile.Write(ref _contact[i], NoContact);
    }

    /// <summary>Tells whether the followers that answered from a ready log lately form a majority of the group without the leader.</summary>
    /// <param name="leaderReplicaIndex">The slot of this node, which does not count.</param>
    /// <param name="window">How recent an answer must be.</param>
    /// <returns><see langword="true" /> when more than half of all replicas are such followers.</returns>
    internal bool HasMajority(int leaderReplicaIndex, TimeSpan window)
    {
        var recent = 0;
        for (var i = 0; i < _contact.Length; i++)
        {
            var at = Volatile.Read(ref _contact[i]);
            if (i != leaderReplicaIndex && at != NoContact && _clock.GetElapsedTime(at) < window)
                recent++;
        }

        return recent * 2 > _contact.Length;
    }
}
