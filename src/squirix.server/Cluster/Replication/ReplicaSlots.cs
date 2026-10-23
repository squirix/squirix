using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Maps between the replica slots of a group and the follower senders of its leader, whichever slot the leader holds.</summary>
/// <param name="LeaderReplicaIndex">Zero-based slot of the leader.</param>
/// <remarks>
/// A group keeps its slot order for its whole life, so slot-indexed state (eligibility, match indexes, repairs) survives a change of
/// leader. The leader holds one slot and sends to every other one; its senders are the other slots in slot order.
/// </remarks>
[Immutable]
internal readonly record struct ReplicaSlots(int LeaderReplicaIndex)
{
    /// <summary>Tells whether a slot is a follower of the leader.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    /// <returns><see langword="true" /> for every slot except the leader's.</returns>
    internal bool IsFollower(int replicaIndex) => replicaIndex != LeaderReplicaIndex;

    /// <summary>Returns the position of a follower slot among the leader's senders.</summary>
    /// <param name="replicaIndex">Zero-based follower slot.</param>
    /// <returns>The sender position.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="replicaIndex" /> is the leader slot, which has no sender.</exception>
    internal int SenderOf(int replicaIndex) => replicaIndex.CompareTo(LeaderReplicaIndex) switch
    {
        < 0 => replicaIndex,
        > 0 => replicaIndex - 1,
        _ => throw new ArgumentOutOfRangeException(nameof(replicaIndex), replicaIndex, "The leader slot has no sender."),
    };

    /// <summary>Returns the follower slot a sender position serves.</summary>
    /// <param name="senderIndex">Zero-based sender position.</param>
    /// <returns>The follower slot.</returns>
    internal int SlotOf(int senderIndex) => senderIndex < LeaderReplicaIndex ? senderIndex : senderIndex + 1;
}
