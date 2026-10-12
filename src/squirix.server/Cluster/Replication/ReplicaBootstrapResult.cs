namespace Squirix.Server.Cluster.Replication;

/// <summary>What one iteration of the first start of a replica group did.</summary>
internal enum ReplicaBootstrapResult
{
    /// <summary>The copy of the owner was unseeded and is complete now; the other members hold an offer.</summary>
    Completed = 0,

    /// <summary>The copy of the owner was complete already; nobody was asked.</summary>
    AlreadyComplete = 1,

    /// <summary>The copy of the owner is incomplete; the first start has nothing to do for it.</summary>
    Incomplete = 2,

    /// <summary>The log of the owner is not ready; nobody was asked.</summary>
    LogNotReady = 3,

    /// <summary>A member did not answer, answered with another topology, is not ready, or is not unseeded; nothing was offered.</summary>
    WaitingForMember = 4,

    /// <summary>A member holds a complete copy of the group, so the empty copy of the owner needs repair; nothing was offered.</summary>
    MemberHoldsCompleteCopy = 5,

    /// <summary>A member did not accept the offer; the copy of the owner stays unseeded.</summary>
    OfferRefused = 6,
}
