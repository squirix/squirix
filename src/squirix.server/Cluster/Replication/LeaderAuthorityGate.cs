using System.Diagnostics.CodeAnalysis;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Fail-closed authority gate for leader reads and writes.</summary>
/// <remarks>
/// A node without majority contact, or one that observed a higher term, refuses reads and writes
/// instead of serving possibly stale state. An old leader that observes a higher term steps down
/// and stops serving. Single-node groups bypass the protocol entirely: no quorum gate and no
/// election timer. Automatic failover and quorum reads stay disabled until failover activation,
/// so this gate is consulted only by tests and future activation wiring.
/// </remarks>
[SuppressMessage(
    "Usage",
    "MA0182:Internal type is apparently never used",
    Justification = "Test-only activation seam until failover activation wires the authority gate in a follow-up milestone.")]
internal static class LeaderAuthorityGate
{
    /// <summary>Checks whether a read may be served at the given read index.</summary>
    /// <param name="replicaCount">The configured replica factor, including the leader.</param>
    /// <param name="hasMajorityContact">Whether the leader recently contacted a majority.</param>
    /// <param name="isLeader">Whether this node considers itself the leader.</param>
    /// <param name="currentTerm">The locally persisted current term.</param>
    /// <param name="observedTerm">The highest term observed from a peer.</param>
    /// <param name="read">The read-specific quorum and index inputs.</param>
    /// <returns>The authority decision for the read.</returns>
    internal static LeaderAuthorityDecision CheckRead(int replicaCount, bool hasMajorityContact, bool isLeader, ulong currentTerm, ulong observedTerm, LeaderReadState read)
    {
        var write = CheckWrite(replicaCount, hasMajorityContact, isLeader, currentTerm, observedTerm);
        if (!write.Allowed)
            return write;

        // A failed quorum confirmation rejects the current read instead of serving stale state.
        if (replicaCount > 1 && !read.QuorumConfirmed)
            return new LeaderAuthorityDecision(false, LeaderAuthorityDenial.QuorumNotConfirmed);

        // The read is served only after the applied index reaches the read index.
        if (replicaCount > 1 && read.AppliedIndex < read.ReadIndex)
            return new LeaderAuthorityDecision(false, LeaderAuthorityDenial.ReadIndexNotApplied);

        return new LeaderAuthorityDecision(true, LeaderAuthorityDenial.None);
    }

    /// <summary>Checks whether a write may be served.</summary>
    /// <param name="replicaCount">The configured replica factor, including the leader.</param>
    /// <param name="hasMajorityContact">Whether the leader recently contacted a majority.</param>
    /// <param name="isLeader">Whether this node considers itself the leader.</param>
    /// <param name="currentTerm">The locally persisted current term.</param>
    /// <param name="observedTerm">The highest term observed from a peer.</param>
    /// <returns>The authority decision for the write.</returns>
    internal static LeaderAuthorityDecision CheckWrite(int replicaCount, bool hasMajorityContact, bool isLeader, ulong currentTerm, ulong observedTerm)
    {
        // RF=1 bypasses the authority protocol entirely: no quorum gate, no timer.
        if (replicaCount <= 1)
            return new LeaderAuthorityDecision(true, LeaderAuthorityDenial.None);

        // A stale term fences the old leader before any other check.
        if (observedTerm > currentTerm)
            return new LeaderAuthorityDecision(false, LeaderAuthorityDenial.StaleTerm);

        if (!isLeader)
            return new LeaderAuthorityDecision(false, LeaderAuthorityDenial.NotLeader);

        // The minority fails closed: without majority contact neither reads nor writes are served.
        if (!hasMajorityContact)
            return new LeaderAuthorityDecision(false, LeaderAuthorityDenial.MinorityFenced);

        return new LeaderAuthorityDecision(true, LeaderAuthorityDenial.None);
    }
}
