using System.Diagnostics.CodeAnalysis;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Explicit post-proof activation gate for automatic failover and quorum reads.</summary>
/// <remarks>
/// Both switches stay disabled by default (<see cref="TopologyOptions.AutomaticFailoverEnabled" /> and
/// <see cref="TopologyOptions.QuorumReadsEnabled" /> default to <see langword="false" />). Enabling is an
/// explicit post-proof opt-in exercised by the failover and quorum-read proof-matrix tests; no public host
/// surface exposes these switches. Automatic failover applies only to RF&gt;=3: RF=2 never promotes after
/// peer loss, RF=1 never elects, the minority fails closed, and a rejoined former leader must catch up
/// before regaining eligibility.
/// </remarks>
[SuppressMessage(
    "Usage",
    "MA0182:Internal type is apparently never used",
    Justification = "Test-only activation seam until failover activation wires the gate in a follow-up milestone.")]
internal static class FailoverActivationGate
{
    /// <summary>Checks whether the node may start an election under explicit failover activation.</summary>
    /// <param name="replicaCount">The configured replica factor, including the leader.</param>
    /// <param name="failoverEnabled">The explicit post-proof failover switch.</param>
    /// <param name="hasMajorityContact">Whether the node recently contacted a majority.</param>
    /// <param name="logCaughtUp">Whether the rejoining log reached the leader commit index.</param>
    /// <param name="currentTerm">The locally persisted current term.</param>
    /// <param name="observedTerm">The highest term observed from a peer.</param>
    /// <returns>The election eligibility verdict.</returns>
    internal static FailoverEligibilityVerdict CheckElection(
        int replicaCount,
        bool failoverEnabled,
        bool hasMajorityContact,
        bool logCaughtUp,
        ulong currentTerm,
        ulong observedTerm)
    {
        return replicaCount switch
        {
            <= 1 => new FailoverEligibilityVerdict(false, FailoverDenial.SingleNode),
            2 => new FailoverEligibilityVerdict(false, FailoverDenial.ReplicaFactorTooLow),
            _ when !failoverEnabled => new FailoverEligibilityVerdict(false, FailoverDenial.Disabled),
            _ when observedTerm > currentTerm => new FailoverEligibilityVerdict(false, FailoverDenial.StaleTerm),
            _ when !hasMajorityContact => new FailoverEligibilityVerdict(false, FailoverDenial.NoMajority),
            _ when !logCaughtUp => new FailoverEligibilityVerdict(false, FailoverDenial.LogNotCaughtUp),
            _ => new FailoverEligibilityVerdict(true, FailoverDenial.None),
        };
    }

    /// <summary>Checks whether a quorum read may be served under explicit quorum-read activation.</summary>
    /// <param name="quorumReadsEnabled">The explicit post-proof quorum-read switch.</param>
    /// <param name="replicaCount">The configured replica factor, including the leader.</param>
    /// <param name="hasMajorityContact">Whether the leader recently contacted a majority.</param>
    /// <param name="isLeader">Whether this node considers itself the leader.</param>
    /// <param name="currentTerm">The locally persisted current term.</param>
    /// <param name="observedTerm">The highest term observed from a peer.</param>
    /// <param name="read">The read-specific quorum and index inputs.</param>
    /// <returns>The authority decision for the read.</returns>
    internal static LeaderAuthorityDecision CheckQuorumRead(
        bool quorumReadsEnabled,
        int replicaCount,
        bool hasMajorityContact,
        bool isLeader,
        ulong currentTerm,
        ulong observedTerm,
        LeaderReadState read)
    {
        const LeaderAuthorityDenial denial = LeaderAuthorityDenial.QuorumNotConfirmed;
        return quorumReadsEnabled ? LeaderAuthorityGate.CheckRead(replicaCount, hasMajorityContact, isLeader, currentTerm, observedTerm, read)
            : new LeaderAuthorityDecision(false, denial);
    }
}
