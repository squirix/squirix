using System;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Node.Services;

/// <summary>Evaluates read-only replica-group snapshots into readiness verdicts.</summary>
/// <remarks>
/// Authority fencing reuses <see cref="LeaderAuthorityGate" />: a stale term fences before any other
/// check, and a leader without majority contact fails closed. A follower that is not the leader needs
/// no authority to be ready; its durability readiness and topology agreement decide.
/// </remarks>
internal static class ReplicaReadiness
{
    /// <summary>Describes a readiness verdict for health and doctor output.</summary>
    /// <param name="verdict">The evaluated verdict.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns>A stable human-readable description.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="verdict" /> is not a known verdict.</exception>
    internal static string Describe(ReplicaReadinessVerdict verdict, string groupId)
    {
        return verdict switch
        {
            ReplicaReadinessVerdict.Ready => $"replica group '{groupId}' is ready.",
            ReplicaReadinessVerdict.StaleTerm => $"replica group '{groupId}' reports a stale term.",
            ReplicaReadinessVerdict.MinorityFenced => $"replica group '{groupId}' is a minority without authority.",
            ReplicaReadinessVerdict.TopologyMismatch => $"replica group '{groupId}' reports a topology mismatch.",
            ReplicaReadinessVerdict.LogNotReady => $"replica group '{groupId}' log is not ready.",
            _ => throw new ArgumentOutOfRangeException(nameof(verdict), verdict, "Unsupported readiness verdict."),
        };
    }

    /// <summary>Evaluates a replica-group snapshot without mutating replication state.</summary>
    /// <param name="snapshot">The observed replica-group status.</param>
    /// <returns>The readiness verdict for the snapshot.</returns>
    internal static ReplicaReadinessVerdict Evaluate(in ReplicaStatusSnapshot snapshot)
    {
        if (!snapshot.LogReady)
            return ReplicaReadinessVerdict.LogNotReady;

        if (!snapshot.FingerprintMatch || !snapshot.GenerationMatch)
            return ReplicaReadinessVerdict.TopologyMismatch;

        var authority = LeaderAuthorityGate.CheckWrite(snapshot.ReplicaCount, snapshot.HasMajorityContact, snapshot.IsLeader, snapshot.CurrentTerm, snapshot.ObservedTerm);
        return (authority.Allowed, authority.Denial) switch
        {
            (true, _) => ReplicaReadinessVerdict.Ready,
            (false, LeaderAuthorityDenial.StaleTerm) => ReplicaReadinessVerdict.StaleTerm,
            (false, LeaderAuthorityDenial.MinorityFenced) => ReplicaReadinessVerdict.MinorityFenced,
            (false, _) => ReplicaReadinessVerdict.Ready,
        };
    }
}
