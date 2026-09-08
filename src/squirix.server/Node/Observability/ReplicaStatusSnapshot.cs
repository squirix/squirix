using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

/// <summary>Read-only replica-group status observed without mutating replication state.</summary>
/// <param name="NodeId">The observing node identifier used for metric labels.</param>
/// <param name="GroupId">The replica group identifier.</param>
/// <param name="ReplicaCount">The configured replica factor including the leader.</param>
/// <param name="CurrentTerm">The locally persisted current term.</param>
/// <param name="ObservedTerm">The highest term observed from a peer.</param>
/// <param name="LastLogIndex">The durable last log index.</param>
/// <param name="CommitIndex">The durable commit index.</param>
/// <param name="LastAppliedIndex">The index last applied to memory.</param>
/// <param name="FingerprintMatch">Whether the durable topology fingerprint matches the expected identity.</param>
/// <param name="GenerationMatch">Whether the durable configuration generation matches the expected identity.</param>
/// <param name="LogReady">Whether the group log reached durability readiness.</param>
/// <param name="IsLeader">Whether this node considers itself the leader of the group.</param>
/// <param name="HasMajorityContact">Whether the group recently contacted a majority of replicas.</param>
[Immutable]
internal readonly record struct ReplicaStatusSnapshot(
    string NodeId,
    string GroupId,
    int ReplicaCount,
    ulong CurrentTerm,
    ulong ObservedTerm,
    ulong LastLogIndex,
    ulong CommitIndex,
    ulong LastAppliedIndex,
    bool FingerprintMatch,
    bool GenerationMatch,
    bool LogReady,
    bool IsLeader,
    bool HasMajorityContact);
