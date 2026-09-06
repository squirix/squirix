using System.Collections.Generic;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Leader append batch with identity, consistency, and commit positions.</summary>
/// <param name="Records">Canonical records in log order.</param>
/// <param name="LeaderNodeId">Leader node identity.</param>
/// <param name="LeaderTerm">Leader term authorizing the append.</param>
/// <param name="PrevLogIndex">Index preceding the batch.</param>
/// <param name="PrevLogTerm">Term at the preceding index.</param>
/// <param name="LeaderCommitIndex">Leader commit index.</param>
[Immutable]
internal readonly record struct FollowerBatch(
    IReadOnlyList<ReplicaLogRecord> Records,
    string LeaderNodeId,
    ulong LeaderTerm,
    ulong PrevLogIndex,
    ulong PrevLogTerm,
    ulong LeaderCommitIndex);
