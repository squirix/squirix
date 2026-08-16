using System;
using Squirix.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Durable per-group metadata carried by the follower log.</summary>
/// <param name="GroupId">The replica group identifier.</param>
/// <param name="TopologyFingerprint">The topology fingerprint the group was created under.</param>
/// <param name="ConfigurationGeneration">The configuration generation of the group.</param>
/// <param name="CurrentTerm">The highest term this node has observed.</param>
/// <param name="VotedFor">The node voted for in the current term, or an empty string.</param>
/// <param name="LastLogIndex">The durable last log index.</param>
/// <param name="CommitIndex">The durable commit index.</param>
/// <param name="LastAppliedIndex">The index last applied to memory by the coordinator.</param>
[Immutable]
internal readonly record struct GroupLogMetadata(
    string GroupId,
    ReadOnlyMemory<byte> TopologyFingerprint,
    ulong ConfigurationGeneration,
    ulong CurrentTerm,
    string VotedFor,
    ulong LastLogIndex,
    ulong CommitIndex,
    ulong LastAppliedIndex);
