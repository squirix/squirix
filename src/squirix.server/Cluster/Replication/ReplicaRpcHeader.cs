using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Replication envelope identity for one internode RPC.</summary>
/// <param name="GroupId">Replica group identifier.</param>
/// <param name="TopologyFingerprint">Sender topology fingerprint.</param>
/// <param name="ConfigurationGeneration">Sender configuration generation.</param>
/// <param name="Term">Sender term authorizing the call.</param>
/// <param name="LeaderNodeId">Leader node identity.</param>
/// <param name="SenderNodeId">Sender node identity.</param>
[Immutable]
internal readonly record struct ReplicaRpcHeader(
    string GroupId,
    ReadOnlyMemory<byte> TopologyFingerprint,
    ulong ConfigurationGeneration,
    ulong Term,
    string LeaderNodeId,
    string SenderNodeId);
