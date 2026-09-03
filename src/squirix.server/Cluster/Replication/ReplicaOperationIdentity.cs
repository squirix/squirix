using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Canonical identity of one replica-group mutation.</summary>
/// <param name="GroupId">Replica group identity.</param>
/// <param name="OperationScope">Scope used to deduplicate the operation.</param>
/// <param name="OperationId">Client operation identity.</param>
/// <param name="OperationFingerprint">Canonical operation fingerprint.</param>
[Immutable]
internal sealed record ReplicaOperationIdentity(
    string GroupId,
    string OperationScope,
    string OperationId,
    ReadOnlyMemory<byte> OperationFingerprint);
