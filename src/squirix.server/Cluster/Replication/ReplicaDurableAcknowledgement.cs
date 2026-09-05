using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Identity-bound proof that one replica durably appended a prepared mutation.</summary>
/// <param name="GroupId">Replica group identity.</param>
/// <param name="Term">Term accepted by the replica.</param>
/// <param name="LogIndex">Highest contiguous durable index acknowledged by this proof.</param>
/// <param name="OperationFingerprint">Canonical operation fingerprint echoed by the replica.</param>
/// <param name="PayloadChecksum">CRC32C of the canonical mutation payload.</param>
/// <param name="IsDurable">Whether the replica completed its durable flush.</param>
/// <param name="IsReady">Whether the replica was eligible to count toward quorum.</param>
[Immutable]
internal readonly record struct ReplicaDurableAcknowledgement(
    string GroupId,
    ulong Term,
    ulong LogIndex,
    ReadOnlyMemory<byte> OperationFingerprint,
    uint PayloadChecksum,
    bool IsDurable,
    bool IsReady)
{
    /// <summary>Checks this acknowledgement identity against the prepared mutation.</summary>
    /// <param name="expected">Prepared mutation whose identity must match.</param>
    /// <returns><see langword="true" /> when the acknowledgement may be recorded.</returns>
    internal bool Matches(PreparedReplicaMutation expected) => IsDurable && IsReady && string.Equals(GroupId, expected.GroupId, StringComparison.Ordinal) &&
                                                               Term == expected.Term && LogIndex == expected.LogIndex && PayloadChecksum == expected.PayloadChecksum &&
                                                               OperationFingerprint.Span.SequenceEqual(expected.OperationFingerprint.Span);
}
