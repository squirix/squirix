using System;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Self-describing, integrity-protected snapshot transfer contract.</summary>
/// <param name="GroupId">Replica group identity.</param>
/// <param name="TopologyFingerprint">Static topology identity.</param>
/// <param name="ConfigurationGeneration">Stopped-topology configuration generation.</param>
/// <param name="LastIncludedTerm">Term at the snapshot baseline.</param>
/// <param name="LastIncludedIndex">Index at the snapshot baseline.</param>
/// <param name="CommitIndex">Committed index carried by the snapshot.</param>
/// <param name="PayloadLength">Canonical encoded snapshot payload length.</param>
/// <param name="PayloadChecksum">CRC32C of the canonical snapshot payload.</param>
/// <param name="Snapshot">Decoded snapshot payload.</param>
[Immutable]
internal sealed record ReplicaSnapshotTransfer(
    string GroupId,
    ReadOnlyMemory<byte> TopologyFingerprint,
    ulong ConfigurationGeneration,
    ulong LastIncludedTerm,
    ulong LastIncludedIndex,
    ulong CommitIndex,
    int PayloadLength,
    uint PayloadChecksum,
    GroupSnapshot Snapshot)
{
    /// <summary>Creates a transfer descriptor from a fully validated published snapshot.</summary>
    /// <param name="snapshot">Published snapshot.</param>
    /// <returns>The self-describing transfer contract.</returns>
    internal static ReplicaSnapshotTransfer Create(in GroupSnapshot snapshot)
    {
        var integrity = GroupSnapshotStore.ComputePayloadIntegrity(snapshot);
        return new ReplicaSnapshotTransfer(
            snapshot.GroupId,
            snapshot.TopologyFingerprint.ToArray(),
            snapshot.ConfigurationGeneration,
            snapshot.LastIncludedTerm,
            snapshot.LastIncludedIndex,
            snapshot.CommitIndex,
            integrity.Length,
            integrity.Checksum,
            snapshot with { TopologyFingerprint = snapshot.TopologyFingerprint.ToArray() });
    }

    /// <summary>Validates descriptor fields and canonical payload integrity for the target group.</summary>
    /// <param name="expectedGroupId">Target replica group.</param>
    /// <returns><see langword="true" /> when all identity, boundary, length, and checksum fields match.</returns>
    internal bool IsValidFor(string expectedGroupId)
    {
        if (!string.Equals(GroupId, expectedGroupId, StringComparison.Ordinal) || !string.Equals(GroupId, Snapshot.GroupId, StringComparison.Ordinal) || PayloadLength <= 0 ||
            ConfigurationGeneration == 0UL || LastIncludedIndex == 0UL || LastIncludedTerm == 0UL || CommitIndex < LastIncludedIndex)
            return false;

        if (ConfigurationGeneration != Snapshot.ConfigurationGeneration || LastIncludedTerm != Snapshot.LastIncludedTerm || LastIncludedIndex != Snapshot.LastIncludedIndex ||
            CommitIndex != Snapshot.CommitIndex || !TopologyFingerprint.Span.SequenceEqual(Snapshot.TopologyFingerprint.Span))
            return false;

        var integrity = GroupSnapshotStore.ComputePayloadIntegrity(Snapshot);
        return PayloadLength == integrity.Length && PayloadChecksum == integrity.Checksum;
    }
}
