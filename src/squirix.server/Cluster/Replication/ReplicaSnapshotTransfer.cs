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
    /// <param name="maxSnapshotBytes">Maximum accepted payload length; bounds the pooled rent.</param>
    /// <returns>The self-describing transfer contract.</returns>
    internal static ReplicaSnapshotTransfer Create(in GroupSnapshot snapshot, int maxSnapshotBytes = GroupSnapshotStore.DefaultMaxSnapshotBytes)
    {
        ArgumentNullException.ThrowIfNull(snapshot.CommittedOutcomes);

        // CommittedOutcomes is caller-owned and may keep mutating: materialize it once so the sizing pass,
        // the encoding pass, and every later IsValidFor read observe identical elements.
        var stable = snapshot with
        {
            CommittedOutcomes = [.. snapshot.CommittedOutcomes],
            TopologyFingerprint = snapshot.TopologyFingerprint.ToArray(),
        };
        var integrity = GroupSnapshotStore.ComputePayloadIntegrity(stable, maxSnapshotBytes);
        return new ReplicaSnapshotTransfer(
            stable.GroupId,
            stable.TopologyFingerprint.ToArray(),
            stable.ConfigurationGeneration,
            stable.LastIncludedTerm,
            stable.LastIncludedIndex,
            stable.CommitIndex,
            integrity.Length,
            integrity.Checksum,
            stable);
    }

    /// <summary>Validates descriptor fields and canonical payload integrity for the target group.</summary>
    /// <param name="expectedGroupId">Target replica group.</param>
    /// <param name="maxSnapshotBytes">Maximum accepted payload length; bounds the pooled rent.</param>
    /// <returns><see langword="true" /> when all identity, boundary, length, and checksum fields match.</returns>
    internal bool IsValidFor(string expectedGroupId, int maxSnapshotBytes = GroupSnapshotStore.DefaultMaxSnapshotBytes)
    {
        if (!string.Equals(GroupId, expectedGroupId, StringComparison.Ordinal) || !string.Equals(GroupId, Snapshot.GroupId, StringComparison.Ordinal) || PayloadLength <= 0 ||
            PayloadLength > maxSnapshotBytes || ConfigurationGeneration == 0UL || LastIncludedIndex == 0UL || LastIncludedTerm == 0UL || CommitIndex < LastIncludedIndex)
            return false;

        if (ConfigurationGeneration != Snapshot.ConfigurationGeneration || LastIncludedTerm != Snapshot.LastIncludedTerm || LastIncludedIndex != Snapshot.LastIncludedIndex ||
            CommitIndex != Snapshot.CommitIndex || !TopologyFingerprint.Span.SequenceEqual(Snapshot.TopologyFingerprint.Span))
            return false;

        GroupSnapshotPayloadIntegrity integrity;
        try
        {
            integrity = GroupSnapshotStore.ComputePayloadIntegrity(Snapshot, maxSnapshotBytes);
        }
        catch (InvalidOperationException)
        {
            return false;
        }

        return PayloadLength == integrity.Length && PayloadChecksum == integrity.Checksum;
    }
}
