using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Exposes the durable state of a replica-group follower log.</summary>
/// <param name="GroupId">The replica group identifier.</param>
/// <param name="TopologyFingerprint">The topology fingerprint the group was created under.</param>
/// <param name="ConfigurationGeneration">The configuration generation of the group.</param>
/// <param name="CurrentTerm">The highest term this node has observed.</param>
/// <param name="VotedFor">The node voted for in the current term, or an empty string.</param>
/// <param name="LastLogIndex">The durable last log index.</param>
/// <param name="LastLogTerm">The term at the durable last log index or installed snapshot baseline.</param>
/// <param name="CommitIndex">The durable commit index.</param>
/// <param name="LastAppliedIndex">The index last applied to memory by the coordinator.</param>
/// <param name="Readiness">The durability readiness state of the log.</param>
[Immutable]
internal readonly record struct FollowerLogStatus(
    string GroupId,
    ReadOnlyMemory<byte> TopologyFingerprint,
    ulong ConfigurationGeneration,
    ulong CurrentTerm,
    string VotedFor,
    ulong LastLogIndex,
    ulong LastLogTerm,
    ulong CommitIndex,
    ulong LastAppliedIndex,
    FollowerLogReadiness Readiness)
{
    /// <summary>Checks whether a leader topology disagrees with this log.</summary>
    /// <param name="fingerprint">Leader topology fingerprint.</param>
    /// <param name="generation">Leader configuration generation.</param>
    /// <returns><see langword="true" /> when the generation is stale or a non-empty durable fingerprint differs.</returns>
    /// <remarks>An empty durable fingerprint adopts nothing here but never conflicts; an older generation is refused.</remarks>
    internal bool IsTopologyMismatch(ReadOnlyMemory<byte> fingerprint, ulong generation)
    {
        var isGenerationStale = generation < ConfigurationGeneration;
        var isFingerprintMismatch = !TopologyFingerprint.IsEmpty && !TopologyFingerprint.Span.SequenceEqual(fingerprint.Span);
        return isGenerationStale || isFingerprintMismatch;
    }
}
