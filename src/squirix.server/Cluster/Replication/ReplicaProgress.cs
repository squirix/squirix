using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Verified durable progress reported for one replica-group participant.</summary>
/// <param name="NextIndex">Next log index the leader should send.</param>
/// <param name="MatchIndex">Highest contiguous durable log index.</param>
/// <param name="CommitIndex">Highest durable committed index.</param>
/// <param name="AppliedIndex">Highest durable applied index.</param>
/// <param name="LastTerm">Term of the last durable log entry or installed snapshot baseline.</param>
/// <param name="TopologyFingerprint">Static topology fingerprint.</param>
/// <param name="ConfigurationGeneration">Stopped-topology configuration generation.</param>
/// <param name="StateChecksum">Checksum of the verified durable state at <paramref name="MatchIndex" />.</param>
[Immutable]
internal readonly record struct ReplicaProgress(
    ulong NextIndex,
    ulong MatchIndex,
    ulong CommitIndex,
    ulong AppliedIndex,
    ulong LastTerm,
    ReadOnlyMemory<byte> TopologyFingerprint,
    ulong ConfigurationGeneration,
    uint StateChecksum)
{
    /// <summary>Gets a value indicating whether the progress watermarks and identity fields form a valid report.</summary>
    internal bool IsValid =>
        !TopologyFingerprint.IsEmpty && ConfigurationGeneration > 0 && MatchIndex != ulong.MaxValue && NextIndex == MatchIndex + 1 && AppliedIndex <= CommitIndex && CommitIndex <= MatchIndex;

    /// <summary>Returns whether this report exactly matches the leader-verified readiness target.</summary>
    /// <param name="expected">Leader-verified target state.</param>
    /// <returns><see langword="true" /> when every readiness field matches.</returns>
    internal bool Matches(in ReplicaProgress expected) =>
        NextIndex == expected.NextIndex && MatchIndex == expected.MatchIndex && CommitIndex == expected.CommitIndex && AppliedIndex == expected.AppliedIndex && LastTerm == expected.LastTerm &&
        ConfigurationGeneration == expected.ConfigurationGeneration && StateChecksum == expected.StateChecksum &&
        TopologyFingerprint.Span.SequenceEqual(expected.TopologyFingerprint.Span);

    /// <summary>Returns whether this report belongs to the same immutable topology identity.</summary>
    /// <param name="other">Other progress report.</param>
    /// <returns><see langword="true" /> when topology identity matches.</returns>
    internal bool MatchesTopology(in ReplicaProgress other) =>
        ConfigurationGeneration == other.ConfigurationGeneration && TopologyFingerprint.Span.SequenceEqual(other.TopologyFingerprint.Span);

    /// <summary>Creates a defensive copy suitable for retention beyond the caller's buffer lifetime.</summary>
    /// <returns>An owned progress report.</returns>
    internal ReplicaProgress WithOwnedFingerprint() => this with { TopologyFingerprint = TopologyFingerprint.ToArray() };
}
