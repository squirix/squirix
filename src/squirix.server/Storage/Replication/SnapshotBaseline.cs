using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Describes the compacted base entry that precedes the retained follower log entries.</summary>
/// <param name="LastIncludedIndex">The highest log index covered by the installed snapshot.</param>
/// <param name="LastIncludedTerm">The term of the entry at <paramref name="LastIncludedIndex" />.</param>
[Immutable]
internal sealed record SnapshotBaseline(ulong LastIncludedIndex, ulong LastIncludedTerm);
