using System.Collections.Generic;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>A range of retained log entries read back from disk, with the position that precedes them and the log bounds observed in the same gated read.</summary>
/// <param name="Retained"><see langword="false" /> when the position before the requested index is not verifiable: compacted below the snapshot baseline, or above the last log index; <paramref name="Entries" /> is empty then.</param>
/// <param name="PrevLogIndex">The requested index minus one.</param>
/// <param name="PrevLogTerm">The term at <paramref name="PrevLogIndex" />; zero at the log origin or when the position is not retained.</param>
/// <param name="LastLogIndex">The durable last log index observed during the read.</param>
/// <param name="CommitIndex">The durable commit index observed during the read.</param>
/// <param name="Entries">Owned entries from the requested index through the smaller of the last log index and the requested count, in index order; empty at the tail.</param>
[Immutable]
internal readonly record struct FollowerLogEntriesRead(
    bool Retained,
    ulong PrevLogIndex,
    ulong PrevLogTerm,
    ulong LastLogIndex,
    ulong CommitIndex,
    IReadOnlyList<FollowerLogEntry> Entries);
