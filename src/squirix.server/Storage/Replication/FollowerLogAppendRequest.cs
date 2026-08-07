using System;

namespace Squirix.Server.Storage.Replication;

/// <summary>An append request carried over the closed replication wire.</summary>
/// <param name="LeaderNodeId">The node identifier of the leader issuing the append.</param>
/// <param name="CurrentTerm">The leader's current term.</param>
/// <param name="PrevLogIndex">The index of the entry immediately preceding the batch.</param>
/// <param name="PrevLogTerm">The term of the entry at <paramref name="PrevLogIndex" />.</param>
/// <param name="LeaderCommitIndex">The leader's committed index.</param>
/// <param name="Entries">The ordered batch of entries to append.</param>
internal readonly record struct FollowerLogAppendRequest(
    string LeaderNodeId,
    ulong CurrentTerm,
    ulong PrevLogIndex,
    ulong PrevLogTerm,
    ulong LeaderCommitIndex,
    ReadOnlyMemory<FollowerLogEntry> Entries);
