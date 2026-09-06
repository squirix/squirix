using System;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>A bounded sequential batch selected for follower repair.</summary>
/// <param name="PrevLogIndex">Index immediately preceding the batch.</param>
/// <param name="PrevLogTerm">Term at <paramref name="PrevLogIndex" />, or zero at the log origin.</param>
/// <param name="Entries">Sequential entries beginning at the follower's verified next index.</param>
[Immutable]
internal readonly record struct ReplicaRepairBatch(ulong PrevLogIndex, ulong PrevLogTerm, ReadOnlyMemory<FollowerLogEntry> Entries);
