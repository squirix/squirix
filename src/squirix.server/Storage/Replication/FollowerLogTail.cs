using System.Collections.Generic;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>A durable log status with the entries above its commit index, read under one log gate acquisition.</summary>
/// <param name="Status">The durable log status.</param>
/// <param name="CommitTerm">Term of the entry at the status commit index; zero at the log origin or when <paramref name="Entries" /> is empty.</param>
/// <param name="Entries">Durable entries above the status commit index through its last log index, in index order.</param>
[Immutable]
internal sealed record FollowerLogTail(FollowerLogStatus Status, ulong CommitTerm, IReadOnlyList<FollowerLogEntry> Entries);
