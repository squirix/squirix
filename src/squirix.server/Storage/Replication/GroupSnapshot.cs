using System;
using System.Collections.Generic;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Durable snapshot of one replica group's committed state.</summary>
/// <param name="GroupId">The replica group identifier.</param>
/// <param name="TopologyFingerprint">The topology fingerprint the group was created under.</param>
/// <param name="ConfigurationGeneration">The configuration generation of the group.</param>
/// <param name="LastIncludedTerm">The term of the entry at <paramref name="LastIncludedIndex" />.</param>
/// <param name="LastIncludedIndex">The highest committed journal index covered by this snapshot.</param>
/// <param name="CommitIndex">The durable commit index carried by the snapshot.</param>
/// <param name="CommittedOutcomes">The committed idempotency outcomes carried by the snapshot.</param>
[Immutable]
internal readonly record struct GroupSnapshot(
    string GroupId,
    ReadOnlyMemory<byte> TopologyFingerprint,
    ulong ConfigurationGeneration,
    ulong LastIncludedTerm,
    ulong LastIncludedIndex,
    ulong CommitIndex,
    IReadOnlyList<GroupIdempotencyRecord> CommittedOutcomes);
