using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Outcome of compacting a replica-group journal prefix.</summary>
/// <param name="Success">Determines whether the journal prefix was compacted.</param>
/// <param name="SnapshotPath">The published snapshot install path retained for lagging replicas, when available.</param>
/// <param name="RefusalCode">Stable refusal marker when compaction was refused; otherwise empty.</param>
[Immutable]
internal readonly record struct GroupCompactionResult(bool Success, string? SnapshotPath, string RefusalCode);
