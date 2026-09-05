using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Next repair payload selected from leader retention state.</summary>
/// <param name="Kind">Selected payload kind.</param>
/// <param name="Batch">Entry batch when <paramref name="Kind" /> is <see cref="ReplicaRepairSelectionKind.Entries" />.</param>
/// <param name="Snapshot">Snapshot transfer when <paramref name="Kind" /> is <see cref="ReplicaRepairSelectionKind.Snapshot" />.</param>
[Immutable]
internal readonly record struct ReplicaRepairSelection(ReplicaRepairSelectionKind Kind, ReplicaRepairBatch Batch, ReplicaSnapshotTransfer? Snapshot);
