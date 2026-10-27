using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>The outcome of applying the slot rule to the two group snapshot slots.</summary>
/// <param name="Snapshot">The snapshot of the selected slot, or <see langword="null" /> when no snapshot is published.</param>
/// <param name="Slot">The index of the selected slot; zero when there is no snapshot.</param>
/// <param name="Sequence">The sequence number of the selected slot; zero when there is no snapshot.</param>
[Immutable]
internal readonly record struct GroupSnapshotSelection(GroupSnapshot? Snapshot, int Slot, ulong Sequence);
