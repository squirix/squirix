using System.Collections.Generic;
using Squirix.Server.Attributes;

namespace Squirix.Server.Runtime.Diagnostics;

/// <summary>Health-ready diagnostics snapshot for `/health/ready/details`.</summary>
[Immutable]
internal sealed class HealthReadyDetailsSnapshot
{
    internal required HealthClientPoolSnapshot ClientPool { get; init; }

    internal required HealthCompactionSnapshot Compaction { get; init; }

    internal required HealthCoordinationSnapshot Coordination { get; init; }

    internal required ulong JournalBacklogOps { get; init; }

    internal required HealthJournalDiskSnapshot JournalDisk { get; init; }

    internal required HealthMemoryPressureSnapshot MemoryPressure { get; init; }

    /// <summary>Gets the retained size of every served replica group log; empty when replication is not configured.</summary>
    internal IReadOnlyList<HealthReplicaGroupSnapshot> ReplicaGroups { get; init; } = [];

    internal required HealthRetentionCleanupSnapshot RetentionCleanup { get; init; }

    internal required double? SnapshotAgeSeconds { get; init; }

    internal required bool SnapshotInFlight { get; init; }
}
