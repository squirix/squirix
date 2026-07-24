namespace Squirix.Server.Runtime.Contracts;

/// <summary>Health-ready diagnostics snapshot for REST endpoints.</summary>
internal sealed class HealthReadyDetailsSnapshot
{
    internal required HealthCompactionSnapshot Compaction { get; init; }

    internal required HealthCoordinationSnapshot Coordination { get; init; }

    internal required ulong JournalBacklogOps { get; init; }

    internal required HealthJournalDiskSnapshot JournalDisk { get; init; }

    internal required HealthMemoryPressureSnapshot MemoryPressure { get; init; }

    internal required HealthRetentionCleanupSnapshot RetentionCleanup { get; init; }

    internal required double? SnapshotAgeSeconds { get; init; }

    internal required bool SnapshotInFlight { get; init; }

    internal required HealthClientPoolSnapshot ClientPool { get; init; }
}
