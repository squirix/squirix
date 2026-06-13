namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Health-ready diagnostics snapshot for REST endpoints.
/// </summary>
internal sealed class HealthReadyDetailsSnapshot
{
    public required HealthClientPoolSnapshot ClientPool { get; init; }

    public required HealthCompactionSnapshot Compaction { get; init; }

    public required HealthCoordinationSnapshot Coordination { get; init; }

    public required long JournalBacklogOps { get; init; }

    public required HealthMemoryPressureSnapshot MemoryPressure { get; init; }

    public required double? SnapshotAgeSeconds { get; init; }

    public required bool SnapshotInFlight { get; init; }
}
