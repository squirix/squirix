namespace Squirix.Server.Adapters.Rest;

internal sealed record HealthReadyDetailSections
{
    internal HealthReadyDetailSections(
        HealthCompactionDetails compaction,
        HealthClientPoolDetails clientPool,
        HealthCoordinationDetails coordination,
        HealthMemoryPressureDetails memoryPressure,
        HealthRetentionCleanupDetails retentionCleanup,
        HealthJournalDiskDetails journalDisk)
    {
        Compaction = compaction;
        ClientPool = clientPool;
        Coordination = coordination;
        MemoryPressure = memoryPressure;
        RetentionCleanup = retentionCleanup;
        JournalDisk = journalDisk;
    }

    internal HealthClientPoolDetails ClientPool { get; }

    internal HealthCompactionDetails Compaction { get; }

    internal HealthCoordinationDetails Coordination { get; }

    internal HealthJournalDiskDetails JournalDisk { get; }

    internal HealthMemoryPressureDetails MemoryPressure { get; }

    internal HealthRetentionCleanupDetails RetentionCleanup { get; }
}
