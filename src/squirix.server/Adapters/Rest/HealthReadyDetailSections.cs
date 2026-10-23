using Squirix.Server.Attributes;

namespace Squirix.Server.Adapters.Rest;

[Immutable]
internal sealed record HealthReadyDetailSections
{
    internal HealthReadyDetailSections(
        HealthCompactionDetails compaction,
        HealthClientPoolDetails clientPool,
        HealthCoordinationDetails coordination,
        HealthMemoryPressureDetails memoryPressure,
        HealthRetentionCleanupDetails retentionCleanup,
        HealthJournalDiskDetails journalDisk,
        HealthReplicaGroupDetails[] replicaGroups)
    {
        Compaction = compaction;
        ClientPool = clientPool;
        Coordination = coordination;
        MemoryPressure = memoryPressure;
        RetentionCleanup = retentionCleanup;
        JournalDisk = journalDisk;
        ReplicaGroups = replicaGroups;
    }

    internal HealthClientPoolDetails ClientPool { get; }

    internal HealthCompactionDetails Compaction { get; }

    internal HealthCoordinationDetails Coordination { get; }

    internal HealthJournalDiskDetails JournalDisk { get; }

    internal HealthMemoryPressureDetails MemoryPressure { get; }

    internal HealthReplicaGroupDetails[] ReplicaGroups { get; }

    internal HealthRetentionCleanupDetails RetentionCleanup { get; }
}
