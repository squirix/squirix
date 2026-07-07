using System;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Endpoint;

internal sealed class HealthReadyDependencies
{
    internal HealthReadyDependencies(
        ManifestStore manifestStore,
        IRetentionCleanupReadinessStatus retentionCleanup,
        IJournalCoordinator journal,
        Coordinator snapshot,
        IJournalCompactionStatus compaction,
        ClusterConfig cluster,
        IMemoryUsageAccounting memoryAccounting)
    {
        ManifestStore = manifestStore ?? throw new ArgumentNullException(nameof(manifestStore));
        RetentionCleanup = retentionCleanup ?? throw new ArgumentNullException(nameof(retentionCleanup));
        Journal = journal ?? throw new ArgumentNullException(nameof(journal));
        Snapshot = snapshot ?? throw new ArgumentNullException(nameof(snapshot));
        Compaction = compaction ?? throw new ArgumentNullException(nameof(compaction));
        Cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
        MemoryAccounting = memoryAccounting ?? throw new ArgumentNullException(nameof(memoryAccounting));
    }

    internal ManifestStore ManifestStore { get; }

    internal IRetentionCleanupReadinessStatus RetentionCleanup { get; }

    internal IJournalCoordinator Journal { get; }

    internal Coordinator Snapshot { get; }

    internal IJournalCompactionStatus Compaction { get; }

    internal ClusterConfig Cluster { get; }

    internal IMemoryUsageAccounting MemoryAccounting { get; }
}
