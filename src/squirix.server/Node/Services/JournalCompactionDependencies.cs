using System;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Services;

internal sealed class JournalCompactionDependencies
{
    internal JournalCompactionDependencies(
        Coordinator snapshot,
        IExclusiveMaintenanceExecutor journalMaintenance,
        ManifestStore manifest,
        ISnapshotReader snapshotReader,
        PersistenceOptions persistence,
        ClusterConfig cluster,
        TimeProvider? timeProvider = null)
    {
        Snapshot = snapshot ?? throw new ArgumentNullException(nameof(snapshot));
        JournalMaintenance = journalMaintenance ?? throw new ArgumentNullException(nameof(journalMaintenance));
        Manifest = manifest ?? throw new ArgumentNullException(nameof(manifest));
        SnapshotReader = snapshotReader ?? throw new ArgumentNullException(nameof(snapshotReader));
        Persistence = persistence ?? throw new ArgumentNullException(nameof(persistence));
        Cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
        TimeProvider = timeProvider ?? TimeProvider.System;
    }

    internal Coordinator Snapshot { get; }

    internal IExclusiveMaintenanceExecutor JournalMaintenance { get; }

    internal ManifestStore Manifest { get; }

    internal ISnapshotReader SnapshotReader { get; }

    internal PersistenceOptions Persistence { get; }

    internal ClusterConfig Cluster { get; }

    internal TimeProvider TimeProvider { get; }
}
