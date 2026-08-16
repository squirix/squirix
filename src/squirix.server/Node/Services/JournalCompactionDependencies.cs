using System;
using Squirix.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Services;

[Immutable]
internal sealed class JournalCompactionDependencies
{
    internal JournalCompactionDependencies(
        Coordinator snapshot,
        IExclusiveMaintenanceExecutor journalMaintenance,
        Ledger manifest,
        ISnapshotReader snapshotReader,
        PersistenceOptions persistence,
        TopologyOptions cluster,
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

    internal TopologyOptions Cluster { get; }

    internal IExclusiveMaintenanceExecutor JournalMaintenance { get; }

    internal Ledger Manifest { get; }

    internal PersistenceOptions Persistence { get; }

    internal Coordinator Snapshot { get; }

    internal ISnapshotReader SnapshotReader { get; }

    internal TimeProvider TimeProvider { get; }
}
