using System;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
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
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(journalMaintenance);
        ArgumentNullException.ThrowIfNull(manifest);
        ArgumentNullException.ThrowIfNull(snapshotReader);
        ArgumentNullException.ThrowIfNull(persistence);
        ArgumentNullException.ThrowIfNull(cluster);
        Snapshot = snapshot;
        JournalMaintenance = journalMaintenance;
        Manifest = manifest;
        SnapshotReader = snapshotReader;
        Persistence = persistence;
        Cluster = cluster;
        TimeProvider = timeProvider ?? TimeProvider.System;
    }

    internal TopologyOptions Cluster { get; }

    /// <summary>Gets who decides expiry; under <see cref="CacheExpiryAuthority.CommittedRecords" /> compaction keeps entries past their deadline.</summary>
    internal CacheExpiryAuthority Expiry { get; init; } = CacheExpiryAuthority.LocalClock;

    internal IExclusiveMaintenanceExecutor JournalMaintenance { get; }

    internal Ledger Manifest { get; }

    internal PersistenceOptions Persistence { get; }

    internal Coordinator Snapshot { get; }

    internal ISnapshotReader SnapshotReader { get; }

    internal TimeProvider TimeProvider { get; }
}
