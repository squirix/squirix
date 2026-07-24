using System;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Composed journal coordination surface for key-value mutations, snapshots, and maintenance.</summary>
internal interface IJournalCoordinator :
    IJournalMetrics,
    IJournalDiskUsage,
    IExclusiveMaintenanceExecutor,
    IJournalMutationAppender,
    IJournalDurabilityCoordinator,
    IJournalSnapshotBarrier,
    IJournalCoordinatorLifecycle,
    IAsyncDisposable;
