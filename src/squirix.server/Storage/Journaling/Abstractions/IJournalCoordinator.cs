using System;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Composed journal coordination surface for key-value mutations, and snapshots.</summary>
internal interface IJournalCoordinator : IJournalMetrics, IJournalDiskUsage, IJournalMutationAppender, IJournalDurabilityCoordinator,
    IJournalSnapshotBarrier, IJournalCoordinatorLifecycle, IAsyncDisposable;
