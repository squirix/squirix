using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Creates <see cref="IJournalCoordinator" /> instances.</summary>
internal static class JournalCoordinatorFactory
{
    internal static async Task<IJournalCoordinator> CreateAsync(
        PersistenceOptions persistence,
        State manifest,
        Ledger store,
        JournalStartupGate gate,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(persistence);
        await JournalRecoveryScan.PrepareActiveSegmentForSequenceScanAsync(manifest, persistence, cancellationToken).ConfigureAwait(false);
        return new JournalCoordinator(persistence, manifest, store, gate);
    }
}
