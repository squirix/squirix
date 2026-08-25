using System;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Creates <see cref="IJournalCoordinator" /> instances.</summary>
internal static class JournalCoordinatorFactory
{
    internal static IJournalCoordinator Create(PersistenceOptions persistence, State manifest, Ledger store, JournalStartupGate gate) =>
        CreateCore(persistence, manifest, store, gate);

    private static JournalCoordinator CreateCore(PersistenceOptions persistence, State manifest, Ledger store, JournalStartupGate gate)
    {
        ArgumentNullException.ThrowIfNull(persistence);
        JournalRecoveryScan.PrepareActiveSegmentForSequenceScan(manifest, persistence);
        return new JournalCoordinator(persistence, manifest, store, gate);
    }
}
