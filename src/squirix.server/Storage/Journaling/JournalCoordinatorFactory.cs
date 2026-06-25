using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Creates <see cref="IJournalCoordinator" /> instances.</summary>
internal static class JournalCoordinatorFactory
{
    public static async Task<IJournalCoordinator> CreateAsync(
        PersistenceOptions persistence,
        Manifest.ManifestState manifest,
        ManifestStore store,
        JournalStartupGate gate,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(persistence);
        return await JournalCoordinator.CreateAsync(persistence, manifest, store, gate, cancellationToken).ConfigureAwait(false);
    }
}
