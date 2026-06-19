using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Journaling.PipelinedWal.Backends.Pipelined;

namespace Squirix.Server.Storage.Journaling.PipelinedWal;

/// <summary>Creates <see cref="IJournalCoordinator"/> instances for the configured backend.</summary>
internal static class JournalCoordinatorFactory
{
    public static async Task<IJournalCoordinator> CreateAsync(
        PersistenceOptions persistence,
        Manifest manifest,
        ManifestStore manifestStore,
        JournalStartupGate startupGate,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(persistence);
        return persistence.JournalBackend switch
        {
            JournalBackend.JsonFramed => await JournalWriter.CreateAsync(persistence, manifest, manifestStore, startupGate, cancellationToken).ConfigureAwait(false),
            JournalBackend.PipelinedWal => await PipelinedWalJournalCoordinator.CreateAsync(persistence, manifest, manifestStore, startupGate, cancellationToken).ConfigureAwait(false),
            _ => throw new ArgumentOutOfRangeException(nameof(persistence), persistence.JournalBackend, "unknown journal backend."),
        };
    }
}
