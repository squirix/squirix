using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.JsonFramed;
using Squirix.Server.Storage.Journaling.Pipelined;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Creates <see cref="IJournalCoordinator" /> instances for the configured backend.</summary>
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
            JournalBackend.Pipelined => await PipelinedJournalCoordinator.CreateAsync(persistence, manifest, manifestStore, startupGate, cancellationToken).ConfigureAwait(false),
            _ => throw new ArgumentOutOfRangeException(nameof(persistence), "unknown journal backend."),
        };
    }
}
