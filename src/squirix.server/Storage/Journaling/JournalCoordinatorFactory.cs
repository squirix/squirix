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
        Manifest manifest,
        ManifestStore manifestStore,
        JournalStartupGate startupGate,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(persistence);
        if (persistence.JournalBackend is not JournalBackend.Pipelined)
        {
            throw new ArgumentOutOfRangeException(
                nameof(persistence),
                persistence.JournalBackend,
                "Only the pipelined journal backend is supported.");
        }

        return await JournalCoordinator.CreateAsync(persistence, manifest, manifestStore, startupGate, cancellationToken).ConfigureAwait(false);
    }
}
