using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.Hosting;

/// <summary>Owns the journal writer singleton lifetime for dependency injection.</summary>
internal sealed class JournalWriterSingleton : IAsyncDisposable
{
    private JournalWriter? _writer;

    public JournalWriter Writer => _writer ?? throw new InvalidOperationException("Journal writer is not initialized.");

    public async Task InitializeAsync(
        PersistenceOptions persistence,
        Manifest manifest,
        ManifestStore manifestStore,
        JournalStartupGate gate,
        CancellationToken cancellationToken)
    {
        if (_writer is not null)
            return;

        _writer = await JournalWriter.CreateAsync(persistence, manifest, manifestStore, gate, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_writer is null)
            return;

        await _writer.DisposeAsync().ConfigureAwait(false);
        _writer = null;
    }
}
