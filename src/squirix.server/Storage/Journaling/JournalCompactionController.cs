using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// On-demand compaction runner. Uses semaphore to ensure only one compaction runs at a time.
/// Safe to co-exist with a periodic compaction service if that service also guards concurrency.
/// </summary>
internal sealed class JournalCompactionController : IDisposable
{
    private readonly IJournalCoordinator _journalWriter;
    private readonly ILogger<JournalCompactionController> _log;
    private readonly ManifestStore _manifestStore;
    private readonly SemaphoreSlim _mutex = new(1, 1);
    private readonly PersistenceOptions _opt;
    private bool _disposed;

    public JournalCompactionController(PersistenceOptions opt, ManifestStore manifestStore, IJournalCoordinator journalWriter, ILogger<JournalCompactionController> log)
    {
        _opt = opt;
        _manifestStore = manifestStore;
        _journalWriter = journalWriter;
        _log = log;
    }

    public async Task<bool> TryTriggerNowAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!await _mutex.WaitAsync(0, cancellationToken).ConfigureAwait(false))
            return false;

        try
        {
            var manifest = _manifestStore.ReadCurrentOrDefault();
            var snapIdx = manifest.LastSnapshot?.Index ?? 0;
            LogManager.ManualCompactionStart(_log, snapIdx);
            await _journalWriter.ExecuteMaintenanceExclusiveAsync(ct => new ValueTask(JournalCompactor.CompactAsync(_opt, _manifestStore, ct)), cancellationToken)
                                .ConfigureAwait(false);
            LogManager.ManualCompactionFinished(_log);
            return true;
        }
        finally
        {
            _ = _mutex.Release();
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _mutex.Dispose();
    }
}
