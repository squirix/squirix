using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Compaction;

/// <summary>
/// On-demand compaction runner. Uses semaphore to ensure only one compaction runs at a time.
/// Safe to co-exist with a periodic compaction service if that service also guards concurrency.
/// </summary>
internal sealed class JournalCompactionController : IDisposable
{
    private readonly IJournalCoordinator _journalWriter;
    private readonly ILogger<JournalCompactionController> _log;
    private readonly Ledger _manifestStore;
    private readonly SemaphoreSlim _mutex = new(1, 1);
    private readonly PersistenceOptions _opt;
    private readonly ISnapshotReader _snapshotReader;
    private bool _disposed;

    internal JournalCompactionController(
        PersistenceOptions opt,
        Ledger manifestStore,
        ISnapshotReader snapshotReader,
        IJournalCoordinator journalWriter,
        ILogger<JournalCompactionController> log)
    {
        _opt = opt;
        _manifestStore = manifestStore;
        _snapshotReader = snapshotReader;
        _journalWriter = journalWriter;
        _log = log;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _mutex.Dispose();
    }

    internal async Task<bool> TryTriggerNowAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!await _mutex.WaitAsync(0, cancellationToken).ConfigureAwait(false))
            return false;

        try
        {
            var manifest = await _manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            var snapIdx = manifest.LastSnapshot?.Index ?? 0;
            LogManager.ManualCompactionStart(_log, snapIdx);
            await _journalWriter.ExecuteMaintenanceExclusiveAsync(ct => new ValueTask(JournalCompactor.CompactAsync(_opt, _manifestStore, _snapshotReader, ct)), cancellationToken)
                                .ConfigureAwait(false);
            LogManager.ManualCompactionFinished(_log);
            return true;
        }
        finally
        {
            _ = _mutex.Release();
        }
    }
}
