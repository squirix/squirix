using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Compaction;

/// <summary>
/// On-demand compaction runner. Uses semaphore to ensure only one compaction runs at a time.
/// Safe to co-exist with a periodic compaction service if that service also guards concurrency.
/// </summary>
internal sealed class JournalCompactionController : IDisposable
{
    private readonly IJournalCoordinator _journalWriter;
    private readonly AsyncLock _lock = new();
    private readonly ILogger<JournalCompactionController> _log;
    private readonly PersistenceOptions _opt;
    private readonly ISnapshotReader _reader;
    private readonly Ledger _store;
    private int _disposed;

    internal JournalCompactionController(PersistenceOptions opt, Ledger store, ISnapshotReader reader, IJournalCoordinator journalWriter, ILogger<JournalCompactionController> log)
    {
        _opt = opt;
        _store = store;
        _reader = reader;
        _journalWriter = journalWriter;
        _log = log;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        _lock.Dispose();
    }

    internal async Task<bool> TryTriggerAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (!_lock.TryLock(out var lockGuard, cancellationToken))
            return false;
        using (lockGuard)
        {
            var manifest = await _store.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            LogManager.ManualCompactionStart(_log, manifest.LastSnapshot?.Index ?? 0);
            await _journalWriter.ExecuteMaintenanceExclusiveAsync(ct => new ValueTask(JournalCompactor.CompactAsync(_opt, _store, _reader, ct)), cancellationToken)
                                .ConfigureAwait(false);
            LogManager.ManualCompactionFinished(_log);
            return true;
        }
    }
}
