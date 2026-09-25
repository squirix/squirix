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
/// Every run holds the snapshot coordinator's compaction reservation, like the periodic compaction service, so it never overlaps a
/// snapshot in flight or another compaction.
/// </summary>
internal sealed class JournalCompactionController : IDisposable
{
    private readonly AsyncLock _lock = new();
    private readonly ILogger<JournalCompactionController> _log;
    private readonly IExclusiveMaintenanceExecutor _maintenance;
    private readonly PersistenceOptions _opt;
    private readonly ISnapshotReader _reader;
    private readonly Coordinator _snapshots;
    private readonly Ledger _store;
    private int _disposed;

    internal JournalCompactionController(
        PersistenceOptions opt,
        Ledger store,
        ISnapshotReader reader,
        IExclusiveMaintenanceExecutor maintenance,
        Coordinator snapshots,
        ILogger<JournalCompactionController> log)
    {
        _opt = opt;
        _store = store;
        _reader = reader;
        _maintenance = maintenance;
        _snapshots = snapshots;
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
            // A compaction publishing between a snapshot cut and its manifest write would delete the segments the snapshot replays from:
            // skip while a snapshot (or the periodic compaction) holds the reservation, as the periodic compaction service does.
            if (!_snapshots.TryEnterCompaction())
                return false;

            try
            {
                var manifest = await _store.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
                LogManager.ManualCompactionStart(_log, manifest.LastSnapshot?.Index ?? 0);
                await _maintenance.ExecuteMaintenanceExclusiveAsync(ct => new ValueTask(JournalCompactor.CompactAsync(_opt, _store, _reader, ct)), cancellationToken)
                                  .ConfigureAwait(false);
                LogManager.ManualCompactionFinished(_log);
                return true;
            }
            finally
            {
                _snapshots.ExitCompaction();
            }
        }
    }
}
