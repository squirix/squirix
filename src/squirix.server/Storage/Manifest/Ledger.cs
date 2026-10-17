using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Manifest store: the manifest lives in the two slots of <c language="csharp">man-current</c>.</summary>
[Mutable]
internal sealed class Ledger : IDisposable
{
    private static readonly TimeSpan DefaultPublisherStopBudget = TimeSpan.FromSeconds(30);

    private readonly Cache _cache = new();
    private readonly Lock _cacheSync = new();
    private readonly string _currentPath;
    private readonly AsyncLock _gate = new();
    private readonly Publisher _publisher;
    private readonly RetentionWorker _retentionWorker;
    private int _disposed;

    internal Ledger(
        PersistenceOptions options,
        ILogger<Ledger> logger,
        IRetentionCleanupReadinessStatus? retentionReadiness = null,
        IManifestRetentionFailureMetrics? failureMetrics = null,
        IStorageFileOperations? fileOperations = null,
        TimeSpan? publisherStopBudget = null)
    {
        ArgumentNullException.ThrowIfNull(logger);
        var dir = options.DataDir;
        _currentPath = PathEx.Combine(dir, $"{FilePrefixes.Manifest}current");
        _publisher = new Publisher(dir, _currentPath, SetCache, ReadRollBaselineLocked, logger, publisherStopBudget ?? DefaultPublisherStopBudget);
        var retentionContext = new RetentionContext(new RetentionSettings(dir), fileOperations, logger, failureMetrics);
        _retentionWorker = new RetentionWorker(retentionContext, retentionReadiness);
    }

    /// <summary>Gets a value indicating whether manifest retention cleanup is stopped; test seam for the disposal order.</summary>
    internal bool IsRetentionStopped => _retentionWorker.IsStopped;

    /// <summary>Gets the first publish failure that left the slot outcome unknown, or <see langword="null" /> while publishing is usable.</summary>
    internal Exception? PublishFailure => _publisher.Failure;

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        // Retention stops before the publisher drains: a roll committed during the drain would otherwise schedule a cleanup
        // that deletes files in the data directory after this ledger is gone.
        _retentionWorker.Stop();
        _publisher.Dispose();
        _gate.Dispose();
    }

    internal void EnqueueRoll(int currentJournal, ulong nextSequence, Action onSuccess, Action<Exception> onRollFailed) => _publisher.EnqueueRoll(
        currentJournal,
        nextSequence,
        onSuccess,
        onRollFailed,
        _retentionWorker.ScheduleRetentionCleanup);

    internal async Task<State> ReadCurrentOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        using var holder = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        return TryGetCachedCurrent(out var cached) ? cached : await LoadCurrentFromDiskAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Publishes a manifest.</summary>
    /// <param name="manifest">The manifest to store.</param>
    /// <param name="cancellationToken">Cancels waiting for the write.</param>
    /// <returns>A task that completes when the manifest is published.</returns>
    /// <remarks>
    /// A throw usually means the outcome is unknown: the target slot may hold the manifest and the cache may follow it. After a failure from a write that
    /// may have landed the ledger refuses every later publish, including <see cref="WriteSnapshotAsync" /> and <see cref="EnqueueRoll" />, with an
    /// <see cref="InvalidOperationException" /> that carries the first failure (see <see cref="PublishFailure" />); reads keep working. Nothing was written
    /// when the wait was canceled, the encoder rejected the manifest, or the existing file could not be opened or read.
    /// </remarks>
    internal async Task WriteAsync(State manifest, CancellationToken cancellationToken = default)
    {
        using var holder = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        await _publisher.PublishAsync(manifest, cancellationToken).ConfigureAwait(false);
        _retentionWorker.ScheduleRetentionCleanup(manifest);
    }

    /// <summary>Publishes a snapshot reference over the manifest the publisher holds when the write runs.</summary>
    /// <param name="snapshot">The snapshot reference with the journal segment and next sequence floors it was captured at.</param>
    /// <param name="cancellationToken">Cancels waiting for the write.</param>
    /// <returns>A task that completes when the manifest is published.</returns>
    /// <remarks>Fails like <see cref="WriteAsync" />: a throw from a write that may have landed leaves the outcome unknown and latches the ledger.</remarks>
    internal async Task WriteSnapshotAsync(State snapshot, CancellationToken cancellationToken = default)
    {
        using var holder = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        // Warms the cache the publisher merges over; an unloaded cache would make it fall back to the slots on disk.
        if (!TryGetCachedCurrent(out _))
            _ = await LoadCurrentFromDiskAsync(cancellationToken).ConfigureAwait(false);

        await _publisher.PublishSnapshotAsync(snapshot, cancellationToken).ConfigureAwait(false);
        _retentionWorker.ScheduleRetentionCleanup(snapshot);
    }

    private async Task<State> LoadCurrentFromDiskAsync(CancellationToken cancellationToken)
    {
        await _publisher.EnsureDataDirectoryExistsAsync(cancellationToken).ConfigureAwait(false);

        var selection = SlotFile.Read(_currentPath);
        if (selection.Sequence == 0UL)
            return selection.State;

        SetCache(selection.State, selection.Sequence);
        return TryGetCachedCurrent(out var current) ? current : selection.State;
    }

    private State? ReadRollBaselineLocked()
    {
        lock (_cacheSync)
            return _cache.IsInitialized ? _cache.Current : null;
    }

    private void SetCache(State manifest, ulong sequence)
    {
        lock (_cacheSync)
        {
            if (_cache.IsInitialized && sequence <= _cache.Sequence)
                return;

            _cache.Set(manifest, sequence);
        }
    }

    private bool TryGetCachedCurrent(out State manifest)
    {
        lock (_cacheSync)
        {
            if (!_cache.IsInitialized)
            {
                manifest = new State();
                return false;
            }

            manifest = _cache.Current;
            return true;
        }
    }

    private sealed class Cache
    {
        internal State Current { get; private set; } = new();

        internal bool IsInitialized { get; private set; }

        internal ulong Sequence { get; private set; }

        internal void Set(State manifest, ulong sequence)
        {
            Current = manifest;
            Sequence = sequence;
            IsInitialized = true;
        }
    }

    private sealed class Publisher : IDisposable
    {
        private readonly string _dir;
        private readonly Func<State?> _rbl;
        private readonly Action<State, ulong> _setCache;
        private readonly SlotWriter _slotWriter;
        private readonly SingleConsumerWorker<WorkItemBase> _worker;
        private bool _dirCreated;
        private Exception? _failure;

        internal Publisher(
            string dir,
            string path,
            Action<State, ulong> setCache,
            Func<State?> rbl,
            ILogger logger,
            TimeSpan stopBudget)
        {
            _dir = dir;
            _setCache = setCache;
            _rbl = rbl;
            _slotWriter = new SlotWriter(path);

            // A manifest write stuck on disk must not hang node shutdown: disposal waits within the budget, and on expiry the queued writes and
            // rolls are refused and the worker thread is leaked loudly.
            _worker = new SingleConsumerWorker<WorkItemBase>(
                work => work.Execute(this),
                static (work, ex) => work.OnFailure?.Invoke(ex),
                stopBudget,
                () => ServerLog.ManifestPublisherLeakedOnShutdownTimeout(logger, stopBudget));
        }

        internal Exception? Failure => Volatile.Read(ref _failure);

        public void Dispose()
        {
            _worker.Dispose();
            _slotWriter.Dispose();
        }

        internal void EnqueueRoll(int currentJournal, ulong nextSequence, Action onSuccess, Action<Exception> onRollFailed, Action<State> onCommitted) =>
            _worker.Post(new RollItem(currentJournal, nextSequence, onSuccess, onCommitted) { OnFailure = onRollFailed });

        internal async Task EnsureDataDirectoryExistsAsync(CancellationToken cancellationToken)
        {
            if (_dirCreated)
                return;

            _ = await DirectoryEx.CreateDirectoryAsync(_dir, cancellationToken: cancellationToken).ConfigureAwait(false);
            _dirCreated = true;
        }

        internal async Task PublishAsync(State manifest, CancellationToken cancellationToken)
        {
            await EnsureDataDirectoryExistsAsync(cancellationToken).ConfigureAwait(false);
            await _worker.EnqueueAsync(new WriteItem(manifest)).ConfigureAwait(false);
        }

        internal async Task PublishSnapshotAsync(State snapshot, CancellationToken cancellationToken)
        {
            await EnsureDataDirectoryExistsAsync(cancellationToken).ConfigureAwait(false);
            await _worker.EnqueueAsync(new SnapshotWriteItem(snapshot)).ConfigureAwait(false);
        }

        private void Process(RollItem rollItem)
        {
            if (!_dirCreated)
            {
                _ = Directory.CreateDirectory(_dir);
                _dirCreated = true;
            }

            var manifest = new State
            {
                CurrentJournal = rollItem.CurrentJournal,
                NextSequence = rollItem.NextSequence,
                LastSnapshot = ReadBaseline().LastSnapshot,
            };
            WriteManifest(manifest);
            rollItem.OnSuccess?.Invoke();
            rollItem.OnCommitted?.Invoke(manifest);
        }

        private void Process(WriteItem work) => WriteManifest(work.Manifest);

        private void Process(SnapshotWriteItem work)
        {
            // A roll queued ahead of this write has already moved the cache, so the floors never move the journal pointer or the
            // next sequence backward.
            var current = ReadBaseline();
            var snapshot = work.Snapshot;
            WriteManifest(
                new State
                {
                    CurrentJournal = Math.Max(current.CurrentJournal, snapshot.CurrentJournal),
                    NextSequence = Math.Max(current.NextSequence, snapshot.NextSequence),
                    LastSnapshot = snapshot.LastSnapshot,
                });
        }

        /// <summary>Reads the manifest a publish builds on; an unloaded cache falls back to the slots on disk, so a publish never drops what an unread manifest names.</summary>
        /// <returns>The cached manifest, or the manifest the slots hold.</returns>
        private State ReadBaseline() => _rbl() ?? _slotWriter.ReadCurrentOrDefault();

        private void WriteManifest(State manifest)
        {
            var failure = Failure;
            if (failure != null)
                throw new InvalidOperationException("The manifest publisher refuses every publish after an earlier publish failed.", failure);

            try
            {
                var sequence = _slotWriter.Publish(manifest);

                // The slot already holds this manifest, so memory follows it before the mirror write that may still fail.
                _setCache(manifest, sequence);
                _slotWriter.Mirror();
            }
            catch (Exception ex) when (_slotWriter.OutcomeUnknown)
            {
                // After a write that may have landed the slots may hold the manifest, so no later publish may report success. A failure before
                // any write (open, read, encode) leaves the publisher usable. A dispose racing the write may latch too, which is harmless: a disposed writer refuses every later publish. Only this worker thread sets the latch.
                Volatile.Write(ref _failure, Failure ?? ex);
                throw;
            }
        }

        [Immutable]
        private sealed class RollItem : WorkItemBase
        {
            internal RollItem(int currentJournal, ulong nextSequence, Action? onSuccess, Action<State>? onCommitted)
            {
                CurrentJournal = currentJournal;
                NextSequence = nextSequence;
                OnSuccess = onSuccess;
                OnCommitted = onCommitted;
            }

            internal int CurrentJournal { get; }

            internal ulong NextSequence { get; }

            internal Action<State>? OnCommitted { get; }

            internal Action? OnSuccess { get; }

            internal override void Execute(Publisher publisher) => publisher.Process(this);
        }

        [Immutable]
        private abstract class WorkItemBase
        {
            internal Action<Exception>? OnFailure { get; init; }

            internal abstract void Execute(Publisher publisher);
        }

        [Immutable]
        private sealed class WriteItem : WorkItemBase
        {
            internal WriteItem(State manifest)
            {
                Manifest = manifest;
            }

            internal State Manifest { get; }

            internal override void Execute(Publisher publisher) => publisher.Process(this);
        }

        [Immutable]
        private sealed class SnapshotWriteItem : WorkItemBase
        {
            internal SnapshotWriteItem(State snapshot)
            {
                Snapshot = snapshot;
            }

            internal State Snapshot { get; }

            internal override void Execute(Publisher publisher) => publisher.Process(this);
        }
    }
}
