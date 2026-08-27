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

/// <summary>Manifest store (<c language="csharp">.bmqx</c> files and fixed-size <c language="csharp">man-current</c> pointer).</summary>
[Mutable]
internal sealed class Ledger : IDisposable
{
    private readonly IndexAllocator _allocator;
    private readonly Lock _cacheSync = new();
    private readonly string _currentPath;
    private readonly AsyncLock _gate = new();
    private readonly Index _index = new();
    private readonly Publisher _publisher;
    private readonly RetentionWorker _retentionWorker;
    private int _disposed;

    internal Ledger(
        PersistenceOptions options,
        ILogger<Ledger>? logger = null,
        IRetentionCleanupReadinessStatus? retentionReadiness = null,
        IManifestRetentionFailureMetrics? failureMetrics = null,
        IStorageFileOperations? fileOperations = null)
    {
        var dir = options.DataDir;
        _currentPath = PathEx.Combine(dir, $"{FilePrefixes.Manifest}current");
        _allocator = new IndexAllocator(dir, _currentPath, PathEx.Combine(dir, FilePrefixes.Manifest), $"{FilePrefixes.Manifest}*{FileExtensions.Manifest}", ReadCurrentIndex);
        _publisher = new Publisher(dir, _currentPath, _allocator, SetCache, ReadRollBaselineLocked);
        var retentionSettings = new RetentionSettings(
            dir,
            options.ManifestRetentionCount > 0 ? options.ManifestRetentionCount : 3,
            options.SnapshotRetentionCount > 0 ? options.SnapshotRetentionCount : 3,
            $"{FilePrefixes.Manifest}*{FileExtensions.Manifest}");
        var retentionContext = new RetentionContext(retentionSettings, fileOperations, logger, IndexAllocator.ParseManifestIndex, failureMetrics);
        _retentionWorker = new RetentionWorker(retentionContext, retentionReadiness);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

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
        if (TryGetCachedCurrent(out var cached))
            return cached;

        return await LoadCurrentFromDiskAsync(cancellationToken).ConfigureAwait(false);
    }

    internal async Task WriteAsync(State manifest, CancellationToken cancellationToken = default)
    {
        using var holder = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        _allocator.EnsureNextManifestIndexInitialized(cancellationToken);
        await _publisher.PublishAsync(manifest, cancellationToken).ConfigureAwait(false);
        _retentionWorker.ScheduleRetentionCleanup(manifest);
    }

    private async Task<State> LoadCurrentFromDiskAsync(CancellationToken cancellationToken)
    {
        await _publisher.EnsureDataDirectoryExistsAsync(cancellationToken).ConfigureAwait(false);

        if (!File.Exists(_currentPath))
            return new State();

        var index = PointerFile.ReadIndex(_currentPath);
        var path = _allocator.BuildManifestFilePath(index);
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        var manifest = FileCodec.Decode(bytes);
        SetCache(manifest, index);
        _allocator.SeedNextManifestIndex(index);
        return TryGetCachedCurrent(out var current) ? current : manifest;
    }

    private int? ReadCurrentIndex()
    {
        lock (_cacheSync)
            return _index.IsInitialized ? _index.CurrentIndex : null;
    }

    private (State Previous, ReadOnlyMemory<byte> SnapshotPathUtf8) ReadRollBaselineLocked()
    {
        lock (_cacheSync)
        {
            if (!_index.IsInitialized)
                return (new State(), ReadOnlyMemory<byte>.Empty);

            return (_index.Current, _index.SnapshotPathUtf8);
        }
    }

    private void SetCache(State manifest, int index)
    {
        lock (_cacheSync)
        {
            if (_index.IsInitialized && index <= _index.CurrentIndex)
                return;

            _index.Set(manifest, index);
        }
    }

    private bool TryGetCachedCurrent(out State manifest)
    {
        lock (_cacheSync)
        {
            if (!_index.IsInitialized)
            {
                manifest = new State();
                return false;
            }

            manifest = _index.Current;
            return true;
        }
    }

    private sealed class Index
    {
        internal State Current { get; private set; } = new();

        internal int CurrentIndex { get; private set; }

        internal bool IsInitialized { get; private set; }

        internal ReadOnlyMemory<byte> SnapshotPathUtf8 { get; private set; } = ReadOnlyMemory<byte>.Empty;

        internal void Set(State manifest, int index)
        {
            Current = manifest;
            CurrentIndex = index;
            SnapshotPathUtf8 = manifest.LastSnapshot?.Path is { Length: > 0 } path ? BufferEx.Utf8ToOwned(path) : ReadOnlyMemory<byte>.Empty;
            IsInitialized = true;
        }
    }

    private sealed class Publisher : IDisposable
    {
        private const int DefaultEncodeBufferCapacity = 256;

        private readonly IndexAllocator _allocator;
        private readonly byte[] _currentPointerBuffer = new byte[Pointer.Size];

        private readonly PersistentPointerWriter _currentPointerWriter;

        private readonly string _dir;
        private readonly Func<(State Previous, ReadOnlyMemory<byte> SnapshotPathUtf8)> _rbl;
        private readonly Action<State, int> _setCache;
        private readonly SingleConsumerWorker<WorkItemBase> _worker;
        private bool _dirCreated;
        private byte[] _encodeBuffer = new byte[DefaultEncodeBufferCapacity];

        internal Publisher(string dir, string path, IndexAllocator allocator, Action<State, int> setCache, Func<(State Previous, ReadOnlyMemory<byte> SnapshotPathUtf8)> rbl)
        {
            _dir = dir;
            _allocator = allocator;
            _setCache = setCache;
            _rbl = rbl;
            _currentPointerWriter = new PersistentPointerWriter(path);
            _worker = new SingleConsumerWorker<WorkItemBase>(work => work.Execute(this), static (work, ex) => work.OnFailure?.Invoke(ex));
        }

        public void Dispose() => _worker.Dispose();

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
            var encodedLength = FileCodec.ComputeEncodedLength(manifest);
            await _worker.EnqueueAsync(new WriteItem(manifest, encodedLength)).ConfigureAwait(false);
        }

        private void EnsureEncodeBufferCapacity(int encodedLength)
        {
            if (_encodeBuffer.Length >= encodedLength)
                return;

            _encodeBuffer = new byte[Math.Max(encodedLength, _encodeBuffer.Length * 2)];
        }

        private void Process(RollItem rollItem)
        {
            if (!_dirCreated)
            {
                _ = Directory.CreateDirectory(_dir);
                _dirCreated = true;
            }

            var nextIndex = _allocator.AllocateNextManifestIndex();
            var (previous, snapshotPathUtf8) = _rbl();
            var format = previous.Format == 0 ? 1 : previous.Format;
            var snapshot = previous.LastSnapshot;

            var encodedLength = FileCodec.ComputeRollEncodedLength(snapshot, snapshotPathUtf8.Length);
            EnsureEncodeBufferCapacity(encodedLength);
            _ = FileCodec.WriteRollEncoded(format, rollItem.CurrentJournal, rollItem.NextSequence, snapshot, snapshotPathUtf8.Span, _encodeBuffer.AsSpan(0, encodedLength));

            var targetPath = _allocator.BuildManifestFilePath(nextIndex);
            Pointer.Write(_currentPointerBuffer, nextIndex);
            FileDurability.WriteManifestRollBlocking(targetPath, _encodeBuffer.AsSpan(0, encodedLength), _currentPointerWriter, _currentPointerBuffer);

            var manifest = new State
            {
                Format = format,
                CurrentJournal = rollItem.CurrentJournal,
                NextSequence = rollItem.NextSequence,
                LastSnapshot = snapshot,
            };
            _setCache(manifest, nextIndex);
            rollItem.OnSuccess?.Invoke();
            rollItem.OnCommitted?.Invoke(manifest);
        }

        private void Process(WriteItem work)
        {
            var nextIndex = _allocator.AllocateNextManifestIndex();
            var targetPath = _allocator.BuildManifestFilePath(nextIndex);
            EnsureEncodeBufferCapacity(work.EncodedLength);
            FileCodec.WriteEncoded(work.Manifest, _encodeBuffer.AsSpan(0, work.EncodedLength));
            FileDurability.WriteManifestDataFileBlocking(targetPath, _encodeBuffer.AsSpan(0, work.EncodedLength));
            Pointer.Write(_currentPointerBuffer, nextIndex);
            FileDurability.WriteCurrentPointerBlocking(_currentPointerWriter, _currentPointerBuffer);
            _setCache(work.Manifest, nextIndex);
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
            internal WriteItem(State manifest, int encodedLength)
            {
                Manifest = manifest;
                EncodedLength = encodedLength;
            }

            internal int EncodedLength { get; }

            internal State Manifest { get; }

            internal override void Execute(Publisher publisher) => publisher.Process(this);
        }
    }
}
