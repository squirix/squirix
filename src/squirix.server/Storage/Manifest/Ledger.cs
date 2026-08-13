using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Manifest store (<c>.bmqx</c> files and fixed-size <c>man-current</c> pointer).</summary>
internal sealed class Ledger : IDisposable
{
    private readonly IndexAllocator _allocator;
    private readonly Lock _cacheSync = new();
    private readonly string _currentPath;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly Index _index = new();
    private readonly Publisher _publisher;
    private readonly RetentionWorker _retentionWorker;
    private bool _disposed;

    internal Ledger(
        PersistenceOptions options,
        ILogger<Ledger>? logger = null,
        IRetentionCleanupReadinessStatus? retentionReadiness = null,
        IManifestRetentionFailureMetrics? failureMetrics = null,
        IStorageFileOperations? fileOperations = null)
    {
        var dataDir = options.DataDir;
        _currentPath = PathEx.Combine(dataDir, $"{FilePrefixes.Manifest}current");
        _allocator = new IndexAllocator(
            dataDir,
            _currentPath,
            PathEx.Combine(dataDir, FilePrefixes.Manifest),
            $"{FilePrefixes.Manifest}*{FileExtensions.Manifest}",
            ReadCurrentIndexForInit);
        _publisher = new Publisher(dataDir, _currentPath, _allocator, SetCache, ReadRollBaselineLocked);
        var retentionContext = new RetentionContext(
            new RetentionSettings(
                dataDir,
                options.ManifestRetentionCount > 0 ? options.ManifestRetentionCount : 3,
                options.SnapshotRetentionCount > 0 ? options.SnapshotRetentionCount : 3,
                $"{FilePrefixes.Manifest}*{FileExtensions.Manifest}"),
            fileOperations,
            logger,
            IndexAllocator.ParseManifestIndex,
            failureMetrics ?? NoOpManifestRetentionFailureMetrics.Instance);
        _retentionWorker = new RetentionWorker(retentionContext, retentionReadiness);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _publisher.Dispose();
        _gate.Dispose();
        _disposed = true;
    }

    internal void PublishRollBlocking(int currentJournal, ulong nextSequence)
    {
        var nextIndex = _allocator.AllocateNextManifestIndex();
        var manifest = _publisher.PublishRollCoreBlocking(currentJournal, nextSequence, nextIndex);
        _retentionWorker.ScheduleRetentionCleanup(manifest);
    }

    internal async Task<State> ReadCurrentOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (TryGetCachedCurrent(out var cached))
                return cached;

            return await LoadCurrentFromDiskAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    internal async Task WriteAsync(State manifest, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _allocator.EnsureNextManifestIndexInitialized(cancellationToken);
            var nextIndex = _allocator.IncrementNextManifestIndex();
            await _publisher.PublishCoreAsync(manifest, nextIndex, cancellationToken).ConfigureAwait(false);
            _retentionWorker.ScheduleRetentionCleanup(manifest);
        }
        finally
        {
            _ = _gate.Release();
        }
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
        return manifest;
    }

    private int? ReadCurrentIndexForInit()
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
            _index.Set(manifest, index);
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

    private sealed class NoOpManifestRetentionFailureMetrics : IManifestRetentionFailureMetrics
    {
        internal static NoOpManifestRetentionFailureMetrics Instance { get; } = new();

        public void RecordDeleteFailure(string artifactKind, string outcome)
        {
            _ = artifactKind;
            _ = outcome;
        }
    }

    private sealed class Publisher : IDisposable
    {
        private const int DefaultEncodeBufferCapacity = 256;

        private static readonly Action<object?> WritePublishedManifestBlockingCallback = static state =>
        {
            if (state is Publisher publisher)
                publisher.WritePublishedManifestBlocking();
        };

        private readonly IndexAllocator _allocator;
        private readonly byte[] _currentPointerBuffer = new byte[Pointer.Size];
        private readonly PersistentPointerWriter _currentPointerWriter;

        private readonly string _dataDir;
        private readonly Func<(State Previous, ReadOnlyMemory<byte> SnapshotPathUtf8)> _readRollBaselineLocked;
        private readonly Action<State, int> _setCache;
        private bool _dataDirEnsured;
        private byte[] _encodeBuffer = new byte[DefaultEncodeBufferCapacity];
        private PublishWork? _publishWork;

        internal Publisher(
            string dataDir,
            string currentPath,
            IndexAllocator allocator,
            Action<State, int> setCache,
            Func<(State Previous, ReadOnlyMemory<byte> SnapshotPathUtf8)> readRollBaselineLocked)
        {
            _dataDir = dataDir;
            _allocator = allocator;
            _setCache = setCache;
            _readRollBaselineLocked = readRollBaselineLocked;
            _currentPointerWriter = new PersistentPointerWriter(currentPath);
        }

        public void Dispose() => _currentPointerWriter.Dispose();

        internal async Task EnsureDataDirectoryExistsAsync(CancellationToken cancellationToken)
        {
            if (_dataDirEnsured)
                return;

            _ = await DirectoryEx.CreateDirectoryAsync(_dataDir, cancellationToken: cancellationToken).ConfigureAwait(false);
            _dataDirEnsured = true;
        }

        internal async Task PublishCoreAsync(State manifest, int nextIndex, CancellationToken cancellationToken)
        {
            await EnsureDataDirectoryExistsAsync(cancellationToken).ConfigureAwait(false);

            var targetPath = _allocator.BuildManifestFilePath(nextIndex);
            var encodedLength = FileCodec.ComputeEncodedLength(manifest);
            EnsureEncodeBufferCapacity(encodedLength);

            FileCodec.WriteEncoded(manifest, _encodeBuffer.AsSpan(0, encodedLength));

            _publishWork = new PublishWork(targetPath, encodedLength, nextIndex);

            await Task.Factory.StartNew(WritePublishedManifestBlockingCallback, this, cancellationToken, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default)
                      .ConfigureAwait(false);

            _setCache(manifest, nextIndex);
        }

        internal State PublishRollCoreBlocking(int currentJournal, ulong nextSequence, int nextIndex)
        {
            if (!_dataDirEnsured)
            {
                _ = Directory.CreateDirectory(_dataDir);
                _dataDirEnsured = true;
            }

            var (previous, snapshotPathUtf8) = _readRollBaselineLocked();
            var format = previous.Format is 0 ? 1 : previous.Format;
            var snapshot = previous.LastSnapshot;

            var encodedLength = FileCodec.ComputeRollEncodedLength(snapshot, snapshotPathUtf8.Length);
            EnsureEncodeBufferCapacity(encodedLength);

            _ = FileCodec.WriteRollEncoded(format, currentJournal, nextSequence, snapshot, snapshotPathUtf8.Span, _encodeBuffer.AsSpan(0, encodedLength));

            var targetPath = _allocator.BuildManifestFilePath(nextIndex);
            Pointer.Write(_currentPointerBuffer, nextIndex);
            FileDurability.WriteManifestRollBlocking(targetPath, _encodeBuffer.AsSpan(0, encodedLength), _currentPointerWriter, _currentPointerBuffer);

            var manifest = new State
            {
                Format = format,
                CurrentJournal = currentJournal,
                NextSequence = nextSequence,
                LastSnapshot = snapshot,
            };
            _setCache(manifest, nextIndex);
            return manifest;
        }

        private void EnsureEncodeBufferCapacity(int encodedLength)
        {
            if (_encodeBuffer.Length >= encodedLength)
                return;

            _encodeBuffer = new byte[Math.Max(encodedLength, _encodeBuffer.Length * 2)];
        }

        private void UpdateCurrentPointerBlocking(int manifestIndex)
        {
            Pointer.Write(_currentPointerBuffer, manifestIndex);
            FileDurability.WriteCurrentPointerBlocking(_currentPointerWriter, _currentPointerBuffer);
        }

        private void WritePublishedManifestBlocking()
        {
            if (_publishWork is not { } publishWork)
                throw new InvalidOperationException("Publish work was not initialized.");

            FileDurability.WriteManifestDataFileBlocking(publishWork.TargetPath, _encodeBuffer.AsSpan(0, publishWork.EncodedLength));
            UpdateCurrentPointerBlocking(publishWork.ManifestIndex);
        }

        private sealed record PublishWork(string TargetPath, int EncodedLength, int ManifestIndex);
    }
}
