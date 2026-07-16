using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Utils;
using Index = Squirix.Server.Storage.Manifest.Index;

namespace Squirix.Server.Storage;

/// <summary>Manifest store (<c>.bmqx</c> files and fixed-size <c>man-current</c> pointer).</summary>
[SuppressMessage(
    "AsyncUsage",
    "MA0045:Use await instead of GetResult()",
    Justification = "Blocking APIs run on the dedicated journal I/O thread without a synchronization context.")]
[SuppressMessage(
    "Usage",
    "VSTHRD002:Avoid problematic synchronous waits",
    Justification = "Blocking APIs run on the dedicated journal I/O thread without a synchronization context.")]
internal sealed class ManifestStore : IDisposable
{
    private readonly Index _index = new();
    private readonly Lock _cacheSync = new();
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly string _currentPath;
    private readonly IndexAllocator _allocator;
    private readonly Publisher _publisher;
    private readonly RetentionWorker _retentionWorker;
    private bool _disposed;

    public ManifestStore(
        PersistenceOptions options,
        ILogger<ManifestStore>? logger = null,
        IRetentionCleanupReadinessStatus? retentionReadiness = null,
        IManifestRetentionFailureMetrics? failureMetrics = null)
        : this(options, logger, retentionReadiness, new FileOperations(), failureMetrics)
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

    public ManifestStore(
        PersistenceOptions options,
        ILogger<ManifestStore>? logger,
        IRetentionCleanupReadinessStatus? retentionReadiness,
        IStorageFileOperations fileOperations,
        IManifestRetentionFailureMetrics? failureMetrics = null)
    {
        var dataDir = options.DataDir;
        _currentPath = PathEx.Combine(dataDir, $"{FilePrefixes.Manifest}current");
        _allocator = new IndexAllocator(
            dataDir,
            _currentPath,
            PathEx.Combine(dataDir, FilePrefixes.Manifest),
            $"{FilePrefixes.Manifest}*{FileExtensions.Manifest}",
            ReadCurrentIndexForInit);
        _publisher = new Publisher(
            dataDir,
            _currentPath,
            _allocator,
            SetCache,
            ReadRollBaselineLocked);
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
            await _allocator.EnsureNextManifestIndexInitializedAsync(cancellationToken).ConfigureAwait(false);
            var nextIndex = _allocator.IncrementNextManifestIndex();
            await _publisher.PublishCoreAsync(manifest, nextIndex, cancellationToken).ConfigureAwait(false);
            _retentionWorker.ScheduleRetentionCleanup(manifest);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    internal void PublishRollBlocking(int currentJournal, ulong nextSequence)
    {
        var nextIndex = _allocator.AllocateNextManifestIndex();
        var manifest = _publisher.PublishRollCoreBlocking(currentJournal, nextSequence, nextIndex);
        _retentionWorker.ScheduleRetentionCleanup(manifest);
    }

    internal State ReadCurrentOrDefaultBlocking() => ReadCurrentOrDefaultAsync(CancellationToken.None).GetAwaiter().GetResult();

    private async Task<State> LoadCurrentFromDiskAsync(CancellationToken cancellationToken)
    {
        await _publisher.EnsureDataDirectoryExistsAsync(cancellationToken).ConfigureAwait(false);

        if (!File.Exists(_currentPath))
            return new State();

        var pointerBytes = await File.ReadAllBytesAsync(_currentPath, cancellationToken).ConfigureAwait(false);
        if (!Pointer.IsValidPointer(pointerBytes))
            throw new InvalidDataException($"Manifest current pointer is invalid: {_currentPath}");

        var index = Pointer.Read(pointerBytes);
        var path = _allocator.BuildManifestFilePath(index);
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        var manifest = FileCodec.Decode(bytes);
        SetCache(manifest, index);
        _allocator.SeedNextManifestIndex(index);
        return manifest;
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
}
