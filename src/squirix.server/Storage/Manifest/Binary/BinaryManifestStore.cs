using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest.Binary;

/// <summary>Binary manifest store (<c>.bmqx</c> files and fixed-size <c>man-current</c> pointer).</summary>
[SuppressMessage("Design", "MA0180:Use ILogger<T> with the current class type", Justification = "Retention logs use the ManifestStore category for stable observability.")]
internal sealed class BinaryManifestStore : IManifestStore
{
    private const int DefaultEncodeBufferCapacity = 256;
    private readonly BinaryManifestState _cache = new();

    private readonly string _currentPath;
    private readonly byte[] _currentPointerBuffer = new byte[BinaryManifestPointer.Size];
    private readonly BinaryManifestPersistentPointerWriter _currentPointerWriter;
    private readonly string _dataDir;
    private readonly string _manifestFileNamePrefix;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly ManifestRetentionContext _retentionContext;
    private readonly Lock _cacheSync = new();
    private readonly Lock _nextIndexInitLock = new();
    private readonly IRetentionCleanupReadinessStatus? _retentionReadiness;
    private byte[] _encodeBuffer = new byte[DefaultEncodeBufferCapacity];
    private bool _dataDirEnsured;
    private int _nextManifestIndex;
    private volatile bool _nextIndexInitialized;
    private volatile ManifestState? _pendingRetentionManifest;
    private int _retentionWorkerScheduled;

    public BinaryManifestStore(PersistenceOptions options, ILogger<ManifestStore>? logger = null, IRetentionCleanupReadinessStatus? retentionReadiness = null)
        : this(options, logger, retentionReadiness, new StorageFileOperations())
    {
    }

    internal BinaryManifestStore(
        PersistenceOptions options,
        ILogger<ManifestStore>? logger,
        IRetentionCleanupReadinessStatus? retentionReadiness,
        IStorageFileOperations fileOperations)
    {
        _dataDir = options.DataDir;
        _currentPath = PathEx.Combine(_dataDir, $"{StorageFilePrefixes.Manifest}current");
        _currentPointerWriter = new BinaryManifestPersistentPointerWriter(_currentPath);
        _manifestFileNamePrefix = PathEx.Combine(_dataDir, StorageFilePrefixes.Manifest);
        _retentionReadiness = retentionReadiness;
        _retentionContext = new ManifestRetentionContext(
            _dataDir,
            options.ManifestRetentionCount > 0 ? options.ManifestRetentionCount : 3,
            options.SnapshotRetentionCount > 0 ? options.SnapshotRetentionCount : 3,
            fileOperations,
            logger,
            $"{StorageFilePrefixes.Manifest}*{StorageFileExtensions.BinaryManifest}",
            TryParseBinaryManifestIndex);
    }

    public void Dispose()
    {
        _currentPointerWriter.Dispose();
        _gate.Dispose();
    }

    [SuppressMessage("AsyncUsage", "MA0045:Use await instead of GetResult()", Justification = "Blocking API for the dedicated journal I/O thread.")]
    [SuppressMessage(
        "Usage",
        "VSTHRD002:Avoid problematic synchronous waits",
        Justification = "Journal I/O thread has no synchronization context and must observe manifest durability before continuing a segment roll.")]
    public void PublishBlocking(ManifestState manifest)
    {
        _gate.Wait(CancellationToken.None);
        try
        {
            var nextIndex = AllocateNextManifestIndex();
            PublishCoreBlocking(manifest, nextIndex);
            ScheduleRetentionCleanup(manifest);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    [SuppressMessage("AsyncUsage", "MA0045:Use await instead of GetResult()", Justification = "Blocking API for the dedicated journal I/O thread.")]
    [SuppressMessage(
        "Usage",
        "VSTHRD002:Avoid problematic synchronous waits",
        Justification = "Journal I/O thread has no synchronization context and must observe manifest durability before continuing a segment roll.")]
    public void PublishRollBlocking(int currentJournal, ulong nextSequence)
    {
        var nextIndex = AllocateNextManifestIndex();
        var manifest = PublishRollCoreBlocking(currentJournal, nextSequence, nextIndex);
        ScheduleRetentionCleanup(manifest);
    }

    /// <inheritdoc />
    [SuppressMessage(
        "Security",
        "CA3003:Review code for file path injection vulnerabilities",
        Justification = "Manifest path is resolved from a validated CURRENT pointer index under the configured data directory.")]
    public async Task<ManifestState> ReadCurrentOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_cache.IsInitialized)
            {
                lock (_cacheSync)
                    return _cache.Current;
            }

            return await LoadCurrentFromDiskAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    [SuppressMessage("AsyncUsage", "MA0045:Use await instead of GetResult()", Justification = "Blocking API for the dedicated journal I/O thread.")]
    [SuppressMessage(
        "Usage",
        "VSTHRD002:Avoid problematic synchronous waits",
        Justification = "Journal I/O thread has no synchronization context and must observe manifest durability before continuing a segment roll.")]
    public ManifestState ReadCurrentOrDefaultBlocking() => ReadCurrentOrDefaultAsync(CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc />
    public async Task WriteAsync(ManifestState manifest, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var nextIndex = AllocateNextManifestIndex();
            await PublishCoreAsync(manifest, nextIndex, cancellationToken).ConfigureAwait(false);
            ScheduleRetentionCleanup(manifest);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    [SuppressMessage("AsyncUsage", "MA0045:Use await instead of GetResult()", Justification = "Blocking API for the dedicated journal I/O thread.")]
    [SuppressMessage(
        "Usage",
        "VSTHRD002:Avoid problematic synchronous waits",
        Justification = "Journal I/O thread has no synchronization context and must observe manifest durability before continuing a segment roll.")]
    public void WriteBlocking(ManifestState manifest) => WriteAsync(manifest, CancellationToken.None).GetAwaiter().GetResult();

    /// <summary>Writes the first binary manifest during JSON-to-binary migration at the given index.</summary>
    /// <param name="manifest">Manifest state read from the JSON backend.</param>
    /// <param name="manifestIndex">Target binary manifest file index (typically matches the JSON manifest index).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the binary manifest and pointer are durable.</returns>
    internal async Task WriteMigrationInitialAsync(ManifestState manifest, int manifestIndex, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            SeedNextManifestIndex(manifestIndex - 1);
            await PublishCoreAsync(manifest, manifestIndex, cancellationToken).ConfigureAwait(false);
            ScheduleRetentionCleanup(manifest);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    private static int TryParseBinaryManifestIndex(string name)
    {
        if (string.IsNullOrEmpty(name))
            return 0;
        if (!name.StartsWith(StorageFilePrefixes.Manifest, StringComparison.OrdinalIgnoreCase))
            return 0;
        if (!name.EndsWith(StorageFileExtensions.BinaryManifest, StringComparison.OrdinalIgnoreCase))
            return 0;

        var numberPart = name.Substring(StorageFilePrefixes.Manifest.Length, name.Length - StorageFilePrefixes.Manifest.Length - StorageFileExtensions.BinaryManifest.Length);
        return int.TryParse(numberPart, CultureInfo.InvariantCulture, out var n) ? n : 0;
    }

    private int AllocateNextManifestIndex()
    {
        EnsureNextManifestIndexInitialized();
        return Interlocked.Increment(ref _nextManifestIndex);
    }

    private string BuildManifestFilePath(int index) =>
        string.Create(
            _manifestFileNamePrefix.Length + 6 + StorageFileExtensions.BinaryManifest.Length,
            (Prefix: _manifestFileNamePrefix, Index: index),
            static (span, state) =>
            {
                state.Prefix.AsSpan().CopyTo(span);
                var suffix = span.Slice(state.Prefix.Length);
                if (!state.Index.TryFormat(suffix, out var charsWritten, "D6", CultureInfo.InvariantCulture))
                    throw new InvalidOperationException("Manifest index did not fit fixed-width field.");

                StorageFileExtensions.BinaryManifest.AsSpan().CopyTo(suffix.Slice(charsWritten));
            });

    private void EnsureDataDirectoryExists()
    {
        if (_dataDirEnsured)
            return;

        _ = Directory.CreateDirectory(_dataDir);
        _dataDirEnsured = true;
    }

    private void EnsureEncodeBufferCapacity(int encodedLength)
    {
        if (_encodeBuffer.Length >= encodedLength)
            return;

        _encodeBuffer = new byte[Math.Max(encodedLength, _encodeBuffer.Length * 2)];
    }

    private void EnsureNextManifestIndexInitialized()
    {
        if (_nextIndexInitialized)
            return;

        lock (_nextIndexInitLock)
        {
            if (_nextIndexInitialized)
                return;

            _nextManifestIndex = TryReadCurrentIndexForInit() ?? ResolveNextIndexFromDiskLocked() - 1;
            _nextIndexInitialized = true;
        }
    }

    private int? TryReadCurrentIndexForInit()
    {
        lock (_cacheSync)
            return _cache.IsInitialized ? _cache.CurrentIndex : null;
    }

    private void SetCache(ManifestState manifest, int index)
    {
        lock (_cacheSync)
            _cache.Set(manifest, index);
    }

    private (ManifestState Previous, ReadOnlyMemory<byte> SnapshotPathUtf8) ReadRollBaselineLocked()
    {
        lock (_cacheSync)
        {
            if (!_cache.IsInitialized)
                return (new ManifestState(), ReadOnlyMemory<byte>.Empty);

            return (_cache.Current, _cache.SnapshotPathUtf8);
        }
    }

    private async Task<ManifestState> LoadCurrentFromDiskAsync(CancellationToken cancellationToken)
    {
        _ = await DirectoryEx.CreateDirectoryAsync(_dataDir, cancellationToken: cancellationToken).ConfigureAwait(false);
        _dataDirEnsured = true;

        if (!File.Exists(_currentPath))
            return new ManifestState();

        var pointerBytes = await File.ReadAllBytesAsync(_currentPath, cancellationToken).ConfigureAwait(false);
        if (!BinaryManifestPointer.IsBinaryPointer(pointerBytes))
            throw new InvalidDataException($"Manifest current pointer is not a binary manifest pointer: {_currentPath}");

        var index = BinaryManifestPointer.Read(pointerBytes);
        var path = BuildManifestFilePath(index);
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        var manifest = BinaryManifestCodec.Decode(bytes);
        SetCache(manifest, index);
        SeedNextManifestIndex(index);
        return manifest;
    }

    private void PublishCoreBlocking(ManifestState manifest, int nextIndex)
    {
        EnsureDataDirectoryExists();

        var targetPath = BuildManifestFilePath(nextIndex);
        var encodedLength = BinaryManifestCodec.ComputeEncodedLength(manifest);
        EnsureEncodeBufferCapacity(encodedLength);

        BinaryManifestCodec.WriteEncoded(manifest, _encodeBuffer.AsSpan(0, encodedLength));
        BinaryManifestDurability.WriteManifestDataFileBlocking(targetPath, _encodeBuffer.AsSpan(0, encodedLength));
        UpdateCurrentPointerBlocking(nextIndex);
        SetCache(manifest, nextIndex);
    }

    private Task PublishCoreAsync(ManifestState manifest, int nextIndex, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        PublishCoreBlocking(manifest, nextIndex);
        return Task.CompletedTask;
    }

    private ManifestState PublishRollCoreBlocking(int currentJournal, ulong nextSequence, int nextIndex)
    {
        EnsureDataDirectoryExists();

        var (previous, snapshotPathUtf8) = ReadRollBaselineLocked();
        var format = previous.Format is 0 ? 1 : previous.Format;
        var snapshot = previous.LastSnapshot;

        var encodedLength = BinaryManifestCodec.ComputeRollEncodedLength(snapshot, snapshotPathUtf8.Length);
        EnsureEncodeBufferCapacity(encodedLength);

        _ = BinaryManifestCodec.WriteRollEncoded(
            format,
            currentJournal,
            nextSequence,
            snapshot,
            snapshotPathUtf8.Span,
            _encodeBuffer.AsSpan(0, encodedLength));

        var targetPath = BuildManifestFilePath(nextIndex);
        BinaryManifestPointer.Write(_currentPointerBuffer, nextIndex);
        BinaryManifestDurability.WriteManifestRollBlocking(
            targetPath,
            _encodeBuffer.AsSpan(0, encodedLength),
            _currentPointerWriter,
            _currentPointerBuffer);

        var manifest = new ManifestState
        {
            Format = format,
            CurrentJournal = currentJournal,
            NextSequence = nextSequence,
            LastSnapshot = snapshot,
        };
        SetCache(manifest, nextIndex);
        return manifest;
    }

    private void SeedNextManifestIndex(int publishedIndex)
    {
        lock (_nextIndexInitLock)
        {
            _nextManifestIndex = publishedIndex;
            _nextIndexInitialized = true;
        }
    }

    private void UpdateCurrentPointerBlocking(int manifestIndex)
    {
        BinaryManifestPointer.Write(_currentPointerBuffer, manifestIndex);
        BinaryManifestDurability.WriteCurrentPointerBlocking(_currentPointerWriter, _currentPointerBuffer);
    }

    [SuppressMessage("AsyncUsage", "MA0045:Use await instead of GetResult()", Justification = "ResolveNextIndexFromDiskLocked runs under PublishBlocking while the gate is held.")]
    private byte[] ReadCurrentPointerBytes() => File.ReadAllBytes(_currentPath);

    private int ResolveNextIndexFromDiskLocked()
    {
        var maxOnDisk = ScanMaxManifestIndexOnDisk();
        if (!File.Exists(_currentPath))
            return maxOnDisk + 1;

        var pointerBytes = ReadCurrentPointerBytes();
        if (!BinaryManifestPointer.IsBinaryPointer(pointerBytes))
            throw new InvalidDataException($"Manifest current pointer is not a binary manifest pointer: {_currentPath}");

        var fromCurrent = BinaryManifestPointer.Read(pointerBytes);
        var baseline = fromCurrent > maxOnDisk ? fromCurrent : maxOnDisk;
        return baseline + 1;
    }

    private void RunRetentionWorkerLoop()
    {
        try
        {
            while (_pendingRetentionManifest is { } manifest)
            {
                _pendingRetentionManifest = null;
                var cleanupFailed = ManifestRetentionCleanup.Run(_retentionContext, manifest);
                _retentionReadiness?.RecordWriteOutcome(cleanupFailed);
            }
        }
        finally
        {
            _ = Interlocked.Exchange(ref _retentionWorkerScheduled, 0);
            if (_pendingRetentionManifest is not null && Interlocked.CompareExchange(ref _retentionWorkerScheduled, 1, 0) is 0)
                _ = Task.Run(RunRetentionWorkerLoop, CancellationToken.None);
        }
    }

    private int ScanMaxManifestIndexOnDisk()
    {
        if (!Directory.Exists(_dataDir))
            return 0;

        var max = 0;
        foreach (var path in Directory.GetFiles(_dataDir, _retentionContext.ManifestFileGlob))
        {
            var index = TryParseBinaryManifestIndex(Path.GetFileName(path));
            if (index > max)
                max = index;
        }

        return max;
    }

    private void ScheduleRetentionCleanup(ManifestState manifest)
    {
        _pendingRetentionManifest = manifest;
        if (Interlocked.CompareExchange(ref _retentionWorkerScheduled, 1, 0) is not 0)
            return;

        _ = Task.Run(RunRetentionWorkerLoop, CancellationToken.None);
    }
}
