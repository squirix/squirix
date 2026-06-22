using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Utils;

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
    private const int DefaultEncodeBufferCapacity = 256;
    private readonly ManifestCache _cache = new();
    private readonly Lock _cacheSync = new();

    private readonly string _currentPath;
    private readonly byte[] _currentPointerBuffer = new byte[ManifestPointer.Size];
    private readonly ManifestPersistentPointerWriter _currentPointerWriter;
    private readonly string _dataDir;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly string _manifestFileNamePrefix;
    private readonly Lock _nextIndexInitLock = new();
    private readonly ManifestRetentionContext _retentionContext;
    private readonly IRetentionCleanupReadinessStatus? _retentionReadiness;
    private bool _dataDirEnsured;
    private byte[] _encodeBuffer = new byte[DefaultEncodeBufferCapacity];
    private volatile bool _nextIndexInitialized;
    private int _nextManifestIndex;
    private volatile ManifestState? _pendingRetentionManifest;
    private int _retentionWorkerScheduled;

    public ManifestStore(PersistenceOptions options, ILogger<ManifestStore>? logger = null, IRetentionCleanupReadinessStatus? retentionReadiness = null)
        : this(options, logger, retentionReadiness, new StorageFileOperations())
    {
    }

    internal ManifestStore(PersistenceOptions options, ILogger<ManifestStore>? logger, IRetentionCleanupReadinessStatus? retentionReadiness, IStorageFileOperations fileOperations)
    {
        _dataDir = options.DataDir;
        _currentPath = PathEx.Combine(_dataDir, $"{StorageFilePrefixes.Manifest}current");
        _currentPointerWriter = new ManifestPersistentPointerWriter(_currentPath);
        _manifestFileNamePrefix = PathEx.Combine(_dataDir, StorageFilePrefixes.Manifest);
        _retentionReadiness = retentionReadiness;
        _retentionContext = new ManifestRetentionContext(
            _dataDir,
            options.ManifestRetentionCount > 0 ? options.ManifestRetentionCount : 3,
            options.SnapshotRetentionCount > 0 ? options.SnapshotRetentionCount : 3,
            fileOperations,
            logger,
            $"{StorageFilePrefixes.Manifest}*{StorageFileExtensions.Manifest}",
            TryParseManifestIndex);
    }

    public async Task<ManifestState> ReadCurrentOrDefaultAsync(CancellationToken cancellationToken = default)
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

    public async Task WriteAsync(ManifestState manifest, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await EnsureNextManifestIndexInitializedAsync(cancellationToken).ConfigureAwait(false);
            var nextIndex = IncrementNextManifestIndex();
            await PublishCoreAsync(manifest, nextIndex, cancellationToken).ConfigureAwait(false);
            ScheduleRetentionCleanup(manifest);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    public void Dispose()
    {
        _currentPointerWriter.Dispose();
        _gate.Dispose();
    }

    internal void PublishRollBlocking(int currentJournal, ulong nextSequence)
    {
        var nextIndex = AllocateNextManifestIndex();
        var manifest = PublishRollCoreBlocking(currentJournal, nextSequence, nextIndex);
        ScheduleRetentionCleanup(manifest);
    }

    internal ManifestState ReadCurrentOrDefaultBlocking() => ReadCurrentOrDefaultAsync(CancellationToken.None).GetAwaiter().GetResult();

    private static int ResolveNextIndexFromPointer(ReadOnlySpan<byte> pointerBytes, int maxOnDisk, string currentPath)
    {
        if (!ManifestPointer.IsValidPointer(pointerBytes))
            throw new InvalidDataException($"Manifest current pointer is invalid: {currentPath}");

        var fromCurrent = ManifestPointer.Read(pointerBytes);
        var baseline = fromCurrent > maxOnDisk ? fromCurrent : maxOnDisk;
        return baseline + 1;
    }

    private static int TryParseManifestIndex(string name) => TryParseManifestIndex(name.AsSpan());

    private static int TryParseManifestIndex(ReadOnlySpan<char> name)
    {
        if (name.IsEmpty)
            return 0;

        var prefix = StorageFilePrefixes.Manifest.AsSpan();
        var extension = StorageFileExtensions.Manifest.AsSpan();
        if (!name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return 0;

        if (!name.EndsWith(extension, StringComparison.OrdinalIgnoreCase))
            return 0;

        var numberPart = name.Slice(prefix.Length, name.Length - prefix.Length - extension.Length);
        return int.TryParse(numberPart, CultureInfo.InvariantCulture, out var n) ? n : 0;
    }

    private int AllocateNextManifestIndex()
    {
        EnsureNextManifestIndexInitialized();
        return IncrementNextManifestIndex();
    }

    private string BuildManifestFilePath(int index) => string.Create(
        _manifestFileNamePrefix.Length + 6 + StorageFileExtensions.Manifest.Length,
        (Prefix: _manifestFileNamePrefix, Index: index),
        static (span, state) =>
        {
            state.Prefix.AsSpan().CopyTo(span);
            var suffix = span[state.Prefix.Length..];
            if (!state.Index.TryFormat(suffix, out var charsWritten, "D6", CultureInfo.InvariantCulture))
                throw new InvalidOperationException("Manifest index did not fit fixed-width field.");

            StorageFileExtensions.Manifest.AsSpan().CopyTo(suffix[charsWritten..]);
        });

    private void EnsureDataDirectoryExists()
    {
        if (_dataDirEnsured)
            return;

        _ = Directory.CreateDirectory(_dataDir);
        _dataDirEnsured = true;
    }

    private async Task EnsureDataDirectoryExistsAsync(CancellationToken cancellationToken)
    {
        if (_dataDirEnsured)
            return;

        _ = await DirectoryEx.CreateDirectoryAsync(_dataDir, cancellationToken: cancellationToken).ConfigureAwait(false);
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

    private async Task EnsureNextManifestIndexInitializedAsync(CancellationToken cancellationToken)
    {
        if (_nextIndexInitialized)
            return;

        lock (_nextIndexInitLock)
        {
            if (_nextIndexInitialized)
                return;

            var fromCache = TryReadCurrentIndexForInit();
            if (fromCache is not null)
            {
                _nextManifestIndex = fromCache.Value;
                _nextIndexInitialized = true;
                return;
            }
        }

        var nextFromDisk = await ResolveNextIndexFromDiskAsync(cancellationToken).ConfigureAwait(false);

        lock (_nextIndexInitLock)
        {
            if (_nextIndexInitialized)
                return;

            _nextManifestIndex = nextFromDisk - 1;
            _nextIndexInitialized = true;
        }
    }

    private int IncrementNextManifestIndex()
    {
        lock (_nextIndexInitLock)
            return ++_nextManifestIndex;
    }

    private async Task<ManifestState> LoadCurrentFromDiskAsync(CancellationToken cancellationToken)
    {
        await EnsureDataDirectoryExistsAsync(cancellationToken).ConfigureAwait(false);

        if (!File.Exists(_currentPath))
            return new ManifestState();

        var pointerBytes = await File.ReadAllBytesAsync(_currentPath, cancellationToken).ConfigureAwait(false);
        if (!ManifestPointer.IsValidPointer(pointerBytes))
            throw new InvalidDataException($"Manifest current pointer is invalid: {_currentPath}");

        var index = ManifestPointer.Read(pointerBytes);
        var path = BuildManifestFilePath(index);
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        var manifest = ManifestCodec.Decode(bytes);
        SetCache(manifest, index);
        SeedNextManifestIndex(index);
        return manifest;
    }

    private async Task PublishCoreAsync(ManifestState manifest, int nextIndex, CancellationToken cancellationToken)
    {
        await EnsureDataDirectoryExistsAsync(cancellationToken).ConfigureAwait(false);

        var targetPath = BuildManifestFilePath(nextIndex);
        var encodedLength = ManifestCodec.ComputeEncodedLength(manifest);
        EnsureEncodeBufferCapacity(encodedLength);

        ManifestCodec.WriteEncoded(manifest, _encodeBuffer.AsSpan(0, encodedLength));

        await Task.Run(
            () =>
            {
                ManifestDurability.WriteManifestDataFileBlocking(targetPath, _encodeBuffer.AsSpan(0, encodedLength));
                UpdateCurrentPointerBlocking(nextIndex);
            },
            cancellationToken).ConfigureAwait(false);

        SetCache(manifest, nextIndex);
    }

    private ManifestState PublishRollCoreBlocking(int currentJournal, ulong nextSequence, int nextIndex)
    {
        EnsureDataDirectoryExists();

        var (previous, snapshotPathUtf8) = ReadRollBaselineLocked();
        var format = previous.Format is 0 ? 1 : previous.Format;
        var snapshot = previous.LastSnapshot;

        var encodedLength = ManifestCodec.ComputeRollEncodedLength(snapshot, snapshotPathUtf8.Length);
        EnsureEncodeBufferCapacity(encodedLength);

        _ = ManifestCodec.WriteRollEncoded(format, currentJournal, nextSequence, snapshot, snapshotPathUtf8.Span, _encodeBuffer.AsSpan(0, encodedLength));

        var targetPath = BuildManifestFilePath(nextIndex);
        ManifestPointer.Write(_currentPointerBuffer, nextIndex);
        ManifestDurability.WriteManifestRollBlocking(targetPath, _encodeBuffer.AsSpan(0, encodedLength), _currentPointerWriter, _currentPointerBuffer);

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

    private byte[] ReadCurrentPointerBytes() => File.ReadAllBytes(_currentPath);

    private (ManifestState Previous, ReadOnlyMemory<byte> SnapshotPathUtf8) ReadRollBaselineLocked()
    {
        lock (_cacheSync)
        {
            if (!_cache.IsInitialized)
                return (new ManifestState(), ReadOnlyMemory<byte>.Empty);

            return (_cache.Current, _cache.SnapshotPathUtf8);
        }
    }

    private async Task<int> ResolveNextIndexFromDiskAsync(CancellationToken cancellationToken)
    {
        var maxOnDisk = ScanMaxManifestIndexOnDisk();
        if (!File.Exists(_currentPath))
            return maxOnDisk + 1;

        var pointerBytes = await File.ReadAllBytesAsync(_currentPath, cancellationToken).ConfigureAwait(false);
        return ResolveNextIndexFromPointer(pointerBytes, maxOnDisk, _currentPath);
    }

    private int ResolveNextIndexFromDiskLocked()
    {
        var maxOnDisk = ScanMaxManifestIndexOnDisk();
        if (!File.Exists(_currentPath))
            return maxOnDisk + 1;

        return ResolveNextIndexFromPointer(ReadCurrentPointerBytes(), maxOnDisk, _currentPath);
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
            var index = TryParseManifestIndex(Path.GetFileName(path));
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

    private void SeedNextManifestIndex(int publishedIndex)
    {
        lock (_nextIndexInitLock)
        {
            _nextManifestIndex = publishedIndex;
            _nextIndexInitialized = true;
        }
    }

    private void SetCache(ManifestState manifest, int index)
    {
        lock (_cacheSync)
            _cache.Set(manifest, index);
    }

    private bool TryGetCachedCurrent(out ManifestState manifest)
    {
        lock (_cacheSync)
        {
            if (!_cache.IsInitialized)
            {
                manifest = new ManifestState();
                return false;
            }

            manifest = _cache.Current;
            return true;
        }
    }

    private int? TryReadCurrentIndexForInit()
    {
        lock (_cacheSync)
            return _cache.IsInitialized ? _cache.CurrentIndex : null;
    }

    private void UpdateCurrentPointerBlocking(int manifestIndex)
    {
        ManifestPointer.Write(_currentPointerBuffer, manifestIndex);
        ManifestDurability.WriteCurrentPointerBlocking(_currentPointerWriter, _currentPointerBuffer);
    }
}
