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
    private readonly string _dataDir;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly ManifestRetentionContext _retentionContext;
    private readonly IRetentionCleanupReadinessStatus? _retentionReadiness;
    private byte[] _encodeBuffer = new byte[DefaultEncodeBufferCapacity];
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

    public void Dispose() => _gate.Dispose();

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
            var nextIndex = ResolveNextIndexLocked();
            PublishCoreAsync(manifest, nextIndex, CancellationToken.None).GetAwaiter().GetResult();
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
        _gate.Wait(CancellationToken.None);
        try
        {
            var manifest = BuildRollManifestLocked(currentJournal, nextSequence);
            var nextIndex = ResolveNextIndexLocked();
            PublishCoreAsync(manifest, nextIndex, CancellationToken.None).GetAwaiter().GetResult();
            ScheduleRetentionCleanup(manifest);
        }
        finally
        {
            _ = _gate.Release();
        }
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
                return _cache.Current;

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
            var nextIndex = ResolveNextIndexLocked();
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
            await PublishCoreAsync(manifest, manifestIndex, cancellationToken).ConfigureAwait(false);
            ScheduleRetentionCleanup(manifest);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    private static string BuildManifestFileName(int index) =>
        $"{StorageFilePrefixes.Manifest}{index.ToString("D6", CultureInfo.InvariantCulture)}{StorageFileExtensions.BinaryManifest}";

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

    private ManifestState BuildRollManifestLocked(int currentJournal, ulong nextSequence)
    {
        var previous = _cache.IsInitialized ? _cache.Current : new ManifestState();
        return new ManifestState
        {
            Format = previous.Format is 0 ? 1 : previous.Format,
            CurrentJournal = currentJournal,
            NextSequence = nextSequence,
            LastSnapshot = previous.LastSnapshot,
        };
    }

    private void EnsureEncodeBufferCapacity(int encodedLength)
    {
        if (_encodeBuffer.Length >= encodedLength)
            return;

        _encodeBuffer = new byte[Math.Max(encodedLength, _encodeBuffer.Length * 2)];
    }

    private async Task<ManifestState> LoadCurrentFromDiskAsync(CancellationToken cancellationToken)
    {
        _ = await DirectoryEx.CreateDirectoryAsync(_dataDir, cancellationToken: cancellationToken).ConfigureAwait(false);

        if (!File.Exists(_currentPath))
            return new ManifestState();

        var pointerBytes = await File.ReadAllBytesAsync(_currentPath, cancellationToken).ConfigureAwait(false);
        if (!BinaryManifestPointer.IsBinaryPointer(pointerBytes))
            throw new InvalidDataException($"Manifest current pointer is not a binary manifest pointer: {_currentPath}");

        var index = BinaryManifestPointer.Read(pointerBytes);
        var path = PathEx.Combine(_dataDir, BuildManifestFileName(index));
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        var manifest = BinaryManifestCodec.Decode(bytes);
        _cache.Set(manifest, index);
        return manifest;
    }

    private async Task PublishCoreAsync(ManifestState manifest, int nextIndex, CancellationToken cancellationToken)
    {
        _ = await DirectoryEx.CreateDirectoryAsync(_dataDir, cancellationToken: cancellationToken).ConfigureAwait(false);

        var targetPath = PathEx.Combine(_dataDir, BuildManifestFileName(nextIndex));
        var encodedLength = BinaryManifestCodec.ComputeEncodedLength(manifest);
        EnsureEncodeBufferCapacity(encodedLength);

        BinaryManifestCodec.WriteEncoded(manifest, _encodeBuffer.AsSpan(0, encodedLength));

        await BinaryManifestDurability.WriteManifestDataFileAsync(
            targetPath,
            _encodeBuffer.AsMemory(0, encodedLength),
            cancellationToken).ConfigureAwait(false);

        await UpdateCurrentPointerAsync(nextIndex, cancellationToken).ConfigureAwait(false);
        _cache.Set(manifest, nextIndex);
    }

    private async Task UpdateCurrentPointerAsync(int manifestIndex, CancellationToken cancellationToken)
    {
        BinaryManifestPointer.Write(_currentPointerBuffer, manifestIndex);
        await BinaryManifestDurability.WriteCurrentPointerAsync(_currentPath, _currentPointerBuffer, cancellationToken).ConfigureAwait(false);
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

    private int ResolveNextIndexLocked() => _cache.IsInitialized ? _cache.CurrentIndex + 1 : ResolveNextIndexFromDiskLocked();

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
