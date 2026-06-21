using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Serialization;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest.Json;

/// <summary>JSON manifest store (<c>.msqx</c> files and UTF-8 <c>man-current</c> pointer).</summary>
[SuppressMessage("Design", "MA0180:Use ILogger<T> with the current class type", Justification = "Retention logs use the ManifestStore category for stable observability.")]
internal sealed class JsonManifestStore : IManifestStore
{
    private readonly string _currentPath;

    private readonly string _dataDir;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly ManifestRetentionContext _retentionContext;
    private readonly IRetentionCleanupReadinessStatus? _retentionReadiness;

    public JsonManifestStore(PersistenceOptions options, ILogger<ManifestStore>? logger = null, IRetentionCleanupReadinessStatus? retentionReadiness = null)
        : this(options, logger, retentionReadiness, new StorageFileOperations())
    {
    }

    internal JsonManifestStore(
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
            $"{StorageFilePrefixes.Manifest}*{StorageFileExtensions.Manifest}",
            TryParseIndex);
    }

    /// <inheritdoc />
    [SuppressMessage(
        "Security",
        "CA3003:Review code for file path injection vulnerabilities",
        Justification = "Manifest path is resolved from a validated CURRENT pointer filename under the configured data directory.")]
    public async Task<ManifestState> ReadCurrentOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _ = await DirectoryEx.CreateDirectoryAsync(_dataDir, cancellationToken: cancellationToken).ConfigureAwait(false);

            if (!File.Exists(_currentPath))
                return new ManifestState();

            var name = (await File.ReadAllTextAsync(_currentPath, cancellationToken).ConfigureAwait(false)).Trim();
            if (string.IsNullOrWhiteSpace(name))
                throw new InvalidDataException($"Manifest current pointer is empty: {_currentPath}");

            if (TryParseIndex(name) <= 0)
                throw new InvalidDataException($"Manifest current pointer is invalid: {_currentPath}");

            var path = PathEx.Combine(_dataDir, name);
            var bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
            return JsonSerializer.Deserialize<ManifestState>(bytes, DurabilityJson.StrictSerializerOptions) ??
                   throw new InvalidDataException($"Manifest file did not contain a valid manifest: {path}");
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc />
    public async Task WriteAsync(ManifestState manifest, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _ = await DirectoryEx.CreateDirectoryAsync(_dataDir, cancellationToken: cancellationToken).ConfigureAwait(false);

            var baselineIndex = await ResolveBaselineManifestIndexAsync(cancellationToken).ConfigureAwait(false);
            var nextIndex = baselineIndex + 1;
            var fileName = $"{StorageFilePrefixes.Manifest}{nextIndex.ToString("D6", CultureInfo.InvariantCulture)}{StorageFileExtensions.Manifest}";
            var targetPath = PathEx.Combine(_dataDir, fileName);

            var manifestStream = new FileStream(targetPath, FileMode.CreateNew, FileAccess.Write, FileShare.None);
            try
            {
                await JsonSerializer.SerializeAsync(manifestStream, manifest, SquirixJsonSerializerContext.Default.ManifestState, cancellationToken).ConfigureAwait(false);
                await manifestStream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                await manifestStream.DisposeAsync().ConfigureAwait(false);
            }

            await UpdateCurrentAtomicallyAsync(fileName, cancellationToken).ConfigureAwait(false);

            var cleanupFailed = ManifestRetentionCleanup.Run(_retentionContext, manifest);
            _retentionReadiness?.RecordWriteOutcome(cleanupFailed);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    public void Dispose() => _gate.Dispose();

    [SuppressMessage("AsyncUsage", "MA0045:Use await instead of GetResult()", Justification = "Blocking API for the dedicated journal I/O thread.")]
    [SuppressMessage(
        "Usage",
        "VSTHRD002:Avoid problematic synchronous waits",
        Justification = "Journal I/O thread has no synchronization context and must observe manifest durability before continuing a segment roll.")]
    public ManifestState ReadCurrentOrDefaultBlocking() => ReadCurrentOrDefaultAsync(CancellationToken.None).GetAwaiter().GetResult();

    [SuppressMessage("AsyncUsage", "MA0045:Use await instead of GetResult()", Justification = "Blocking API for the dedicated journal I/O thread.")]
    [SuppressMessage(
        "Usage",
        "VSTHRD002:Avoid problematic synchronous waits",
        Justification = "Journal I/O thread has no synchronization context and must observe manifest durability before continuing a segment roll.")]
    public void WriteBlocking(ManifestState manifest) => WriteAsync(manifest, CancellationToken.None).GetAwaiter().GetResult();

    public void PublishBlocking(ManifestState manifest) => WriteBlocking(manifest);

    public void PublishRollBlocking(int currentJournal, ulong nextSequence)
    {
        var prev = ReadCurrentOrDefaultBlocking();
        WriteBlocking(
            new ManifestState
            {
                Format = prev.Format is 0 ? 1 : prev.Format,
                CurrentJournal = currentJournal,
                NextSequence = nextSequence,
                LastSnapshot = prev.LastSnapshot,
            });
    }

    private static FileOptions GetCurrentFileWriteOptions()
    {
        var opts = FileOptions.SequentialScan;
        if (OperatingSystem.IsWindows())
            opts |= FileOptions.WriteThrough;
        return opts;
    }

    private static int TryParseIndex(string name)
    {
        if (string.IsNullOrEmpty(name))
            return 0;
        if (!name.StartsWith(StorageFilePrefixes.Manifest, StringComparison.OrdinalIgnoreCase))
            return 0;
        if (!name.EndsWith(StorageFileExtensions.Manifest, StringComparison.OrdinalIgnoreCase))
            return 0;

        var numberPart = name.Substring(StorageFilePrefixes.Manifest.Length, name.Length - StorageFilePrefixes.Manifest.Length - StorageFileExtensions.Manifest.Length);
        return int.TryParse(numberPart, CultureInfo.InvariantCulture, out var n) ? n : 0;
    }

    private async Task<int> ResolveBaselineManifestIndexAsync(CancellationToken cancellationToken)
    {
        var maxOnDisk = ScanMaxManifestIndexOnDisk();

        if (!File.Exists(_currentPath))
            return maxOnDisk;

        string name;
        try
        {
            name = (await File.ReadAllTextAsync(_currentPath, cancellationToken).ConfigureAwait(false)).Trim();
        }
        catch (IOException ex)
        {
            throw new InvalidDataException($"Manifest current pointer is unreadable: {_currentPath}", ex);
        }
        catch (UnauthorizedAccessException ex)
        {
            throw new InvalidDataException($"Manifest current pointer is unreadable: {_currentPath}", ex);
        }

        if (string.IsNullOrWhiteSpace(name))
            throw new InvalidDataException($"Manifest current pointer is empty: {_currentPath}");

        var fromCurrent = TryParseIndex(name);
        if (fromCurrent <= 0)
            throw new InvalidDataException($"Manifest current pointer is invalid: {_currentPath}");

        return fromCurrent > maxOnDisk ? fromCurrent : maxOnDisk;
    }

    private int ScanMaxManifestIndexOnDisk()
    {
        if (!Directory.Exists(_dataDir))
            return 0;

        var max = 0;
        foreach (var path in Directory.GetFiles(_dataDir, $"{StorageFilePrefixes.Manifest}*{StorageFileExtensions.Manifest}"))
        {
            var index = TryParseIndex(Path.GetFileName(path));
            if (index > max)
                max = index;
        }

        return max;
    }

    private async Task UpdateCurrentAtomicallyAsync(string newFileName, CancellationToken cancellationToken)
    {
        var tmp = PathEx.Combine(_dataDir, $"{StorageFilePrefixes.Manifest}current.tmp");
        var payload = Encoding.UTF8.GetBytes(newFileName + Environment.NewLine);

        var currentStream = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 4096, GetCurrentFileWriteOptions());
        try
        {
            await currentStream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
            await currentStream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await currentStream.DisposeAsync().ConfigureAwait(false);
        }

        if (File.Exists(_currentPath))
            File.Replace(tmp, _currentPath, null);
        else
            File.Move(tmp, _currentPath, true);
    }
}
