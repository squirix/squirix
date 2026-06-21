using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Node.Observability;
using Squirix.Server.Serialization;
using Squirix.Server.Storage.Journaling.JsonFramed;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage;

internal sealed class ManifestStore : IDisposable
{
    private readonly string _currentPath;

    private readonly string _dataDir;
    private readonly IStorageFileOperations _fileOperations;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly ILogger<ManifestStore>? _logger;
    private readonly int _retention;
    private readonly IRetentionCleanupReadinessStatus? _retentionReadiness;
    private readonly int _snapshotRetention;

    public ManifestStore(PersistenceOptions options, ILogger<ManifestStore>? logger = null, IRetentionCleanupReadinessStatus? retentionReadiness = null)
        : this(options, logger, retentionReadiness, new StorageFileOperations())
    {
    }

    internal ManifestStore(PersistenceOptions options, ILogger<ManifestStore>? logger, IRetentionCleanupReadinessStatus? retentionReadiness, IStorageFileOperations fileOperations)
    {
        _dataDir = options.DataDir;
        _logger = logger;
        _currentPath = PathEx.Combine(_dataDir, $"{StorageFilePrefixes.Manifest}current");
        _retention = options.ManifestRetentionCount > 0 ? options.ManifestRetentionCount : 3;
        _snapshotRetention = options.SnapshotRetentionCount > 0 ? options.SnapshotRetentionCount : 3;
        _fileOperations = fileOperations;
        _retentionReadiness = retentionReadiness;
    }

    /// <summary>
    /// Reads the manifest referenced by the <c>CURRENT</c> file in the data directory.
    /// Returns a new default manifest only when the current pointer does not exist.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// The deserialized <see cref="Manifest" /> when available; otherwise a new default manifest on first boot.
    /// </returns>
    /// <remarks>
    ///     <para>
    ///     The method is tolerant only to first boot. Empty pointers, missing target files,
    ///     unreadable manifests, and invalid manifest contents are treated as storage
    ///     corruption and are surfaced to the caller.
    ///     </para>
    ///     <para>Thread-safe: the entire operation is performed under an internal gate.</para>
    /// </remarks>
    [SuppressMessage("Security", "CA3003:Review code for file path injection vulnerabilities", Justification = "Manifest path is resolved from a validated CURRENT pointer filename under the configured data directory.")]
    public async Task<Manifest> ReadCurrentOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _ = await DirectoryEx.CreateDirectoryAsync(_dataDir, cancellationToken: cancellationToken).ConfigureAwait(false);

            if (!File.Exists(_currentPath))
                return new Manifest();

            var name = (await File.ReadAllTextAsync(_currentPath, cancellationToken).ConfigureAwait(false)).Trim();
            if (string.IsNullOrWhiteSpace(name))
                throw new InvalidDataException($"Manifest current pointer is empty: {_currentPath}");

            if (TryParseIndex(name) <= 0)
                throw new InvalidDataException($"Manifest current pointer is invalid: {_currentPath}");

            var path = PathEx.Combine(_dataDir, name);
            var bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
            return JsonSerializer.Deserialize<Manifest>(bytes, DurabilityJson.StrictSerializerOptions) ??
                   throw new InvalidDataException($"Manifest file did not contain a valid manifest: {path}");
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <summary>
    /// Persists the given <paramref name="manifest" /> as the next monotonically numbered
    /// manifest file in the data directory and atomically updates the <c>CURRENT</c> pointer
    /// to reference it. Old manifest files are then trimmed according to retention settings.
    /// </summary>
    /// <param name="manifest">The in-memory manifest snapshot to write to disk.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <remarks>
    ///     <para>The operation performs three steps under an internal gate (thread-safe):</para>
    ///     <list type="number">
    ///         <item>
    ///             <description>Write a new manifest file (next sequential index) and flush it to disk.</description>
    ///         </item>
    ///         <item>
    ///             <description>Atomically replace/update the <c>CURRENT</c> file so readers observe an all-or-nothing switch.</description>
    ///         </item>
    ///         <item>
    ///             <description>Delete older manifest files, keeping only the most recent ones per retention policy.</description>
    ///         </item>
    ///     </list>
    ///     <para>
    ///     Although the method minimizes torn writes (fsync + atomic replace), callers should treat the
    ///     returned state as durable only after the method completes without exceptions.
    ///     </para>
    /// </remarks>
    /// <returns>A <see cref="Task" /> that completes when the manifest is durable on disk.</returns>
    /// <exception cref="IOException">
    /// An I/O error occurred while writing the manifest or updating <c>CURRENT</c>.
    /// </exception>
    /// <exception cref="UnauthorizedAccessException">The process lacks filesystem permissions for the data directory or files.</exception>
    /// <exception cref="JsonException">
    /// The <paramref name="manifest" /> could not be serialized.
    /// </exception>
    /// <exception cref="NotSupportedException">The manifest contains a value that cannot be serialized by the configured JSON options.</exception>
    public async Task WriteAsync(Manifest manifest, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _ = await DirectoryEx.CreateDirectoryAsync(_dataDir, cancellationToken: cancellationToken).ConfigureAwait(false);

            var baselineIndex = await ResolveBaselineManifestIndexAsync(cancellationToken).ConfigureAwait(false);
            var nextIndex = baselineIndex + 1;
            var fileName = $"{StorageFilePrefixes.Manifest}{nextIndex.ToString("D6", CultureInfo.InvariantCulture)}{StorageFileExtensions.Manifest}";
            var targetPath = PathEx.Combine(_dataDir, fileName);

            // 1) Write a new manifest file
            var manifestStream = new FileStream(targetPath, FileMode.CreateNew, FileAccess.Write, FileShare.None);
            try
            {
                await JsonSerializer.SerializeAsync(manifestStream, manifest, SquirixJsonSerializerContext.Default.Manifest, cancellationToken).ConfigureAwait(false);
                await manifestStream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                await manifestStream.DisposeAsync().ConfigureAwait(false);
            }

            // 2) Atomically update CURRENT to point to the new manifest file
            await UpdateCurrentAtomicallyAsync(fileName, cancellationToken).ConfigureAwait(false);

            // 3) Retention: keep only the last N manifest files
            var manifestCleanupFailed = TryCleanupOldManifests();
            var snapshotCleanupFailed = TryCleanupOldSnapshots(manifest.LastSnapshot);
            var journalCleanupFailed = TryCleanupObsoleteJournalSegments(manifest);
            var cleanupFailed = manifestCleanupFailed || snapshotCleanupFailed || journalCleanupFailed;
            _retentionReadiness?.RecordWriteOutcome(cleanupFailed);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    public void Dispose() => _gate.Dispose();

    [SuppressMessage("AsyncUsage", "MA0045:Use await instead of GetResult()", Justification = "Blocking API for the dedicated journal I/O thread.")]
    [SuppressMessage("Usage", "VSTHRD002:Avoid problematic synchronous waits", Justification = "Journal I/O thread has no synchronization context and must observe manifest durability before continuing a segment roll.")]
    internal Manifest ReadCurrentOrDefaultBlocking() => ReadCurrentOrDefaultAsync(CancellationToken.None).GetAwaiter().GetResult();

    [SuppressMessage("AsyncUsage", "MA0045:Use await instead of GetResult()", Justification = "Blocking API for the dedicated journal I/O thread.")]
    [SuppressMessage("Usage", "VSTHRD002:Avoid problematic synchronous waits", Justification = "Journal I/O thread has no synchronization context and must observe manifest durability before continuing a segment roll.")]
    internal void WriteBlocking(Manifest manifest) => WriteAsync(manifest, CancellationToken.None).GetAwaiter().GetResult();

    private static FileOptions GetCurrentFileWriteOptions()
    {
        var opts = FileOptions.SequentialScan;
        if (OperatingSystem.IsWindows())
            opts |= FileOptions.WriteThrough;
        return opts;
    }

    private static IndexedStorageFile[] GetIndexedFiles(string[] files, Func<string, int> parseIndex)
    {
        var result = new List<IndexedStorageFile>();
        foreach (var path in files)
        {
            var index = parseIndex(Path.GetFileName(path));
            if (index > 0)
                result.Add(new IndexedStorageFile(path, index));
        }

        result.Sort(static (left, right) => right.Index.CompareTo(left.Index));
        return [.. result];
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

    private static int TryParseSnapshotIndex(string name)
    {
        if (string.IsNullOrEmpty(name))
            return 0;
        if (!name.StartsWith(StorageFilePrefixes.Snapshot, StringComparison.OrdinalIgnoreCase))
            return 0;
        if (!name.EndsWith(StorageFileExtensions.Snapshot, StringComparison.OrdinalIgnoreCase))
            return 0;

        var numberPart = name.Substring(StorageFilePrefixes.Snapshot.Length, name.Length - StorageFilePrefixes.Snapshot.Length - StorageFileExtensions.Snapshot.Length);
        return int.TryParse(numberPart, CultureInfo.InvariantCulture, out var n) ? n : 0;
    }

    private void ReportRetentionCleanupException(string artifactKind, Exception exception)
    {
        StorageRetentionMetrics.DeleteFailuresTotal.WithLabels(artifactKind, ManifestRetentionFailureOutcome.CleanupException).Inc(1);

        if (_logger is not null)
            LogManager.ManifestRetentionCleanupFailed(_logger, exception, artifactKind);
    }

    private void ReportRetentionDeleteFailure(string artifactKind, string path)
    {
        StorageRetentionMetrics.DeleteFailuresTotal.WithLabels(artifactKind, ManifestRetentionFailureOutcome.DeleteFailed).Inc(1);

        if (_logger is not null)
            LogManager.ManifestRetentionDeleteFailed(_logger, artifactKind, path);
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

    private bool TryCleanupObsoleteJournalSegments(Manifest manifest)
    {
        try
        {
            var replayFromSegment = manifest.LastSnapshot?.ReplayFromJournalSegment ?? 0;
            if (replayFromSegment <= 1)
                return false;

            if (manifest.CurrentJournal < replayFromSegment)
                return false;

            var failed = false;
            foreach (var segment in JournalReader.EnumerateSegments(_dataDir, 1))
            {
                if (segment.Index >= replayFromSegment)
                    continue;

                if (segment.Index >= manifest.CurrentJournal)
                    continue;

                failed |= TryDeleteRetentionArtifact(segment.Path, ManifestRetentionArtifactKind.JournalSegment);
            }

            return failed;
        }
        catch (IOException ex)
        {
            ReportRetentionCleanupException(ManifestRetentionArtifactKind.JournalSegment, ex);
            return true;
        }
        catch (UnauthorizedAccessException ex)
        {
            ReportRetentionCleanupException(ManifestRetentionArtifactKind.JournalSegment, ex);
            return true;
        }
    }

    private bool TryCleanupOldManifests()
    {
        try
        {
            var files = Directory.GetFiles(_dataDir, $"{StorageFilePrefixes.Manifest}*{StorageFileExtensions.Manifest}");
            if (files.Length <= _retention)
                return false;

            var ordered = GetIndexedFiles(files, TryParseIndex);

            if (ordered.Length <= _retention)
                return false;

            var failed = false;
            for (var i = _retention; i < ordered.Length; i++)
                failed |= TryDeleteRetentionArtifact(ordered[i].Path, ManifestRetentionArtifactKind.Manifest);

            return failed;
        }
        catch (IOException ex)
        {
            ReportRetentionCleanupException(ManifestRetentionArtifactKind.Manifest, ex);
            return true;
        }
        catch (UnauthorizedAccessException ex)
        {
            ReportRetentionCleanupException(ManifestRetentionArtifactKind.Manifest, ex);
            return true;
        }
    }

    private bool TryCleanupOldSnapshots(Manifest.SnapshotRef? currentSnapshot)
    {
        try
        {
            var files = Directory.GetFiles(_dataDir, $"{StorageFilePrefixes.Snapshot}*{StorageFileExtensions.Snapshot}");
            if (files.Length <= _snapshotRetention)
                return false;

            var ordered = GetIndexedFiles(files, TryParseSnapshotIndex);

            if (ordered.Length <= _snapshotRetention)
                return false;

            var keep = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < _snapshotRetention && i < ordered.Length; i++)
                _ = keep.Add(ordered[i].Path);

            if (!string.IsNullOrWhiteSpace(currentSnapshot?.Path))
                _ = keep.Add(currentSnapshot.Path);

            var failed = false;
            for (var i = _snapshotRetention; i < ordered.Length; i++)
            {
                var stale = ordered[i];
                if (keep.Contains(stale.Path))
                    continue;

                failed |= TryDeleteRetentionArtifact(stale.Path, ManifestRetentionArtifactKind.Snapshot);
            }

            return failed;
        }
        catch (IOException ex)
        {
            ReportRetentionCleanupException(ManifestRetentionArtifactKind.Snapshot, ex);
            return true;
        }
        catch (UnauthorizedAccessException ex)
        {
            ReportRetentionCleanupException(ManifestRetentionArtifactKind.Snapshot, ex);
            return true;
        }
    }

    private bool TryDeleteRetentionArtifact(string path, string artifactKind)
    {
        if (_fileOperations.TryDelete(path))
            return false;

        ReportRetentionDeleteFailure(artifactKind, path);
        return true;
    }

    private async Task UpdateCurrentAtomicallyAsync(string newFileName, CancellationToken cancellationToken)
    {
        var tmp = PathEx.Combine(_dataDir, $"{StorageFilePrefixes.Manifest}current.tmp");
        var payload = Encoding.UTF8.GetBytes(newFileName + Environment.NewLine);

        // Write tmp with explicit fsync semantics
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

        // Atomically replace it when the destination exists (maps to Win32 ReplaceFile on Windows)
        if (File.Exists(_currentPath))
            File.Replace(tmp, _currentPath, null);
        else // First time: move/rename
            File.Move(tmp, _currentPath, true);
    }

    private readonly struct IndexedStorageFile
    {
        public IndexedStorageFile(string path, int index)
        {
            Path = path;
            Index = index;
        }

        public int Index { get; }

        public string Path { get; }
    }
}
