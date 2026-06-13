using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using Microsoft.Extensions.Logging;
using Squirix.Server.Node.Observability;
using Squirix.Server.Serialization;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage;

internal sealed class ManifestStore
{
    private readonly string _currentPath;

    private readonly string _dataDir;
    private readonly IStorageFileOperations _fileOperations;
    private readonly Lock _lock = new();
    private readonly ILogger<ManifestStore>? _logger;
    private readonly int _retention;
    private readonly int _snapshotRetention;

    public ManifestStore(PersistenceOptions options, ILogger<ManifestStore>? logger = null)
        : this(options, logger, new StorageFileOperations())
    {
    }

    internal ManifestStore(PersistenceOptions options, ILogger<ManifestStore>? logger, IStorageFileOperations fileOperations)
    {
        _dataDir = options.DataDir;
        _logger = logger;
        _currentPath = PathEx.Combine(_dataDir, $"{StorageFilePrefixes.Manifest}current");
        _retention = options.ManifestRetentionCount > 0 ? options.ManifestRetentionCount : 3;
        _snapshotRetention = options.SnapshotRetentionCount > 0 ? options.SnapshotRetentionCount : 3;
        _fileOperations = fileOperations;
    }

    /// <summary>
    /// Reads the manifest referenced by the <c>CURRENT</c> file in the data directory.
    /// Returns a new default manifest only when the current pointer does not exist.
    /// </summary>
    /// <returns>
    /// The deserialized <see cref="Manifest" /> when available; otherwise a new default manifest on first boot.
    /// </returns>
    /// <remarks>
    ///     <para>
    ///     The method is tolerant only to first boot. Empty pointers, missing target files,
    ///     unreadable manifests, and invalid manifest contents are treated as storage
    ///     corruption and are surfaced to the caller.
    ///     </para>
    ///     <para>Thread-safe: the entire operation is performed under an internal lock.</para>
    /// </remarks>
    public Manifest ReadCurrentOrDefault()
    {
        lock (_lock)
        {
            _ = DirectoryEx.CreateDirectory(_dataDir);

            if (!File.Exists(_currentPath))
                return new Manifest();

            var name = File.ReadAllText(_currentPath).Trim();
            if (string.IsNullOrWhiteSpace(name))
                throw new InvalidDataException($"Manifest current pointer is empty: {_currentPath}");

            var path = PathEx.Combine(_dataDir, name);
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            return JsonSerializer.Deserialize<Manifest>(fs, DurabilityJson.StrictSerializerOptions) ??
                   throw new InvalidDataException($"Manifest file did not contain a valid manifest: {path}");
        }
    }

    /// <summary>
    /// Persists the given <paramref name="manifest" /> as the next monotonically numbered
    /// manifest file in the data directory and atomically updates the <c>CURRENT</c> pointer
    /// to reference it. Old manifest files are then trimmed according to retention settings.
    /// </summary>
    /// <param name="manifest">
    /// The in-memory manifest snapshot to write to disk.
    /// </param>
    /// <remarks>
    ///     <para>The operation performs three steps under an internal lock (thread-safe):</para>
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
    /// <exception cref="IOException">
    /// An I/O error occurred while writing the manifest or updating <c>CURRENT</c>.
    /// </exception>
    /// <exception cref="UnauthorizedAccessException">
    /// The process lacks filesystem permissions for the data directory or files.
    /// </exception>
    /// <exception cref="JsonException">
    /// The <paramref name="manifest" /> could not be serialized.
    /// </exception>
    /// <exception cref="NotSupportedException">
    /// The manifest contains a value that cannot be serialized by the configured JSON options.
    /// </exception>
    public void Write(Manifest manifest)
    {
        lock (_lock)
        {
            _ = DirectoryEx.CreateDirectory(_dataDir);

            var baselineIndex = ResolveBaselineManifestIndex();
            var nextIndex = baselineIndex + 1;
            var fileName = $"{StorageFilePrefixes.Manifest}{nextIndex:D6}{StorageFileExtensions.Manifest}";
            var targetPath = PathEx.Combine(_dataDir, fileName);

            // 1) Write a new manifest file
            using (var fs = new FileStream(targetPath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                JsonSerializer.Serialize(fs, manifest, SquirixJsonSerializerContext.Default.Manifest);
                fs.Flush(true);
            }

            // 2) Atomically update CURRENT to point to the new manifest file
            UpdateCurrentAtomically(fileName);

            // 3) Retention: keep only the last N manifest files
            TryCleanupOldManifests();
            TryCleanupOldSnapshots(manifest.LastSnapshot);
            TryCleanupObsoleteJournalSegments(manifest);
        }
    }

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
        return int.TryParse(numberPart, out var n) ? n : 0;
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
        return int.TryParse(numberPart, out var n) ? n : 0;
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

    private int ResolveBaselineManifestIndex()
    {
        var maxOnDisk = ScanMaxManifestIndexOnDisk();

        if (!File.Exists(_currentPath))
            return maxOnDisk;

        string name;
        try
        {
            name = File.ReadAllText(_currentPath).Trim();
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
        return fromCurrent <= 0 ? throw new InvalidDataException($"Manifest current pointer is invalid: {_currentPath}") : fromCurrent > maxOnDisk ? fromCurrent : maxOnDisk;
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

    private void TryCleanupObsoleteJournalSegments(Manifest manifest)
    {
        try
        {
            var replayFromSegment = manifest.LastSnapshot?.ReplayFromJournalSegment ?? 0;
            if (replayFromSegment <= 1)
                return;

            if (manifest.CurrentJournal < replayFromSegment)
                return;

            foreach (var segment in JournalReader.EnumerateSegments(_dataDir, 1))
            {
                if (segment.Index >= replayFromSegment)
                    continue;

                if (segment.Index >= manifest.CurrentJournal)
                    continue;

                TryDeleteRetentionArtifact(segment.Path, ManifestRetentionArtifactKind.JournalSegment);
            }
        }
        catch (IOException ex)
        {
            ReportRetentionCleanupException(ManifestRetentionArtifactKind.JournalSegment, ex);
        }
        catch (UnauthorizedAccessException ex)
        {
            ReportRetentionCleanupException(ManifestRetentionArtifactKind.JournalSegment, ex);
        }
    }

    private void TryCleanupOldManifests()
    {
        try
        {
            var files = Directory.GetFiles(_dataDir, $"{StorageFilePrefixes.Manifest}*{StorageFileExtensions.Manifest}");
            if (files.Length <= _retention)
                return;

            var ordered = GetIndexedFiles(files, TryParseIndex);

            if (ordered.Length <= _retention)
                return;

            for (var i = _retention; i < ordered.Length; i++)
                TryDeleteRetentionArtifact(ordered[i].Path, ManifestRetentionArtifactKind.Manifest);
        }
        catch (IOException ex)
        {
            ReportRetentionCleanupException(ManifestRetentionArtifactKind.Manifest, ex);
        }
        catch (UnauthorizedAccessException ex)
        {
            ReportRetentionCleanupException(ManifestRetentionArtifactKind.Manifest, ex);
        }
    }

    private void TryCleanupOldSnapshots(Manifest.SnapshotRef? currentSnapshot)
    {
        try
        {
            var files = Directory.GetFiles(_dataDir, $"{StorageFilePrefixes.Snapshot}*{StorageFileExtensions.Snapshot}");
            if (files.Length <= _snapshotRetention)
                return;

            var ordered = GetIndexedFiles(files, TryParseSnapshotIndex);

            if (ordered.Length <= _snapshotRetention)
                return;

            var keep = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < _snapshotRetention && i < ordered.Length; i++)
                _ = keep.Add(ordered[i].Path);

            if (!string.IsNullOrWhiteSpace(currentSnapshot?.Path))
                _ = keep.Add(currentSnapshot.Path);

            for (var i = _snapshotRetention; i < ordered.Length; i++)
            {
                var stale = ordered[i];
                if (keep.Contains(stale.Path))
                    continue;

                TryDeleteRetentionArtifact(stale.Path, ManifestRetentionArtifactKind.Snapshot);
            }
        }
        catch (IOException ex)
        {
            ReportRetentionCleanupException(ManifestRetentionArtifactKind.Snapshot, ex);
        }
        catch (UnauthorizedAccessException ex)
        {
            ReportRetentionCleanupException(ManifestRetentionArtifactKind.Snapshot, ex);
        }
    }

    private void TryDeleteRetentionArtifact(string path, string artifactKind)
    {
        if (_fileOperations.TryDelete(path))
            return;

        ReportRetentionDeleteFailure(artifactKind, path);
    }

    private void UpdateCurrentAtomically(string newFileName)
    {
        var tmp = PathEx.Combine(_dataDir, $"{StorageFilePrefixes.Manifest}current.tmp");

        // Write tmp with explicit fsync semantics
        using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 4096, GetCurrentFileWriteOptions()))
        using (var sw = new StreamWriter(fs))
        {
            sw.Write(newFileName + Environment.NewLine);
            sw.Flush();
            fs.Flush(true);
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
