using System;
using System.IO;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.Endpoint;

/// <summary>
/// Builds admin storage diagnostics snapshots from persistence runtime state.
/// </summary>
internal sealed class AdminStorageDiagnosticsProvider : IAdminStorageDiagnostics
{
    private readonly IJournalCoordinator _journal;
    private readonly ManifestStore _manifestStore;
    private readonly PersistenceOptions _persistence;

    public AdminStorageDiagnosticsProvider(ManifestStore manifestStore, IJournalCoordinator journal, PersistenceOptions persistence)
    {
        _manifestStore = manifestStore ?? throw new ArgumentNullException(nameof(manifestStore));
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
        _persistence = persistence ?? throw new ArgumentNullException(nameof(persistence));
    }

    /// <inheritdoc />
    public AdminStorageDiagnosticsSnapshot GetSnapshot(int recentSegmentLimit)
    {
        var manifest = _manifestStore.ReadCurrentOrDefault();
        var segmentArray = JournalReader.SelectNewestSegments(_persistence.DataDir, 1, recentSegmentLimit);
        var segmentCount = segmentArray.Length;
        var segments = new AdminJournalSegmentDiagnosticsSnapshot[segmentCount];
        for (var i = 0; i < segmentCount; i++)
            segments[i] = CreateJournalSegmentDiagnostic(segmentArray[segmentCount - 1 - i]);

        return new AdminStorageDiagnosticsSnapshot
        {
            DataDir = _persistence.DataDir,
            Manifest = MapManifest(manifest),
            Writer = new AdminJournalWriterDiagnosticsSnapshot(
                _journal.CurrentSegmentIndex,
                _journal.NextSequence,
                _journal.AppendedOps,
                _journal.AppendedBytes,
                _journal.RecentAppendLatencyMs),
            Journal = new AdminJournalDiagnosticsSnapshot
            {
                RecentSegmentLimit = recentSegmentLimit,
                Segments = segments,
            },
        };
    }

    private static AdminJournalSegmentDiagnosticsSnapshot CreateJournalSegmentDiagnostic(JournalSegment segment)
    {
        var file = new FileInfo(segment.Path);
        var headerValid = false;
        string? error = null;

        try
        {
            if (file.Exists)
            {
                using var fs = new FileStream(segment.Path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                headerValid = JournalFraming.TryReadAndValidateHeader(fs);
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            error = ex.GetType().Name;
        }

        return new AdminJournalSegmentDiagnosticsSnapshot(
            segment.Index,
            segment.Path,
            Path.GetFileName(segment.Path),
            file.Exists,
            file.Exists ? file.Length : 0L,
            file.Exists ? file.LastWriteTimeUtc : null,
            headerValid,
            error);
    }

    private static AdminManifestSnapshot MapManifest(Manifest manifest) => new()
    {
        CurrentJournal = manifest.CurrentJournal,
        Format = manifest.Format,
        NextSequence = manifest.NextSequence,
        LastSnapshot = manifest.LastSnapshot is null
            ? null
            : new AdminManifestSnapshotRef
            {
                CreatedUtc = manifest.LastSnapshot.CreatedUtc,
                Index = manifest.LastSnapshot.Index,
                LastAppliedSequence = manifest.LastSnapshot.LastAppliedSequence,
                Path = manifest.LastSnapshot.Path,
                ReplayFromJournalSegment = manifest.LastSnapshot.ReplayFromJournalSegment,
            },
    };
}
