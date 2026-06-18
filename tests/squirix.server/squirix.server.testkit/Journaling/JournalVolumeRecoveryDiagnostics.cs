using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot.Binary;

namespace Squirix.Server.TestKit.Journaling;

/// <summary>Forensic helpers when journal volume recovery loses keys after restart.</summary>
public static class JournalVolumeRecoveryDiagnostics
{
    /// <summary>Builds a multi-line report of on-disk persistence state for a probe key.</summary>
    /// <param name="dataDir">Node persistence directory.</param>
    /// <param name="cacheNamespace">Cache namespace of the probe key.</param>
    /// <param name="probeKey">Key string to locate in snapshot and journal.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Human-readable diagnostic report.</returns>
    public static async Task<string> BuildReportAsync(
        string dataDir,
        string cacheNamespace,
        string probeKey,
        CancellationToken cancellationToken = default)
    {
        var report = new StringBuilder();
        _ = report.AppendLine(CultureInfo.InvariantCulture, $"dataDir={dataDir}");
        _ = report.AppendLine(CultureInfo.InvariantCulture, $"journalBytes={JournalStorageProbe.GetTotalJournalBytes(dataDir)}");
        _ = report.AppendLine(CultureInfo.InvariantCulture, $"journalSegments={JournalStorageProbe.CountJournalSegments(dataDir)}");

        using var manifestStore = new ManifestStore(new PersistenceOptions { DataDir = dataDir });
        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        _ = report.AppendLine(CultureInfo.InvariantCulture, $"manifest currentJournal={manifest.CurrentJournal} nextSequence={manifest.NextSequence}");

        var replayFrom = 1;
        if (manifest.LastSnapshot is { } snapshotRef)
        {
            replayFrom = snapshotRef.ReplayFromJournalSegment > 0 ? snapshotRef.ReplayFromJournalSegment : 1;
            _ = report.AppendLine(
                CultureInfo.InvariantCulture,
                $"snapshot index={snapshotRef.Index} lastAppliedSeq={snapshotRef.LastAppliedSequence} replayFromSeg={snapshotRef.ReplayFromJournalSegment} path={snapshotRef.Path}");
            _ = report.AppendLine(
                CultureInfo.InvariantCulture,
                $"probeInLatestSnapshot={await KeyInSnapshotAsync(snapshotRef.Path, cacheNamespace, probeKey, cancellationToken).ConfigureAwait(false)}");
        }
        else
        {
            _ = report.AppendLine("snapshot=none");
        }

        var (fullFound, fullSeq) = JournalCompactionProbe.FindKeyInJournal(dataDir, cacheNamespace, probeKey);
        _ = report.AppendLine(
            CultureInfo.InvariantCulture,
            $"probeInFullJournal(fromSeg=1) found={fullFound} lastSeq={fullSeq}");

        var (tailFound, tailSeq) = JournalCompactionProbe.FindKeyInJournal(dataDir, cacheNamespace, probeKey, replayFrom);
        _ = report.AppendLine(
            CultureInfo.InvariantCulture,
            $"probeInReplayTail(fromSeg={replayFrom}) found={tailFound} lastSeq={tailSeq}");

        return report.ToString();
    }

    private static async Task<bool> KeyInSnapshotAsync(string? path, string cacheNamespace, string probeKey, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            return false;

        var snapshot = await StoreFactory.CreateReader(new PersistenceOptions { DataDir = string.Empty })
            .LoadStrictAsync<byte[]>(path, true, cancellationToken).ConfigureAwait(false);
        foreach (var (key, _) in snapshot.Entries)
        {
            if (string.Equals(key.Namespace, cacheNamespace, StringComparison.Ordinal)
                && string.Equals(key.Key, probeKey, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
