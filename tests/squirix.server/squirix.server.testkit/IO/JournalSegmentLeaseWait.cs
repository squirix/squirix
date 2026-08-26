using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.TestKit.IO;

/// <summary>
/// Waits until persistence files in a data directory can be opened with the same sharing mode used by
/// writers (journal segments, the <c>man-current</c> pointer, and its <c>man-current.next</c> staging file).
/// </summary>
public static class JournalSegmentLeaseWait
{
    private const string JournalSegmentGlob = "jrn-*.jsqx";
    private const string ManifestCurrentFileName = "man-current";
    private const string ManifestCurrentStagingFileName = "man-current.next";
    private const int ProbeIntervalMilliseconds = 25;
    private const int RequiredCleanProbes = 4;

    /// <summary>
    /// Waits until journal segment files, <c>man-current</c>, and <c>man-current.next</c> in
    /// <paramref name="dataDir" /> are not locked incompatibly by another handle. Release must hold across
    /// several consecutive probes: the staging file only exists mid-roll, so a single clean pass cannot
    /// distinguish a quiet directory from the gap between a dying writer's rolls.
    /// </summary>
    /// <param name="dataDir">Node data directory containing journal segments and manifest pointer files.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="TimeoutException">Thrown when the files remain locked until the wait budget expires.</exception>
    public static Task WaitForReleasedAsync(string dataDir, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataDir);
        return PollUntilPersistenceFilesReleasedAsync(dataDir, cancellationToken);
    }

    private static bool CanAcquireRepairLease(string dataDir, CancellationToken cancellationToken)
    {
        if (!Directory.Exists(dataDir))
            return true;

        var files = Directory.GetFiles(dataDir, JournalSegmentGlob);
        for (var i = 0; i < files.Length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!TryOpenRepairLease(files[i], cancellationToken))
                return false;
        }

        var path = Path.Join(dataDir, ManifestCurrentFileName);
        if (File.Exists(path) && !TryOpenRepairLease(path, cancellationToken))
            return false;

        // The manifest pointer writer stages each update in man-current.next with an exclusive
        // (FileShare.None) handle before renaming it into place; an abrupt shutdown can leave a draining
        // handle on it. Probe with the same exclusive mode, so the staging file is shareable by the writer
        // during the offline compact that skipped reading it (issue #396).
        var join = Path.Join(dataDir, ManifestCurrentStagingFileName);
        if (!File.Exists(join))
            return true;

        var handle = TryOpenExclusiveStageHandle(join);
        cancellationToken.ThrowIfCancellationRequested();
        return handle;
    }

    private static bool TryOpenExclusiveStageHandle(string path)
    {
        try
        {
            using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Write, FileShare.None);
            return true;
        }
        catch (IOException)
        {
            return false;
        }
    }

    private static async Task PollUntilPersistenceFilesReleasedAsync(string dataDir, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        var cleanProbes = 0;
        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (CanAcquireRepairLease(dataDir, cancellationToken))
            {
                cleanProbes++;
                if (cleanProbes >= RequiredCleanProbes)
                    return;
            }
            else
            {
                cleanProbes = 0;
            }

            await Task.Delay(ProbeIntervalMilliseconds, cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException($"persistence files in '{dataDir}' remained locked after shutdown.");
    }

    private static bool TryOpenRepairLease(string path, CancellationToken cancellationToken)
    {
        try
        {
            using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
            cancellationToken.ThrowIfCancellationRequested();
            return true;
        }
        catch (IOException)
        {
            return false;
        }
    }
}
