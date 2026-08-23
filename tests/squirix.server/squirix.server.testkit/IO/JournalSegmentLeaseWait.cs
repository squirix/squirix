using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.TestKit.IO;

/// <summary>
/// Waits until persistence files in a data directory can be opened with the same sharing mode used by
/// writers (journal segments and the <c>man-current</c> pointer).
/// </summary>
public static class JournalSegmentLeaseWait
{
    private const string JournalSegmentGlob = "jrn-*.jsqx";
    private const string ManifestCurrentFileName = "man-current";

    /// <summary>
    /// Waits until journal segment files and <c>man-current</c> in <paramref name="dataDir" /> are not locked
    /// incompatibly by another handle.
    /// </summary>
    /// <param name="dataDir">Node data directory containing journal segments and manifest pointer.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="TimeoutException">Thrown when the files remain locked until the wait budget expires.</exception>
    public static Task WaitForReleasedAsync(string dataDir, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataDir);
        return PollUntilPersistenceFilesReleasedAsync(dataDir, cancellationToken);
    }

    private static async Task<bool> CanAcquireRepairLeaseAsync(string dataDir, CancellationToken cancellationToken)
    {
        if (!Directory.Exists(dataDir))
            return true;

        var files = Directory.GetFiles(dataDir, JournalSegmentGlob);
        for (var i = 0; i < files.Length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!await TryOpenRepairLeaseAsync(files[i], cancellationToken).ConfigureAwait(false))
                return false;
        }

        var currentPath = Path.Join(dataDir, ManifestCurrentFileName);
        if (!File.Exists(currentPath))
            return true;

        return await TryOpenRepairLeaseAsync(currentPath, cancellationToken).ConfigureAwait(false);
    }

    private static async Task PollUntilPersistenceFilesReleasedAsync(string dataDir, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (await CanAcquireRepairLeaseAsync(dataDir, cancellationToken).ConfigureAwait(false))
                return;

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException($"persistence files in '{dataDir}' remained locked after shutdown.");
    }

    private static async Task<bool> TryOpenRepairLeaseAsync(string path, CancellationToken cancellationToken)
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
