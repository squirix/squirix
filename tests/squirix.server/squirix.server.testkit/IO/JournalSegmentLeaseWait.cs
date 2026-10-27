using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.TestKit.IO;

/// <summary>
/// Waits until persistence files in a data directory can be opened with the same sharing mode used by
/// writers (journal segments and the <c language="csharp">man-current</c> pointer).
/// </summary>
public static class JournalSegmentLeaseWait
{
    private const string JournalSegmentGlob = "jrn-*.jsqx";
    private const string ManifestCurrentFileName = "man-current";
    private const int PollIntervalMilliseconds = 25;
    private const long ReleaseBudgetMilliseconds = 10_000;

    /// <summary>
    /// Waits until journal segment files and <c language="csharp">man-current</c> in
    /// <paramref name="dataDir" /> are not locked incompatibly by another handle.
    /// </summary>
    /// <param name="dataDir">Node data directory containing journal segments and the manifest pointer.</param>
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
        return !File.Exists(path) || TryOpenRepairLease(path, cancellationToken);
    }

    private static async Task PollUntilPersistenceFilesReleasedAsync(string dataDir, CancellationToken cancellationToken)
    {
        var deadline = Environment.TickCount64 + ReleaseBudgetMilliseconds;
        while (!CanAcquireRepairLease(dataDir, cancellationToken))
        {
            if (Environment.TickCount64 >= deadline)
                throw new TimeoutException($"persistence files in '{dataDir}' remained locked after shutdown.");

            await Task.Delay(PollIntervalMilliseconds, cancellationToken).ConfigureAwait(false);
        }
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
