using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage;

namespace Squirix.Server.TestKit.IO;

/// <summary>
/// Waits until journal segment files in a data directory can be opened with the same sharing mode used during writer startup.
/// </summary>
public static class JournalSegmentLeaseWait
{
    private const int BufferSize = 64 * 1024;

    /// <summary>
    /// Waits until all journal segment files in <paramref name="dataDir" /> are not locked by another handle.
    /// </summary>
    /// <param name="dataDir">Node data directory containing journal segments.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the journal files are available.</returns>
    /// <exception cref="TimeoutException">Thrown when the files remain locked until the wait budget expires.</exception>
    public static async Task WaitForReleasedAsync(string dataDir, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataDir);

        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (CanAcquireRepairLease(dataDir))
                return;

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException($"journal segments in '{dataDir}' remained locked after shutdown.");
    }

    private static bool CanAcquireRepairLease(string dataDir)
    {
        if (!Directory.Exists(dataDir))
            return true;

        var files = Directory.GetFiles(dataDir, StorageFilePrefixes.JournalSegmentGlob);
        if (files.Length == 0)
            return true;

        for (var i = 0; i < files.Length; i++)
        {
            try
            {
                using var stream = new FileStream(files[i], FileMode.Open, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete, BufferSize, FileOptions.None);
            }
            catch (IOException)
            {
                return false;
            }
        }

        return true;
    }
}
