using System;
using System.IO;
using System.Threading;

namespace Squirix.Benchmarks.Utils;

/// <summary>
/// Utilities for robust, cross-platform directory creation and cleanup,
/// with guardrails suitable for tests and tooling.
/// </summary>
public static class DirectoryKit
{
    /// <summary>
    /// Best-effort recursive delete of a directory.
    /// </summary>
    /// <param name="dir">Path to the directory to delete recursively.</param>
    /// <remarks>
    /// Performs up to 6 retries on transient <see cref="IOException" /> and
    /// <see cref="UnauthorizedAccessException" /> (common on Windows due to file locks).
    /// If the directory still exists after retries, a final delete is attempted and any resulting
    /// exception is allowed to bubble up.
    /// </remarks>
    /// <exception cref="IOException">May be thrown by the final delete if files remain locked or for other I/O errors.</exception>
    /// <exception cref="UnauthorizedAccessException">May be thrown by the final delete if access is denied.</exception>
    public static void TryDeleteDirectory(string dir)
    {
        for (var i = 0; i < 6; i++)
        {
            try
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, true);

                return;
            }
            catch (IOException)
            {
                Thread.Sleep(25 * (i + 1));
            }
            catch (UnauthorizedAccessException)
            {
                Thread.Sleep(25 * (i + 1));
            }
        }

        if (Directory.Exists(dir))
            Directory.Delete(dir, true);
    }
}
