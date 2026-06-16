using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Squirix.E2EBenchmarks.Support.IO;

/// <summary>
/// Utilities for robust, cross-platform directory creation and cleanup,
/// with guardrails suitable for tests and tooling.
/// </summary>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Utility is shared across benchmark and test tooling.")]
public static class DirectoryKit
{
    /// <summary>
    /// Creates a new unique temporary directory under the system temp path.
    /// </summary>
    /// <param name="innerDirectory">A subfolder name under the system temp path.</param>
    /// <param name="hint">Optional additional subfolder appended for traceability.</param>
    /// <returns>The absolute path to the created directory.</returns>
    public static string CreateTempDirectory(string innerDirectory, [CallerMemberName] string? hint = null)
    {
        var directory = PathKit.Combine(Path.GetTempPath(), innerDirectory, Guid.NewGuid().ToString("N"));
        if (!string.IsNullOrEmpty(hint))
            directory = PathKit.Combine(directory, hint);

        _ = Directory.CreateDirectory(directory);
        return directory;
    }

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
    public static void TryDeleteDirectory(string? dir)
    {
        if (string.IsNullOrEmpty(dir))
            return;
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
