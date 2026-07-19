using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.TestKit.IO;

/// <summary>
/// Utilities for robust, cross-platform directory creation and cleanup,
/// with guardrails suitable for tests and tooling.
/// </summary>
public static class DirectoryKit
{
    /// <summary>Safely creates a directory with strict validation and guardrails.</summary>
    /// <param name="path">Target directory path (relative or absolute).</param>
    /// <param name="baseDir">
    /// Optional base directory that constrains <paramref name="path" />. If provided and
    /// <paramref name="path" /> is relative, it is resolved against <paramref name="baseDir" />.
    /// The final directory must remain within <paramref name="baseDir" />.
    /// </param>
    /// <param name="ensureEmpty">
    /// When <see langword="true" />, existing directory contents are deleted (the directory itself is preserved).
    /// Refuses to clean a filesystem root.
    /// </param>
    /// <param name="forbidSymlinks">
    /// When <see langword="true" />, rejects symlinks/junctions in the parent chain and at the target.
    /// </param>
    /// <exception cref="ArgumentException">
    /// Thrown when the path is empty/whitespace, contains invalid characters or wildcards, has empty segments,
    /// uses Windows-reserved names, ends with dot/space on Windows, or when attempting to clean a filesystem root.
    /// </exception>
    /// <exception cref="UnauthorizedAccessException">
    /// Thrown when the resolved path escapes the provided <paramref name="baseDir" /> or OS denies access.
    /// </exception>
    /// <exception cref="IOException">
    /// Thrown when a regular file exists at the target path, a symlink/junction is detected while forbidden,
    /// or other I/O errors occur during creation/cleanup.
    /// </exception>
    /// <exception cref="PathTooLongException">May be thrown by underlying file APIs if the path exceeds platform limits.</exception>
    /// <remarks>
    ///     <para>
    ///     Behavior overview:
    ///     </para>
    ///     <list type="bullet">
    ///         <item>
    ///             <description>Validates <paramref name="path" /> (and <paramref name="baseDir" />) for invalid characters and wildcards.</description>
    ///         </item>
    ///         <item>
    ///             <description>Normalizes to an absolute path via <see cref="Path.GetFullPath(string)" />.</description>
    ///         </item>
    ///         <item>
    ///             <description>Enforces that the target remains under <paramref name="baseDir" /> when provided.</description>
    ///         </item>
    ///         <item>
    ///             <description>On Windows: rejects reserved device names (e.g., <c>CON</c>, <c>PRN</c>, <c>COM1</c>) and trailing dot/space.</description>
    ///         </item>
    ///         <item>
    ///             <description>Optionally rejects symlinks/junctions in the parent chain and at the target.</description>
    ///         </item>
    ///         <item>
    ///             <description>If <paramref name="ensureEmpty" /> is <see langword="true" />, removes files/subdirectories (not the root).</description>
    ///         </item>
    ///     </list>
    /// </remarks>
    public static void CreateDirectory(string path, string? baseDir = null, bool ensureEmpty = false, bool forbidSymlinks = true)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("Path must be a non-empty string.", nameof(path));

        PathValidationKit.ValidateNoInvalidChars(path);

        var baseFull = PrepareBaseFullPath(baseDir, forbidSymlinks);
        var full = Path.GetFullPath(Path.IsPathRooted(path) ? path : NodePathKit.Combine(baseFull ?? Environment.CurrentDirectory, path));

        if (baseFull is not null && !IsSubPathOf(full, baseFull))
            throw new UnauthorizedAccessException($"Target path escapes base directory: '{full}' not under '{baseFull}'.");

        PathValidationKit.ValidateSegments(full);

        if (forbidSymlinks)
            EnsureNoSymlinksInChain(full, baseFull);

        CreateOrCleanTargetDirectory(full, ensureEmpty, forbidSymlinks);
    }

    /// <summary>Creates a new unique temporary directory under the system temp path.</summary>
    /// <param name="innerDirectory">
    /// A subfolder name under <see cref="Path.GetTempPath()" /> used to group related temp directories.
    /// </param>
    /// <param name="hint">Optional additional subfolder (e.g., calling member name) appended for easier traceability in test logs.</param>
    /// <returns>The absolute path to the created directory.</returns>
    /// <remarks>
    /// The created path is of the form:
    /// <c>{Temp}\{innerDirectory}\{Guid:N}\[{hint}]</c>. Validation, normalization, and safety checks
    /// are delegated to <see cref="CreateDirectory(string,string?,bool,bool)" />.
    /// </remarks>
    /// <exception cref="ArgumentException">Propagated from <see cref="CreateDirectory(string,string?,bool,bool)" /> for invalid inputs.</exception>
    /// <exception cref="IOException">Propagated from <see cref="CreateDirectory(string,string?,bool,bool)" /> on I/O errors.</exception>
    /// <exception cref="UnauthorizedAccessException">Propagated from <see cref="CreateDirectory(string,string?,bool,bool)" /> on access errors.</exception>
    public static string CreateTempDirectory(string innerDirectory, [CallerMemberName] string? hint = null)
    {
        var d = string.IsNullOrEmpty(hint)
            ? Path.Join(Path.GetTempPath(), innerDirectory, Guid.NewGuid().ToString("N"))
            : Path.Join(Path.GetTempPath(), innerDirectory, Guid.NewGuid().ToString("N"), hint);
        CreateDirectory(d);
        return d;
    }

    /// <summary>Best-effort recursive delete of a directory.</summary>
    /// <param name="dir">Path to the directory to delete recursively.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <remarks>
    /// Performs up to 6 retries on transient <see cref="IOException" /> and
    /// <see cref="UnauthorizedAccessException" /> (common on Windows due to file locks).
    /// If the directory still exists after retries, a final delete is attempted and any resulting
    /// exception is allowed to bubble up.
    /// </remarks>
    /// <exception cref="IOException">May be thrown by the final delete if files remain locked or for other I/O errors.</exception>
    /// <exception cref="UnauthorizedAccessException">May be thrown by the final delete if access is denied.</exception>
    public static Task DeleteDirectoryAsync(string dir, CancellationToken cancellationToken = default) => DeleteDirectoryCoreAsync(dir, cancellationToken);

    /// <summary>Best-effort recursive delete of a directory.</summary>
    /// <param name="dir">Path to the directory to delete recursively.</param>
    /// <remarks>Prefer <see cref="DeleteDirectoryAsync" /> in async code paths.</remarks>
    public static void DeleteDirectory(string dir)
    {
        for (var i = 0; i < 6; i++)
            try
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, true);

                return;
            }
            catch (IOException) when (i < 5)
            {
                // Retry after transient delete failure.
            }
            catch (UnauthorizedAccessException) when (i < 5)
            {
                // Retry after transient access failure.
            }

        if (Directory.Exists(dir))
            Directory.Delete(dir, true);
    }

    private static async Task DeleteDirectoryCoreAsync(string dir, CancellationToken cancellationToken)
    {
        for (var i = 0; i < 6; i++)
        {
            try
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, true);

                return;
            }
            catch (IOException) when (i < 5)
            {
                await Task.Delay(25 * (i + 1), cancellationToken).ConfigureAwait(false);
            }
            catch (UnauthorizedAccessException) when (i < 5)
            {
                await Task.Delay(25 * (i + 1), cancellationToken).ConfigureAwait(false);
            }
        }

        if (Directory.Exists(dir))
            Directory.Delete(dir, true);
    }

    private static void CleanDirectoryContents(string dir, bool forbidSymlinks)
    {
        // Delete contents (not the root). Retry a few times for Windows file locks.
        const int retries = 3;

        for (var attempt = 0; attempt < retries; attempt++)
            try
            {
                var files = Directory.GetFiles(dir, "*", SearchOption.TopDirectoryOnly);
                for (var fi = 0; fi < files.Length; fi++)
                {
                    var f = files[fi];
                    ClearReadOnlyAttributes(f);
                    File.Delete(f);
                }

                var directories = Directory.GetDirectories(dir, "*", SearchOption.TopDirectoryOnly);
                for (var di = 0; di < directories.Length; di++)
                {
                    var d = directories[di];
                    var directoryInfo = new DirectoryInfo(d);
                    if (forbidSymlinks && IsSymlink(directoryInfo))
                        throw new IOException($"Refusing to descend into symlink/junction: '{d}'.");

                    Directory.Delete(d, true);
                }

                return;
            }
            catch (IOException) when (attempt < retries - 1)
            {
                // Retry after transient cleanup failure.
            }
            catch (UnauthorizedAccessException) when (attempt < retries - 1)
            {
                // Retry after transient access failure.
            }
    }

    private static void ClearReadOnlyAttributes(string file)
    {
        try
        {
            var attrs = File.GetAttributes(file);
            if ((attrs & FileAttributes.ReadOnly) is not FileAttributes.None)
                File.SetAttributes(file, attrs & ~FileAttributes.ReadOnly);
        }
        catch (IOException)
        {
            // ignore
        }
        catch (UnauthorizedAccessException)
        {
            // ignore
        }
    }

    private static void CreateOrCleanTargetDirectory(string full, bool ensureEmpty, bool forbidSymlinks)
    {
        if (File.Exists(full))
            throw new IOException($"A file already exists at '{full}'.");

        if (!Directory.Exists(full))
        {
            _ = Directory.CreateDirectory(full);

            if (!forbidSymlinks)
                return;

            var di = new DirectoryInfo(full);
            if (IsSymlink(di))
                throw new IOException($"Created directory resolved to a symlink/junction: '{full}'.");
        }
        else if (ensureEmpty)
        {
            var root = Path.GetPathRoot(full) ?? string.Empty;
            if (string.Equals(full.TrimEnd(Path.DirectorySeparatorChar), root.TrimEnd(Path.DirectorySeparatorChar), StringComparison.OrdinalIgnoreCase))
                throw new IOException("Refusing to clean a filesystem root.");

            CleanDirectoryContents(full, forbidSymlinks);
        }
    }

    private static async Task DeleteDirectoryCoreAsync(string dir, CancellationToken cancellationToken)
    {
        for (var i = 0; i < 6; i++)
            try
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, true);

                return;
            }
            catch (IOException) when (i < 5)
            {
                await Task.Delay(25 * (i + 1), cancellationToken).ConfigureAwait(false);
            }
            catch (UnauthorizedAccessException) when (i < 5)
            {
                await Task.Delay(25 * (i + 1), cancellationToken).ConfigureAwait(false);
            }

        if (Directory.Exists(dir))
            Directory.Delete(dir, true);
    }

    private static void EnsureNoSymlinksInChain(string full, string? baseFull)
    {
        // Walk from base (if provided) or drive root towards the target, checking each existing segment.
        var start = baseFull ?? Path.GetPathRoot(full)!;
        var remainder = full.AsSpan(start.Length);
        while (!remainder.IsEmpty && IsDirectorySeparator(remainder[0]))
            remainder = remainder[1..];
        if (remainder.IsEmpty)
            return;

        var curLength = TrimTrailingSeparatorsLength(start.AsSpan());
        var cur = curLength == start.Length ? start : start[..curLength];

        while (!remainder.IsEmpty)
        {
            cur = NodePathKit.Combine(cur, p);
            var di = new DirectoryInfo(cur);
            if (!di.Exists)
                break; // Not yet existing — will be created as regular directories

            if (IsSymlink(di))
                throw new IOException($"Symlink/junction detected in path: '{cur}'.");
        }
    }

    private static bool IsDirectorySeparator(char value) =>
        value == Path.DirectorySeparatorChar || value == Path.AltDirectorySeparatorChar;

    private static int IndexOfDirectorySeparator(ReadOnlySpan<char> value)
    {
        var primary = value.IndexOf(Path.DirectorySeparatorChar);
        var alternate = value.IndexOf(Path.AltDirectorySeparatorChar);
        if (primary < 0)
            return alternate;
        if (alternate < 0)
            return primary;
        return primary < alternate ? primary : alternate;
    }

    private static int TrimTrailingSeparatorsLength(ReadOnlySpan<char> span)
    {
        var length = span.Length;
        while (length > 0 && IsDirectorySeparator(span[length - 1]))
            length--;
        return length;
    }

    private static bool IsSubPathOf(string candidateFull, string baseFull)
    {
        // Use case-insensitive comparison on Windows and macOS (default FS often case-insensitive),
        // strict case-sensitive on Linux.
        var ignoreCase = OperatingSystem.IsWindows() || OperatingSystem.IsMacOS();
        var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        var baseWithSep = baseFull.EndsWith(Path.DirectorySeparatorChar) ? baseFull : $"{baseFull}{Path.DirectorySeparatorChar}";
        return candidateFull.Equals(baseFull, comparison) || candidateFull.StartsWith(baseWithSep, comparison);
    }

    private static bool IsSymlink(FileSystemInfo fsi)
    {
        try
        {
            // .NET 6+ cross-platform symlink test
            if (fsi.LinkTarget is not null)
                return true;
        }
        catch (IOException)
        {
            // Some FS/providers may throw; fall back to attributes
        }
        catch (UnauthorizedAccessException)
        {
            // Some FS/providers may throw; fall back to attributes
        }
        catch (NotSupportedException)
        {
            // LinkTarget may be unsupported on some providers; fall back to attributes
        }

        try
        {
            return (fsi.Attributes & FileAttributes.ReparsePoint) is not FileAttributes.None;
        }
        catch (IOException)
        {
            return false;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }
    }

    private static string? PrepareBaseFullPath(string? baseDir, bool forbidSymlinks)
    {
        if (string.IsNullOrWhiteSpace(baseDir))
            return null;

        PathValidationKit.ValidateNoInvalidChars(baseDir);
        var baseFull = Path.GetFullPath(baseDir);

        if (forbidSymlinks)
        {
            var baseInfo = new DirectoryInfo(baseFull);
            if (baseInfo.Exists && IsSymlink(baseInfo))
                throw new IOException($"Base directory is a symlink/junction: '{baseFull}'.");
        }

        if (!Directory.Exists(baseFull))
            _ = Directory.CreateDirectory(baseFull);

        return baseFull;
    }

    private static void ClearReadOnlyAttributes(string file)
    {
        try
        {
            var attrs = File.GetAttributes(file);
            if ((attrs & FileAttributes.ReadOnly) is not FileAttributes.None)
                File.SetAttributes(file, attrs & ~FileAttributes.ReadOnly);
        }
        catch (IOException)
        {
            // ignore
        }
        catch (UnauthorizedAccessException)
        {
            // ignore
        }
    }
}
