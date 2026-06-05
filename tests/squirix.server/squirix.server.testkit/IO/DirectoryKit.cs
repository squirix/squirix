using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Squirix.Server.TestKit.IO;

/// <summary>
/// Utilities for robust, cross-platform directory creation and cleanup,
/// with guardrails suitable for tests and tooling.
/// </summary>
public static class DirectoryKit
{
    /// <summary>
    /// Counts files in the specified directory matching the provided search pattern.
    /// </summary>
    /// <param name="dir">Directory path to inspect.</param>
    /// <param name="searchPattern">Search pattern passed to <see cref="Directory.GetFiles(string,string)" />.</param>
    /// <returns>The number of matching files, or 0 if the directory does not exist.</returns>
    public static int CountFiles(string dir, string searchPattern)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dir);
        ArgumentException.ThrowIfNullOrWhiteSpace(searchPattern);

        return Directory.Exists(dir) ? Directory.GetFiles(dir, searchPattern).Length : 0;
    }

    /// <summary>
    /// Safely creates a directory with strict validation and guardrails.
    /// </summary>
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
    /// <exception cref="PathTooLongException">
    /// May be thrown by underlying file APIs if the path exceeds platform limits.
    /// </exception>
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

        ValidateNoInvalidChars(path);

        // Prepare a base directory (if provided)
        string? baseFull = null;
        if (!string.IsNullOrWhiteSpace(baseDir))
        {
            ValidateNoInvalidChars(baseDir);
            baseFull = Path.GetFullPath(baseDir);

            // Optionally: ensure the base is not a symlink if symlinks are forbidden
            if (forbidSymlinks)
            {
                var baseInfo = new DirectoryInfo(baseFull);
                if (baseInfo.Exists && IsSymlink(baseInfo))
                    throw new IOException($"Base directory is a symlink/junction: '{baseFull}'.");
            }

            if (!Directory.Exists(baseFull))
                _ = Directory.CreateDirectory(baseFull);
        }

        // Build absolute path
        var full = Path.GetFullPath(Path.IsPathRooted(path) ? path : PathKit.Combine(baseFull ?? Environment.CurrentDirectory, path));

        // Ensure the target is under baseDir (if baseDir set)
        if (baseFull is not null && !IsSubPathOf(full, baseFull))
            throw new UnauthorizedAccessException($"Target path escapes base directory: '{full}' not under '{baseFull}'.");

        // Validate path segments (reserved names etc.)
        ValidateSegments(full);

        // Check the parent chain for symlinks (optional)
        if (forbidSymlinks)
            EnsureNoSymlinksInChain(full, baseFull);

        // Create or clean
        if (File.Exists(full))
            throw new IOException($"A file already exists at '{full}'.");

        if (!Directory.Exists(full))
        {
            _ = Directory.CreateDirectory(full);

            // Ensure the created target is not a symlink
            if (!forbidSymlinks)
                return;

            var di = new DirectoryInfo(full);
            if (IsSymlink(di))
                throw new IOException($"Created directory resolved to a symlink/junction: '{full}'.");
        }
        else if (ensureEmpty)
        {
            // Extra safety: do not allow cleaning the root or drive root
            var root = Path.GetPathRoot(full) ?? string.Empty;
            if (string.Equals(full.TrimEnd(Path.DirectorySeparatorChar), root.TrimEnd(Path.DirectorySeparatorChar), StringComparison.OrdinalIgnoreCase))
                throw new IOException("Refusing to clean a filesystem root.");

            CleanDirectoryContents(full, forbidSymlinks);
        }
    }

    /// <summary>
    /// Creates a new unique temporary directory under the system temp path.
    /// </summary>
    /// <param name="innerDirectory">
    /// A subfolder name under <see cref="Path.GetTempPath()" /> used to group related temp directories.
    /// </param>
    /// <param name="hint">
    /// Optional additional subfolder (e.g., calling member name) appended for easier traceability in test logs.
    /// </param>
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
        var d = PathKit.Combine(Path.GetTempPath(), innerDirectory, Guid.NewGuid().ToString("N"));
        if (!string.IsNullOrEmpty(hint))
            d = PathKit.Combine(d, hint);
        CreateDirectory(d);
        return d;
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

    private static void CleanDirectoryContents(string dir, bool forbidSymlinks)
    {
        // Delete contents (not the root). Retry a few times for Windows file locks.
        const int retries = 3;
        const int delayMs = 80;

        for (var attempt = 0; attempt < retries; attempt++)
        {
            try
            {
                // Files
                foreach (var f in Directory.EnumerateFiles(dir, "*", SearchOption.TopDirectoryOnly))
                {
                    TryMakeWritable(f);
                    File.Delete(f);
                }

                // Dirs
                foreach (var d in Directory.EnumerateDirectories(dir, "*", SearchOption.TopDirectoryOnly))
                {
                    var di = new DirectoryInfo(d);
                    if (forbidSymlinks && IsSymlink(di))
                        throw new IOException($"Refusing to descend into symlink/junction: '{d}'.");

                    Directory.Delete(d, true);
                }

                return;
            }
            catch when (attempt < retries - 1)
            {
                Thread.Sleep(delayMs);
            }
        }
    }

    private static void EnsureNoSymlinksInChain(string full, string? baseFull)
    {
        // Walk from base (if provided) or drive root towards the target, checking each existing segment.
        var start = baseFull ?? Path.GetPathRoot(full)!;
        var relative = full[start.Length..].TrimStart(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        if (relative.Length == 0)
            return;

        var parts = relative.Split([Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar], StringSplitOptions.RemoveEmptyEntries);
        var cur = start.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

        foreach (var p in parts)
        {
            cur = PathKit.Combine(cur, p);
            var di = new DirectoryInfo(cur);
            if (!di.Exists)
                break; // Not yet existing — will be created as regular directories

            if (IsSymlink(di))
                throw new IOException($"Symlink/junction detected in path: '{cur}'.");
        }
    }

    private static bool IsSubPathOf(string candidateFull, string baseFull)
    {
        // Use case-insensitive comparison on Windows and macOS (default FS often case-insensitive),
        // strict case-sensitive on Linux.
        var ignoreCase = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) || RuntimeInformation.IsOSPlatform(OSPlatform.OSX);
        var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        var baseWithSep = baseFull.EndsWith(Path.DirectorySeparatorChar) ? baseFull : baseFull + Path.DirectorySeparatorChar;
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
        catch
        {
            // Some FS/providers may throw; fall back to attributes
        }

        try
        {
            return (fsi.Attributes & FileAttributes.ReparsePoint) == FileAttributes.ReparsePoint;
        }
        catch
        {
            return false;
        }
    }

    private static bool IsWindowsReservedName(string seg)
    {
        // Check name without extension
        var name = seg;
        var dot = seg.IndexOf('.');
        if (dot > 0)
            name = seg[..dot];

        string[] fixedNames = ["CON", "PRN", "AUX", "NUL"];
        if (fixedNames.Any(n => string.Equals(name, n, StringComparison.OrdinalIgnoreCase)))
            return true;

        if (name.Length < 4)
            return false;

        var prefix = name[..3].ToUpperInvariant();
        return prefix is "COM" or "LPT" && int.TryParse(name.AsSpan(3), out var num) && num is >= 0 and <= 9;
    }

    private static void TryMakeWritable(string file)
    {
        try
        {
            var attrs = File.GetAttributes(file);
            if (attrs.HasFlag(FileAttributes.ReadOnly))
                File.SetAttributes(file, attrs & ~FileAttributes.ReadOnly);
        }
        catch
        {
            // ignore
        }
    }

    private static void ValidateNoInvalidChars(string path)
    {
        if (path.IndexOfAny(Path.GetInvalidPathChars()) >= 0)
            throw new ArgumentException($"Path contains invalid characters: '{path}'.", nameof(path));

        // Wildcards typically indicate a glob, not a concrete path
        if (path.Contains('*') || path.Contains('?'))
            throw new ArgumentException("Path must not contain wildcards (* or ?).", nameof(path));
    }

    private static void ValidateSegments(string fullPath)
    {
        var root = Path.GetPathRoot(fullPath) ?? string.Empty;
        var rest = fullPath[root.Length..];
        var segments = rest.Split([Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar], StringSplitOptions.RemoveEmptyEntries);

        foreach (var rawSeg in segments)
        {
            var seg = rawSeg.Trim();
            if (seg.Length == 0)
                throw new ArgumentException($"Empty segment in path: '{fullPath}'.");

            // Windows-only constraints
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                if (seg.EndsWith(' ') || seg.EndsWith('.'))
                    throw new ArgumentException($"Segment ends with space or dot: '{seg}' in '{fullPath}'.");

                if (IsWindowsReservedName(seg))
                    throw new ArgumentException($"Segment is a reserved Windows name: '{seg}' in '{fullPath}'.");
            }

            // File-name level invalid chars (cross-platform)
            if (seg.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
                throw new ArgumentException($"Segment contains invalid characters: '{seg}' in '{fullPath}'.");
        }
    }
}
