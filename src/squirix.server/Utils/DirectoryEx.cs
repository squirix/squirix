using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;

namespace Squirix.Server.Utils;

internal static class DirectoryEx
{
    /// <summary>
    /// Safely creates a directory with strict validation and returns its normalized absolute path.
    /// </summary>
    /// <param name="path">
    /// The target directory path. May be relative or absolute. Must not be <c>null</c>, empty, or whitespace,
    /// and must not contain invalid characters or wildcards.
    /// </param>
    /// <param name="baseDir">
    /// Optional base directory used to resolve a relative <paramref name="path" />. When provided,
    /// the resulting target must reside within this base directory (the method throws otherwise).
    /// If <paramref name="baseDir" /> does not exist, it is created.
    /// </param>
    /// <param name="ensureEmpty">
    /// When <c>true</c>, and the target directory already exists, its contents are removed recursively
    /// (fails for roots and when forbidden symlinks are encountered).
    /// </param>
    /// <param name="forbidSymlinks">
    /// When <c>true</c> (default), forbids symbolic links/junctions both in the parent chain and at the
    /// target directory; the method throws if a link is detected. When <c>false</c>, link checks are skipped.
    /// </param>
    /// <returns>
    /// The normalized absolute path of the created (or already existing) directory.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// Thrown if <paramref name="path" /> (or <paramref name="baseDir" /> when provided) is empty or contains invalid characters.
    /// </exception>
    /// <exception cref="UnauthorizedAccessException">
    /// Thrown when the resolved target escapes <paramref name="baseDir" /> or the process lacks permissions to create/clean the directory.
    /// </exception>
    /// <exception cref="IOException">
    /// Thrown if a file exists at the target path, a forbidden symlink/junction is detected (when
    /// <paramref name="forbidSymlinks" /> is <c>true</c>), the target resolves to a link after creation,
    /// or when attempting to clean a filesystem root.
    /// </exception>
    /// <remarks>
    ///     <para>
    ///     The method performs the following steps:
    ///     (1) validates inputs; (2) resolves an absolute path (combining with <paramref name="baseDir" /> or current working directory
    ///     for relative inputs); (3) ensures the target is within <paramref name="baseDir" /> if provided;
    ///     (4) validates path segments (e.g., on Windows: reserved names like <c>CON</c>, <c>PRN</c>, trailing dot/space);
    ///     (5) optionally checks for symlinks/junctions; (6) creates the directory or cleans it if it already exists
    ///     and <paramref name="ensureEmpty" /> is <c>true</c>.
    ///     </para>
    ///     <para>
    ///     This routine minimizes directory traversal and link attacks by rejecting targets that escape the base directory
    ///     and, by default, forbidding symlinks. Use the returned path immediately for subsequent operations.
    ///     </para>
    /// </remarks>
    public static string CreateDirectory(string path, string? baseDir = null, bool ensureEmpty = false, bool forbidSymlinks = true)
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

        var full = Path.GetFullPath(Path.IsPathRooted(path) ? path : PathEx.Combine(baseFull ?? Environment.CurrentDirectory, path));

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
                return full;

            var di = new DirectoryInfo(full);
            if (IsSymlink(di))
                throw new IOException($"Created directory resolved to a symlink/junction: '{full}'.");
        }
        else
        {
            if (forbidSymlinks)
            {
                var di = new DirectoryInfo(full);
                if (IsSymlink(di))
                    throw new IOException($"Target directory is a symlink/junction: '{full}'.");
            }

            if (!ensureEmpty)
                return full;

            // Extra safety: do not allow cleaning the root or drive root
            var root = Path.GetPathRoot(full) ?? string.Empty;
            if (string.Equals(full.TrimEnd(Path.DirectorySeparatorChar), root.TrimEnd(Path.DirectorySeparatorChar), StringComparison.OrdinalIgnoreCase))
                throw new IOException("Refusing to clean a filesystem root.");

            CleanDirectoryContents(full, forbidSymlinks);
        }

        return full;
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
            catch (IOException) when (attempt < retries - 1)
            {
                Thread.Sleep(delayMs);
            }
            catch (UnauthorizedAccessException) when (attempt < retries - 1)
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
        var trimmedStart = start.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

        // Trimming trailing separators can turn a root-only path into an empty string
        // (for example "/" on Unix). PathEx.Combine cannot start from empty, so when
        // trimming empties a non-empty start, preserve the original root as the seed.
        var cur = trimmedStart.Length == 0 && start.Length > 0 ? start : trimmedStart;

        foreach (var p in parts)
        {
            cur = PathEx.Combine(cur, p);
            var di = new DirectoryInfo(cur);
            if (!di.Exists) // Not yet existing — will be created as regular directories
                break;

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
            return (fsi.Attributes & FileAttributes.ReparsePoint) == FileAttributes.ReparsePoint;
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

    private static bool IsWindowsReservedName(string seg)
    {
        // Check name without extension
        var name = seg;
        var dot = seg.IndexOf('.', StringComparison.Ordinal);
        if (dot > 0)
            name = seg[..dot];

        string[] fixedNames = ["CON", "PRN", "AUX", "NUL"];
        foreach (var fixedName in fixedNames)
        {
            if (string.Equals(name, fixedName, StringComparison.OrdinalIgnoreCase))
                return true;
        }

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
        catch (IOException)
        {
            // Best-effort cleanup: inability to clear read-only attributes must not block deletion attempts.
        }
        catch (UnauthorizedAccessException)
        {
            // Best-effort cleanup: inability to clear read-only attributes must not block deletion attempts.
        }
    }

    private static void ValidateNoInvalidChars(string path)
    {
        if (path.IndexOfAny(Path.GetInvalidPathChars()) >= 0)
            throw new ArgumentException($"Path contains invalid characters: '{path}'.", nameof(path));

        // Wildcards typically indicate a glob, not a concrete path
        if (path.Contains('*', StringComparison.Ordinal) || path.Contains('?', StringComparison.Ordinal))
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
