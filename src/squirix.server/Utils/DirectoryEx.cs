using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Utils;

internal static class DirectoryEx
{
    private static readonly char[] InvalidFileNameChars = Path.GetInvalidFileNameChars();
    private static readonly char[] InvalidPathChars = Path.GetInvalidPathChars();

    private static readonly StringComparison SubPathComparison = OperatingSystem.IsWindows() || OperatingSystem.IsMacOS()
        ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

    /// <summary>Safely creates a directory with strict validation and returns its normalized absolute path.</summary>
    /// <param name="path">
    /// The target directory path. May be relative or absolute. Must not be <see langword="null" />, empty, or whitespace,
    /// and must not contain invalid characters or wildcards.
    /// </param>
    /// <param name="baseDir">
    /// Optional base directory used to resolve a relative <paramref name="path" />. When provided,
    /// the resulting target must reside within this base directory (the method throws otherwise).
    /// If <paramref name="baseDir" /> does not exist, it is created.
    /// </param>
    /// <param name="forbidSymlinks">
    /// When <see langword="true" /> (default), forbids symbolic links/junctions both in the parent chain and at the
    /// target directory; the method throws if a link is detected. When <see langword="false" />, link checks are skipped.
    /// </param>
    /// <returns>The normalized absolute path of the created (or already existing) directory.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown if <paramref name="path" /> (or <paramref name="baseDir" /> when provided) is empty or contains invalid characters.
    /// </exception>
    /// <exception cref="UnauthorizedAccessException">
    /// Thrown when the resolved target escapes <paramref name="baseDir" /> or the process lacks permissions to create/clean the directory.
    /// </exception>
    /// <exception cref="IOException">
    /// Thrown if a file exists at the target path, a forbidden symlink/junction is detected (when
    /// <paramref name="forbidSymlinks" /> is <see langword="true" />), or the target resolves to a link after creation.
    /// </exception>
    /// <remarks>
    ///     <para>
    ///     The method performs the following steps:
    ///     (1) validates inputs; (2) resolves an absolute path (combining with <paramref name="baseDir" /> or current working directory
    ///     for relative inputs); (3) ensures the target is within <paramref name="baseDir" /> if provided;
    ///     (4) validates path segments (e.g., on Windows: reserved names like <c>CON</c>, <c>PRN</c>, trailing dot/space);
    ///     (5) optionally checks for symlinks/junctions; (6) creates the directory when it does not exist.
    ///     </para>
    ///     <para>
    ///     This routine minimizes directory traversal and link attacks by rejecting targets that escape the base directory
    ///     and, by default, forbidding symlinks. Use the returned path immediately for subsequent operations.
    ///     </para>
    /// </remarks>
    internal static string CreateDirectory(string path, string? baseDir = null, bool forbidSymlinks = true)
    {
        var full = ResolveValidatedDirectoryPath(path, baseDir, forbidSymlinks);
        if (!Directory.Exists(full))
        {
            _ = Directory.CreateDirectory(full);
            EnsureRegularDirectory(full, true, forbidSymlinks);
            return full;
        }

        EnsureRegularDirectory(full, false, forbidSymlinks);
        return full;
    }

    /// <summary>Safely creates a directory with strict validation and returns its normalized absolute path.</summary>
    /// <param name="path">The target directory path.</param>
    /// <param name="baseDir">Optional base directory used to resolve a relative <paramref name="path" />.</param>
    /// <param name="ensureEmpty">When <see langword="true" />, deletes existing contents of an already-present directory.</param>
    /// <param name="forbidSymlinks">When <see langword="true" />, forbids symbolic links/junctions in the path chain.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The normalized absolute path of the created (or already existing) directory.</returns>
    internal static Task<string> CreateDirectoryAsync(
        string path,
        string? baseDir = null,
        bool ensureEmpty = false,
        bool forbidSymlinks = true,
        CancellationToken cancellationToken = default) => EnsureDirectoryReadyAsync(
        ResolveValidatedDirectoryPath(path, baseDir, forbidSymlinks),
        ensureEmpty,
        forbidSymlinks,
        cancellationToken);

    private static async Task CleanDirectoryContentsAsync(string dir, bool forbidSymlinks, CancellationToken cancellationToken)
    {
        // Delete contents (not the root). Retry a few times for Windows file locks.
        const int retries = 3;
        const int delayMs = 80;

        for (var attempt = 0; attempt < retries; attempt++)
        {
            try
            {
                var files = Directory.GetFiles(dir, "*", SearchOption.TopDirectoryOnly);
                for (var i = 0; i < files.Length; i++)
                {
                    var f = files[i];
                    ClearReadOnlyAttributes(f);
                    File.Delete(f);
                }

                var directories = Directory.GetDirectories(dir, "*", SearchOption.TopDirectoryOnly);
                for (var i = 0; i < directories.Length; i++)
                {
                    var d = directories[i];
                    var di = new DirectoryInfo(d);
                    if (forbidSymlinks && IsSymlink(di))
                        throw new IOException($"Refusing to descend into symlink/junction: '{d}'.");

                    Directory.Delete(d, true);
                }

                return;
            }
            catch (IOException) when (attempt < retries - 1)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(delayMs), TimeProvider.System, cancellationToken).ConfigureAwait(false);
            }
            catch (UnauthorizedAccessException) when (attempt < retries - 1)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(delayMs), TimeProvider.System, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static async Task<string> EnsureDirectoryReadyAsync(string full, bool ensureEmpty, bool forbidSymlinks, CancellationToken cancellationToken)
    {
        if (!Directory.Exists(full))
        {
            _ = Directory.CreateDirectory(full);
            EnsureRegularDirectory(full, true, forbidSymlinks);
            return full;
        }

        EnsureRegularDirectory(full, false, forbidSymlinks);

        if (!ensureEmpty)
            return full;

        var root = Path.GetPathRoot(full) ?? string.Empty;
        if (string.Equals(full.TrimEnd(Path.DirectorySeparatorChar), root.TrimEnd(Path.DirectorySeparatorChar), StringComparison.OrdinalIgnoreCase))
            throw new IOException("Refusing to clean a filesystem root.");

        await CleanDirectoryContentsAsync(full, forbidSymlinks, cancellationToken).ConfigureAwait(false);
        return full;
    }

    private static void EnsureNoSymlinksInChain(string full, string? baseFull)
    {
        // Walk from base (if provided) or drive root towards the target, checking each existing segment.
        var start = baseFull ?? Path.GetPathRoot(full)!;
        var relative = full.AsSpan(start.Length);
        while (relative.Length > 0 && IsDirectorySeparator(relative[0]))
            relative = relative[1..];

        if (relative.IsEmpty)
            return;

        var trimmedStart = start.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

        // Trimming trailing separators can turn a root-only path into an empty string
        // (for example "/" on Unix). PathEx.Combine cannot start from empty, so when
        // trimming empties a non-empty start, preserve the original root as the seed.
        var cur = trimmedStart.Length is 0 && start.Length > 0 ? start : trimmedStart;

        while (TryReadNextSegment(ref relative, out var segment))
        {
            cur = PathEx.Combine(cur, segment.ToString());
            var di = new DirectoryInfo(cur);
            if (!di.Exists) // Not yet existing — will be created as regular directories
                break;

            if (!IsSymlink(di))
                continue;

            // macOS ships compatibility symlinks (/var -> /private/var, /tmp -> /private/tmp, /etc -> /private/etc).
            // Rejecting them breaks every DataDir under Path.GetTempPath() (/var/folders/...). Follow only those
            // well-known OS links; any other symlink/junction in the chain remains forbidden.
            if (TryFollowMacOsCompatibilitySymlink(di, out var resolved))
            {
                cur = resolved;
                continue;
            }

            throw new IOException($"Symlink/junction detected in path: '{cur}'.");
        }
    }

    private static bool TryFollowMacOsCompatibilitySymlink(DirectoryInfo directory, out string resolvedFullPath)
    {
        resolvedFullPath = string.Empty;
        if (!OperatingSystem.IsMacOS() && !OperatingSystem.IsMacCatalyst())
            return false;

        // Only the three historical Darwin compatibility links at the filesystem root.
        var root = Path.GetPathRoot(directory.FullName);
        var parent = directory.Parent;
        if (root is null || parent is null)
            return false;

        var rootTrimmed = root.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var parentTrimmed = parent.FullName.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        if (!parentTrimmed.Equals(rootTrimmed, StringComparison.Ordinal)
            && !parent.FullName.Equals(root, StringComparison.Ordinal))
        {
            return false;
        }

        var name = directory.Name;
        if (name is not ("var" or "tmp" or "etc"))
            return false;

        FileSystemInfo? target;
        try
        {
            target = directory.ResolveLinkTarget(returnFinalTarget: true);
        }
        catch (IOException)
        {
            return false;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }
        catch (NotSupportedException)
        {
            return false;
        }

        if (target is null)
            return false;

        var expected = Path.Combine(root, "private", name);
        var expectedTrimmed = expected.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var targetFull = target.FullName.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        if (!targetFull.Equals(expectedTrimmed, StringComparison.OrdinalIgnoreCase))
            return false;

        resolvedFullPath = expectedTrimmed;
        return true;
    }

    private static void EnsureRegularDirectory(string full, bool created, bool forbidSymlinks)
    {
        if (!forbidSymlinks)
            return;

        var info = new DirectoryInfo(full);
        if (!IsSymlink(info))
            return;

        throw new IOException(created ? $"Created directory resolved to a symlink/junction: '{full}'." : $"Target directory is a symlink/junction: '{full}'.");
    }

    private static bool IsDirectorySeparator(char value) => value == Path.DirectorySeparatorChar || value == Path.AltDirectorySeparatorChar;

    private static bool IsSubPathOf(string candidateFull, string baseFull)
    {
        if (candidateFull.Equals(baseFull, SubPathComparison))
            return true;

        if (baseFull.EndsWith(Path.DirectorySeparatorChar))
            return candidateFull.StartsWith(baseFull, SubPathComparison);

        if (candidateFull.Length <= baseFull.Length)
            return false;

        if (!candidateFull.AsSpan(0, baseFull.Length).Equals(baseFull.AsSpan(), SubPathComparison))
            return false;

        return IsDirectorySeparator(candidateFull[baseFull.Length]);
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

    private static bool IsWindowsReservedName(ReadOnlySpan<char> segment)
    {
        var name = segment;
        var dot = segment.IndexOf('.');
        if (dot > 0)
        {
            name = segment[..dot];
        }

        if (name.Equals("CON", StringComparison.OrdinalIgnoreCase) || name.Equals("PRN", StringComparison.OrdinalIgnoreCase) ||
            name.Equals("AUX", StringComparison.OrdinalIgnoreCase) || name.Equals("NUL", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (name.Length < 4)
            return false;

        var prefix = name[..3];
        if (!prefix.Equals("COM", StringComparison.OrdinalIgnoreCase) && !prefix.Equals("LPT", StringComparison.OrdinalIgnoreCase))
            return false;

        return int.TryParse(name[3..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var num) && num is >= 0 and <= 9;
    }

    private static string? PrepareBaseDirectory(string? baseDir, bool forbidSymlinks)
    {
        if (string.IsNullOrWhiteSpace(baseDir))
            return null;

        ValidateNoInvalidChars(baseDir);
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

    private static string ResolveFullPath(string path, string? baseFull) =>
        Path.GetFullPath(Path.IsPathRooted(path) ? path : PathEx.Combine(baseFull ?? Environment.CurrentDirectory, path));

    private static string ResolveValidatedDirectoryPath(string path, string? baseDir, bool forbidSymlinks)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("Path must be a non-empty string.", nameof(path));

        ValidateNoInvalidChars(path);

        var baseFull = PrepareBaseDirectory(baseDir, forbidSymlinks);
        var full = ResolveFullPath(path, baseFull);

        if (baseFull is not null && !IsSubPathOf(full, baseFull))
            throw new UnauthorizedAccessException($"Target path escapes base directory: '{full}' not under '{baseFull}'.");

        ValidateSegments(full);

        if (forbidSymlinks)
            EnsureNoSymlinksInChain(full, baseFull);

        if (File.Exists(full))
            throw new IOException($"A file already exists at '{full}'.");

        return full;
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
            // Best-effort cleanup: inability to clear read-only attributes must not block deletion attempts.
        }
        catch (UnauthorizedAccessException)
        {
            // Best-effort cleanup: inability to clear read-only attributes must not block deletion attempts.
        }
    }

    private static bool TryReadNextSegment(ref ReadOnlySpan<char> path, out ReadOnlySpan<char> segment)
    {
        while (path.Length > 0 && IsDirectorySeparator(path[0]))
            path = path[1..];

        if (path.IsEmpty)
        {
            segment = default;
            return false;
        }

        var end = path.IndexOfAny(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        if (end < 0)
        {
            segment = path.Trim();
            path = default;
            return !segment.IsEmpty;
        }

        segment = path[..end].Trim();
        path = path[(end + 1)..];
        return !segment.IsEmpty;
    }

    private static void ValidateNoInvalidChars(string path)
    {
        if (path.AsSpan().IndexOfAny(InvalidPathChars) >= 0)
            throw new ArgumentException($"Path contains invalid characters: '{path}'.", nameof(path));

        // Wildcards typically indicate a glob, not a concrete path
        if (path.Contains('*', StringComparison.Ordinal) || path.Contains('?', StringComparison.Ordinal))
            throw new ArgumentException("Path must not contain wildcards (* or ?).", nameof(path));
    }

    private static void ValidateSegment(ReadOnlySpan<char> segment, string fullPath)
    {
        if (segment.IsEmpty)
            throw new ArgumentException($"Empty segment in path: '{fullPath}'.", nameof(fullPath));

        // Windows-only constraints
        if (OperatingSystem.IsWindows())
        {
            if (segment.EndsWith(' ') || segment.EndsWith('.'))
                throw new ArgumentException($"Segment ends with space or dot: '{segment}' in '{fullPath}'.", nameof(fullPath));

            if (IsWindowsReservedName(segment))
                throw new ArgumentException($"Segment is a reserved Windows name: '{segment}' in '{fullPath}'.", nameof(fullPath));
        }

        // File-name level invalid chars (cross-platform)
        if (segment.IndexOfAny(InvalidFileNameChars) >= 0)
            throw new ArgumentException($"Segment contains invalid characters: '{segment}' in '{fullPath}'.", nameof(fullPath));
    }

    private static void ValidateSegments(string fullPath)
    {
        var root = Path.GetPathRoot(fullPath) ?? string.Empty;
        var rest = fullPath.AsSpan(root.Length);
        while (TryReadNextSegment(ref rest, out var segment))
            ValidateSegment(segment, fullPath);
    }
}
