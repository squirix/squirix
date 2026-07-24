using System;
using System.IO;

namespace Squirix.Server.Utils;

/// <summary>Validates and resolves directory paths for <see cref="DirectoryEx" />.</summary>
internal static class DirectoryPathValidator
{
    private static readonly StringComparison SubPathComparison = OperatingSystem.IsWindows() || OperatingSystem.IsMacOS()
        ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

    /// <summary>Validates <paramref name="path" />, optionally constrains it under <paramref name="baseDir" />, and returns the absolute path.</summary>
    /// <param name="path">Target directory path.</param>
    /// <param name="baseDir">Optional base directory; when set, the target must remain under it.</param>
    /// <param name="forbidSymlinks">When <see langword="true" />, rejects symlinks in the path chain.</param>
    /// <returns>Normalized absolute path.</returns>
    /// <exception cref="ArgumentException">Thrown when the path is empty, contains invalid characters, or has invalid segments.</exception>
    /// <exception cref="UnauthorizedAccessException">Thrown when the resolved target escapes <paramref name="baseDir" />.</exception>
    /// <exception cref="IOException">Thrown when a file exists at the target or a forbidden symlink is detected.</exception>
    internal static string ResolveValidatedDirectoryPath(string path, string? baseDir, bool forbidSymlinks)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("Path must be a non-empty string.", nameof(path));

        PathValidation.ValidateNoInvalidChars(path, nameof(path));

        var baseFull = PrepareBaseDirectory(baseDir, forbidSymlinks);
        var full = ResolveFullPath(path, baseFull);

        if (baseFull is not null && !IsSubPathOf(full, baseFull))
            throw new UnauthorizedAccessException($"Target path escapes base directory: '{full}' not under '{baseFull}'.");

        ValidateSegments(full);

        if (forbidSymlinks)
            DirectorySymlinkGuard.EnsureNoSymlinksInChain(full, baseFull);

        if (File.Exists(full))
            throw new IOException($"A file already exists at '{full}'.");

        return full;
    }

    /// <summary>Returns whether <paramref name="value" /> is a directory separator.</summary>
    /// <param name="value">Character to test.</param>
    /// <returns><see langword="true" /> when the character is a directory separator.</returns>
    internal static bool IsDirectorySeparator(char value) =>
        value == Path.DirectorySeparatorChar || value == Path.AltDirectorySeparatorChar;

    /// <summary>Reads the next non-empty path segment from <paramref name="path" />.</summary>
    /// <param name="path">Remaining path span; advanced past the consumed segment.</param>
    /// <param name="segment">Consumed segment when this method returns <see langword="true" />.</param>
    /// <returns><see langword="true" /> when a segment was read.</returns>
    internal static bool TryReadNextSegment(ref ReadOnlySpan<char> path, out ReadOnlySpan<char> segment)
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
            segment = path;
            path = default;
            return !segment.IsEmpty;
        }

        segment = path[..end];
        path = path[(end + 1)..];
        return !segment.IsEmpty;
    }

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

    private static string? PrepareBaseDirectory(string? baseDir, bool forbidSymlinks)
    {
        if (string.IsNullOrWhiteSpace(baseDir))
            return null;

        PathValidation.ValidateNoInvalidChars(baseDir, nameof(baseDir));
        var baseFull = Path.GetFullPath(baseDir);

        if (forbidSymlinks)
        {
            var baseInfo = new DirectoryInfo(baseFull);
            if (baseInfo.Exists && DirectorySymlinkGuard.IsSymlink(baseInfo))
            {
                // Align with EnsureNoSymlinksInChain: allow only Darwin root compatibility links as DataDir bases.
                if (!MacOsCompatibilitySymlink.TryFollow(baseInfo, out var resolvedBase))
                    throw new IOException($"Base directory is a symlink/junction: '{baseFull}'.");

                baseFull = resolvedBase;
            }
        }

        ValidateSegments(baseFull);

        if (!Directory.Exists(baseFull))
            _ = Directory.CreateDirectory(baseFull);

        return baseFull;
    }

    private static string ResolveFullPath(string path, string? baseFull) =>
        Path.GetFullPath(Path.IsPathRooted(path) ? path : PathEx.Combine(baseFull ?? Environment.CurrentDirectory, path));

    private static void ValidateSegments(string fullPath)
    {
        var root = Path.GetPathRoot(fullPath) ?? string.Empty;
        var rest = fullPath.AsSpan(root.Length);
        while (TryReadNextSegment(ref rest, out var segment))
            PathValidation.ValidateSegment(segment, fullPath, nameof(fullPath), false);
    }
}
