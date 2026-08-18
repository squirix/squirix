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

        if (baseFull != null && !IsSubPathOf(full, baseFull))
            throw new UnauthorizedAccessException("Target path escapes base directory.");

        ValidateSegments(full);

        if (forbidSymlinks)
            DirectorySymlinkGuard.EnsureNoSymlinksInChain(full, baseFull);

        if (File.Exists(full))
            throw new IOException("A file already exists at the target path.");

        return full;
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

        return DirectoryPathHelpers.IsDirectorySeparator(candidateFull[baseFull.Length]);
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
                    throw new IOException("Base directory is a symlink/junction.");

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
        while (PathEx.TryReadNextSegment(ref rest, out var segment))
            PathValidation.ValidateSegment(segment, nameof(fullPath), false);
    }
}
