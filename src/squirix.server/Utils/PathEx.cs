using System;
using System.IO;

namespace Squirix.Server.Utils;

/// <summary>
/// Provides helper methods for working with file system paths in a safe,
/// cross-platform way.
/// </summary>
/// <remarks>
/// The utilities in <see cref="PathEx" /> are intended to sanitize and manipulate
/// path segments (such as file names) rather than perform actual I/O.
/// They do not create or validate files or directories on disk.
/// </remarks>
internal static class PathEx
{
    private static readonly StringComparison PathComparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

    /// <summary>Resolves two relative path segments under a trusted root directory.</summary>
    /// <param name="rootDirectory">Trusted absolute root directory.</param>
    /// <param name="segment1">First relative segment.</param>
    /// <param name="segment2">Second relative segment.</param>
    /// <returns>Absolute normalized path under the root.</returns>
    internal static string Combine(string rootDirectory, string segment1, string segment2)
    {
        ArgumentNullException.ThrowIfNull(rootDirectory);
        ArgumentNullException.ThrowIfNull(segment1);
        ArgumentNullException.ThrowIfNull(segment2);

        ValidateSegment(segment1);
        ValidateSegment(segment2);

        return Combine(rootDirectory, Path.Join(segment1, segment2));
    }

    /// <summary>Resolves three relative path segments under a trusted root directory.</summary>
    /// <param name="rootDirectory">Trusted absolute root directory.</param>
    /// <param name="segment1">First relative segment.</param>
    /// <param name="segment2">Second relative segment.</param>
    /// <param name="segment3">Third relative segment.</param>
    /// <returns>Absolute normalized path under the root.</returns>
    internal static string Combine(string rootDirectory, string segment1, string segment2, string segment3)
    {
        ArgumentNullException.ThrowIfNull(rootDirectory);
        ArgumentNullException.ThrowIfNull(segment1);
        ArgumentNullException.ThrowIfNull(segment2);
        ArgumentNullException.ThrowIfNull(segment3);

        ValidateSegment(segment1);
        ValidateSegment(segment2);
        ValidateSegment(segment3);

        return Combine(rootDirectory, Path.Join(segment1, segment2, segment3));
    }

    /// <summary>Resolves a relative path under a trusted root directory and rejects paths that escape the root.</summary>
    /// <param name="rootDirectory">Trusted absolute root directory.</param>
    /// <param name="relativePath">Relative path under <paramref name="rootDirectory" />.</param>
    /// <returns>Absolute normalized path under the root.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="rootDirectory" /> or <paramref name="relativePath" /> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when paths are empty, rooted, or escape <paramref name="rootDirectory" />.</exception>
    public static string Combine(string rootDirectory, string relativePath)
    {
        ArgumentNullException.ThrowIfNull(rootDirectory);
        ArgumentNullException.ThrowIfNull(relativePath);

        if (string.IsNullOrWhiteSpace(rootDirectory))
            throw new ArgumentException("Root directory must not be empty.", nameof(rootDirectory));

        if (string.IsNullOrWhiteSpace(relativePath))
            throw new ArgumentException("Relative path must not be empty.", nameof(relativePath));

        if (Path.IsPathRooted(relativePath))
            throw new ArgumentException("Path must be relative.", nameof(relativePath));

        var root = Path.GetFullPath(rootDirectory);
        var fullPath = Path.GetFullPath(Path.Join(root, relativePath));

        return IsPathUnderRoot(fullPath, root) ? fullPath : throw new ArgumentException("Path escapes the configured root directory.", nameof(relativePath));
    }

    /// <summary>Resolves two relative path segments under a trusted root directory.</summary>
    /// <param name="rootDirectory">Trusted absolute root directory.</param>
    /// <param name="segment1">First relative segment.</param>
    /// <param name="segment2">Second relative segment.</param>
    /// <returns>Absolute normalized path under the root.</returns>
    public static string Combine(string rootDirectory, string segment1, string segment2)
    {
        ArgumentNullException.ThrowIfNull(rootDirectory);
        ArgumentNullException.ThrowIfNull(segment1);
        ArgumentNullException.ThrowIfNull(segment2);

        ValidateSegment(segment1);
        ValidateSegment(segment2);

        return Combine(rootDirectory, Path.Join(segment1, segment2));
    }

    /// <summary>Resolves three relative path segments under a trusted root directory.</summary>
    /// <param name="rootDirectory">Trusted absolute root directory.</param>
    /// <param name="segment1">First relative segment.</param>
    /// <param name="segment2">Second relative segment.</param>
    /// <param name="segment3">Third relative segment.</param>
    /// <returns>Absolute normalized path under the root.</returns>
    public static string Combine(string rootDirectory, string segment1, string segment2, string segment3)
    {
        ArgumentNullException.ThrowIfNull(rootDirectory);
        ArgumentNullException.ThrowIfNull(segment1);
        ArgumentNullException.ThrowIfNull(segment2);
        ArgumentNullException.ThrowIfNull(segment3);

        ValidateSegment(segment1);
        ValidateSegment(segment2);
        ValidateSegment(segment3);

        return Combine(rootDirectory, Path.Join(segment1, segment2, segment3));
    }

    private static void ValidateSegment(string segment)
    {
        if (string.IsNullOrWhiteSpace(segment))
            throw new ArgumentException("Path segments must not be empty.", nameof(segment));

        if (Path.IsPathRooted(segment))
            throw new ArgumentException("Path segments must be relative.", nameof(segment));
    }

    private static bool IsPathUnderRoot(string fullPath, string rootFullPath)
    {
        var rootLength = GetNormalizedRootPrefixLength(rootFullPath.AsSpan());
        var root = rootFullPath.AsSpan(0, rootLength);
        var path = fullPath.AsSpan();

        if (path.Length == root.Length)
            return path.Equals(root, PathComparison);

        if (path.Length < root.Length)
            return false;

        if (!path.StartsWith(root, PathComparison))
            return false;

        if (IsFilesystemRoot(root))
            return true;

        return IsDirectorySeparator(path[root.Length]);
    }

    private static int GetNormalizedRootPrefixLength(ReadOnlySpan<char> path)
    {
        var end = path.Length;
        while (end > 0 && IsDirectorySeparator(path[end - 1]))
            end--;

        return end > 0 ? end : path.Length;
    }

    private static bool IsFilesystemRoot(ReadOnlySpan<char> root)
    {
        if (root.Length is 1 && IsDirectorySeparator(root[0]))
            return true;

        return OperatingSystem.IsWindows() && root.Length is 2 && root[1] is ':';
    }

    private static bool IsDirectorySeparator(char value) => value == Path.DirectorySeparatorChar || value == Path.AltDirectorySeparatorChar;
}
