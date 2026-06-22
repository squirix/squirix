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

    /// <summary>Resolves a relative path under a trusted root directory and rejects paths that escape the root.</summary>
    /// <param name="rootDirectory">Trusted absolute root directory.</param>
    /// <param name="relativePath">Relative path under <paramref name="rootDirectory" />.</param>
    /// <returns>Absolute normalized path under the root.</returns>
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

        var root = EnsureTrailingDirectorySeparator(Path.GetFullPath(rootDirectory));
        var fullPath = Path.GetFullPath(root + relativePath);

        return fullPath.StartsWith(root, PathComparison) ? fullPath : throw new ArgumentException("Path escapes the configured root directory.", nameof(relativePath));
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

        return Combine(rootDirectory, string.Concat(segment1, Path.DirectorySeparatorChar, segment2));
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

        return Combine(rootDirectory, string.Concat(segment1, Path.DirectorySeparatorChar, segment2, Path.DirectorySeparatorChar, segment3));
    }

    private static void ValidateSegment(string segment)
    {
        if (string.IsNullOrWhiteSpace(segment))
            throw new ArgumentException("Path segments must not be empty.", nameof(segment));

        if (Path.IsPathRooted(segment))
            throw new ArgumentException("Path segments must be relative.", nameof(segment));
    }

    private static string EnsureTrailingDirectorySeparator(string path) => Path.EndsInDirectorySeparator(path) ? path : path + Path.DirectorySeparatorChar;
}
