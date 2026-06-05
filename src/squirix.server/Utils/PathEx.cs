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

    /// <summary>
    /// Resolves a relative path under a trusted root directory and rejects paths that escape the root.
    /// </summary>
    /// <param name="rootDirectory">Trusted root directory.</param>
    /// <param name="relativePath">Relative path to resolve under <paramref name="rootDirectory" />.</param>
    /// <returns>The canonical absolute path inside <paramref name="rootDirectory" />.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="rootDirectory" /> or <paramref name="relativePath" /> is empty,
    /// when <paramref name="relativePath" /> is rooted,
    /// or when the resolved path escapes <paramref name="rootDirectory" />.
    /// </exception>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="rootDirectory" /> or <paramref name="relativePath" /> is <see langword="null" />.
    /// </exception>
    public static string Combine(string rootDirectory, string relativePath)
    {
        ArgumentNullException.ThrowIfNull(rootDirectory);
        ArgumentNullException.ThrowIfNull(relativePath);

        if (string.IsNullOrWhiteSpace(rootDirectory))
        {
            throw new ArgumentException("Root directory must not be empty.", nameof(rootDirectory));
        }

        if (string.IsNullOrWhiteSpace(relativePath))
        {
            throw new ArgumentException("Relative path must not be empty.", nameof(relativePath));
        }

        if (Path.IsPathRooted(relativePath))
        {
            throw new ArgumentException("Path must be relative.", nameof(relativePath));
        }

        var root = EnsureTrailingDirectorySeparator(Path.GetFullPath(rootDirectory));
        var fullPath = Path.GetFullPath(Path.Combine(root, relativePath));

        return fullPath.StartsWith(root, PathComparison) ? fullPath : throw new ArgumentException("Path escapes the configured root directory.", nameof(relativePath));
    }

    /// <summary>
    /// Resolves relative path segments under a trusted root directory and rejects paths that escape the root.
    /// </summary>
    /// <param name="rootDirectory">Trusted root directory.</param>
    /// <param name="segments">Relative path segments to resolve.</param>
    /// <returns>The canonical absolute path inside <paramref name="rootDirectory" />.</returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="rootDirectory" /> or <paramref name="segments" /> is <see langword="null" />.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when input is empty, rooted, or escapes <paramref name="rootDirectory" />.
    /// </exception>
    public static string Combine(string rootDirectory, params string[] segments)
    {
        ArgumentNullException.ThrowIfNull(rootDirectory);
        ArgumentNullException.ThrowIfNull(segments);

        if (segments.Length == 0)
        {
            throw new ArgumentException("At least one path segment must be supplied.", nameof(segments));
        }

        foreach (var segment in segments)
        {
            if (string.IsNullOrWhiteSpace(segment))
            {
                throw new ArgumentException("Path segments must not be empty.", nameof(segments));
            }

            if (Path.IsPathRooted(segment))
            {
                throw new ArgumentException("Path segments must be relative.", nameof(segments));
            }
        }

        return Combine(rootDirectory, Path.Combine(segments));
    }

    private static string EnsureTrailingDirectorySeparator(string path) => Path.EndsInDirectorySeparator(path) ? path : path + Path.DirectorySeparatorChar;
}
