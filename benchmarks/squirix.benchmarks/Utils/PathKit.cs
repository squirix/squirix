using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Squirix.Benchmarks.Utils;

internal static class PathKit
{
    /// <summary>
    /// Combines path segments into a single path, optionally sanitizing each segment first.
    /// </summary>
    /// <param name="paths">Path segments to combine. Null, empty, or whitespace-only segments are ignored.</param>
    /// <returns>The combined path, or an empty string when no usable segments are supplied.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="paths" /> is <see langword="null" />.</exception>
    internal static string Combine(params string[] paths) => Combine(true, paths);

    /// <summary>
    /// Combines path segments into a single path, optionally sanitizing each segment first.
    /// </summary>
    /// <param name="sanitize">
    /// When <see langword="true" />, each non-root segment is passed through <see cref="SanitizePath(string)" />
    /// before combining.
    /// </param>
    /// <param name="paths">Path segments to combine. Null, empty, or whitespace-only segments are ignored.</param>
    /// <returns>The combined path, or an empty string when no usable segments are supplied.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="paths" /> is <see langword="null" />.</exception>
    private static string Combine(bool sanitize = true, params string[] paths)
    {
        ArgumentNullException.ThrowIfNull(paths);

        if (paths.Length == 0)
            return string.Empty;

        var segments = new List<string>(paths.Length);
        foreach (var path in paths)
        {
            if (string.IsNullOrWhiteSpace(path))
                continue;

            if (!sanitize)
            {
                segments.Add(path);
                continue;
            }

            // Preserve rooted prefixes verbatim so callers can safely combine onto absolute paths.
            var root = Path.GetPathRoot(path);
            if (!string.IsNullOrEmpty(root) && string.Equals(path, root, StringComparison.Ordinal))
            {
                segments.Add(path);
                continue;
            }

            if (!string.IsNullOrEmpty(root) && path.StartsWith(root, StringComparison.Ordinal))
            {
                var remainder = path[root.Length..].Split([Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar], StringSplitOptions.RemoveEmptyEntries).Select(SanitizePath)
                                                   .ToArray();
                segments.Add(root);
                segments.AddRange(remainder);
                continue;
            }

            var sanitizedParts = path.Split([Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar], StringSplitOptions.RemoveEmptyEntries).Select(SanitizePath);

            segments.AddRange(sanitizedParts);
        }

        return JoinSegments(segments);
    }

    private static string JoinSegments(List<string> segments)
    {
        if (segments.Count == 0)
            return string.Empty;

        var result = new StringBuilder(segments[0]);
        for (var i = 1; i < segments.Count; i++)
        {
            var segment = segments[i];
            if (Path.IsPathRooted(segment))
                throw new InvalidOperationException($"Path segment must be relative: '{segment}'.");

            while (result.Length > 0 && (result[^1] == Path.DirectorySeparatorChar || result[^1] == Path.AltDirectorySeparatorChar))
            {
                result.Length--;
            }

            _ = result.Append(Path.DirectorySeparatorChar);
            _ = result.Append(segment);
        }

        return result.ToString();
    }

    private static string SanitizePath(string s)
    {
        ArgumentNullException.ThrowIfNull(s);

        var invalid = Path.GetInvalidFileNameChars();
        var sb = new StringBuilder(s.Length);
        foreach (var ch in s)
            _ = sb.Append(Array.IndexOf(invalid, ch) >= 0 ? '_' : ch);
        return sb.ToString();
    }
}
