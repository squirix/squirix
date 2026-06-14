using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace Squirix.TestKit.IO;

/// <summary>
/// Provides helper methods for working with file system paths in a safe,
/// cross-platform way.
/// </summary>
/// <remarks>
/// The utilities in <see cref="PathKit" /> are intended to sanitize and manipulate
/// path segments (such as file names) rather than perform actual I/O.
/// They do not create or validate files or directories on disk.
/// </remarks>
public static class PathKit
{
    private static readonly string ProcessSessionSegment = BuildProcessSessionSegment();
    private static readonly char[] CrossPlatformInvalidFileNameChars = [.. Path.GetInvalidFileNameChars(), '<', '>', ':', '"', '/', '\\', '|', '?', '*'];

    /// <summary>
    /// Combines path segments into a single path, optionally sanitizing each segment first.
    /// </summary>
    /// <param name="paths">Path segments to combine. Null, empty, or whitespace-only segments are ignored.</param>
    /// <returns>The combined path, or an empty string when no usable segments are supplied.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="paths" /> is <see langword="null" />.</exception>
    public static string Combine(params string[] paths) => Combine(true, paths);

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
    public static string Combine(bool sanitize = true, params string[] paths)
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

    /// <summary>
    /// Returns the directory component of the specified path, or an empty string when none exists.
    /// </summary>
    /// <param name="path">Path to inspect.</param>
    /// <returns>The directory component of the path, or an empty string.</returns>
    public static string GetDirectoryName(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        return Path.GetDirectoryName(path) ?? string.Empty;
    }

    /// <summary>
    /// Returns the file name and extension of the specified path.
    /// </summary>
    /// <param name="path">Path to inspect.</param>
    /// <returns>The file name and extension.</returns>
    public static string GetFileName(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        return Path.GetFileName(path);
    }

    /// <summary>
    /// Returns the file name without extension of the specified path.
    /// </summary>
    /// <param name="path">Path to inspect.</param>
    /// <returns>The file name without extension.</returns>
    public static string GetFileNameWithoutExtension(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        return Path.GetFileNameWithoutExtension(path);
    }

    /// <summary>
    /// Builds a process-scoped temporary root path under <see cref="Path.GetTempPath" />.
    /// </summary>
    /// <param name="subdirectory">
    /// Optional root subdirectory under the system temp path. When provided, it is appended before
    /// the target-framework and process-id segments.
    /// </param>
    /// <returns>
    /// A path of the form <c>&lt;temp&gt;\&lt;subdirectory&gt;\&lt;tfm&gt;\pid&lt;processId&gt;-start&lt;utcTicks&gt;</c>.
    /// </returns>
    public static string GetProcTempPath(string subdirectory = "")
    {
        var root = Combine(Path.GetTempPath(), subdirectory);
        var tfmSegment = SanitizePath(AppContext.TargetFrameworkName ?? "unknown");
        return Combine(root, tfmSegment, ProcessSessionSegment);
    }

    private static string BuildProcessSessionSegment()
    {
        long startTicks;
        try
        {
            startTicks = Process.GetCurrentProcess().StartTime.ToUniversalTime().Ticks;
        }
        catch (InvalidOperationException)
        {
            startTicks = DateTime.UtcNow.Ticks;
        }
        catch (PlatformNotSupportedException)
        {
            startTicks = DateTime.UtcNow.Ticks;
        }
        catch (NotSupportedException)
        {
            startTicks = DateTime.UtcNow.Ticks;
        }

        return $"pid{Environment.ProcessId}-start{startTicks}";
    }

    private static string JoinSegments(List<string> segments)
    {
        if (segments.Count == 0)
            return string.Empty;

        var sb = new StringBuilder(segments[0]);
        for (var i = 1; i < segments.Count; i++)
        {
            var segment = segments[i];
            if (Path.IsPathRooted(segment))
                throw new InvalidOperationException($"Path segment must be relative: '{segment}'.");

            var current = sb.ToString().TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            _ = sb.Clear();
            _ = sb.Append(current);
            _ = sb.Append(Path.DirectorySeparatorChar);
            _ = sb.Append(segment);
        }

        return sb.ToString();
    }

    /// <summary>
    /// Replaces all characters in a file name that are invalid for the current platform
    /// (as returned by <see cref="Path.GetInvalidFileNameChars" />)
    /// with an underscore (<c>_</c>).
    /// </summary>
    /// <param name="s">The candidate file name to sanitize.</param>
    /// <returns>
    /// A new string in which every invalid file-name character has been replaced by <c>_</c>.
    /// If <paramref name="s" /> contains no invalid characters, the original string is returned unchanged.
    /// </returns>
    /// <remarks>
    /// This method does not validate or alter directory separators or full paths; it is intended
    /// for file <em>names</em> only. It also preserves character casing and length.
    /// </remarks>
    /// <example>
    ///     <code>
    /// var raw = "report:Q3*final?.txt";
    /// var safe = PathKit.SanitizePath(raw); // "report_Q3_final_.txt"
    /// </code>
    /// </example>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="s" /> is <see langword="null" />.</exception>
    private static string SanitizePath(string s)
    {
        ArgumentNullException.ThrowIfNull(s);

        var sb = new StringBuilder(s.Length);
        foreach (var ch in s)
            _ = sb.Append(Array.IndexOf(CrossPlatformInvalidFileNameChars, ch) >= 0 ? '_' : ch);
        return sb.ToString();
    }
}
