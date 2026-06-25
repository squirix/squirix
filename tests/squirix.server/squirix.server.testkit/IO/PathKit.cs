using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;

namespace Squirix.Server.TestKit.IO;

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
    private const int MaxSegmentBufferLength = 16;
    private static readonly string ProcessSessionSegment = BuildProcessSessionSegment();

    /// <summary>Combines path segments into a single path, sanitizing each segment first.</summary>
    /// <param name="path1">First path segment. Null, empty, or whitespace-only segments are ignored.</param>
    /// <param name="path2">Second path segment. Null, empty, or whitespace-only segments are ignored.</param>
    /// <returns>The combined path, or an empty string when no usable segments are supplied.</returns>
    public static string Combine(string path1, string path2) => CombineCore(true, path1, path2);

    /// <inheritdoc cref="Combine(string,string)" />
    public static string Combine(string path1, string path2, string path3) => CombineCore(true, path1, path2, path3);

    /// <inheritdoc cref="Combine(string,string)" />
    public static string Combine(string path1, string path2, string path3, string path4, string path5) => CombineCore(true, path1, path2, path3, path4, path5);

    /// <inheritdoc cref="Combine(string,string)" />
    public static string Combine(string path1, string path2, string path3, string path4, string path5, string path6) => CombineCore(true, path1, path2, path3, path4, path5, path6);

    /// <summary>Combines path segments into a single path, optionally sanitizing each segment first.</summary>
    /// <param name="sanitize">
    /// When <see langword="true" />, each non-root segment is passed through <see cref="SanitizePath(string)" />
    /// before combining.
    /// </param>
    /// <param name="path1">First path segment. Null, empty, or whitespace-only segments are ignored.</param>
    /// <param name="path2">Second path segment. Null, empty, or whitespace-only segments are ignored.</param>
    /// <returns>The combined path, or an empty string when no usable segments are supplied.</returns>
    public static string Combine(bool sanitize, string path1, string path2) => CombineCore(sanitize, path1, path2);

    /// <inheritdoc cref="Combine(bool,string,string)" />
    public static string Combine(bool sanitize, string path1, string path2, string path3, string path4) => CombineCore(sanitize, path1, path2, path3, path4);

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

    private static void AddSegment(string segment, string[] buffer, ref int count, ref List<string>? heapBuffer)
    {
        if (heapBuffer is not null)
        {
            heapBuffer.Add(segment);
            return;
        }

        if (count >= buffer.Length)
        {
            heapBuffer = new List<string>(buffer.Length + 4);
            for (var i = 0; i < count; i++)
                heapBuffer.Add(buffer[i]);
            heapBuffer.Add(segment);
            return;
        }

        buffer[count++] = segment;
    }

    private static void AppendIfNotWhiteSpace(string? path, bool sanitize, string[] buffer, ref int count, ref List<string>? heapBuffer)
    {
        if (string.IsNullOrWhiteSpace(path))
            return;

        AppendPathSegments(path, sanitize, buffer, ref count, ref heapBuffer);
    }

    private static void AppendPathSegments(string path, bool sanitize, string[] buffer, ref int count, ref List<string>? heapBuffer)
    {
        if (!sanitize)
        {
            AddSegment(path, buffer, ref count, ref heapBuffer);
            return;
        }

        // Preserve rooted prefixes verbatim so callers can safely combine onto absolute paths.
        var root = Path.GetPathRoot(path);
        if (!string.IsNullOrEmpty(root) && string.Equals(path, root, StringComparison.Ordinal))
        {
            AddSegment(path, buffer, ref count, ref heapBuffer);
            return;
        }

        if (!string.IsNullOrEmpty(root) && path.StartsWith(root, StringComparison.Ordinal))
        {
            var parts = path[root.Length..].Split([Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar], StringSplitOptions.RemoveEmptyEntries);
            AddSegment(root, buffer, ref count, ref heapBuffer);
            for (var i = 0; i < parts.Length; i++)
                AddSegment(SanitizePath(parts[i]), buffer, ref count, ref heapBuffer);
            return;
        }

        var sanitizedParts = path.Split([Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar], StringSplitOptions.RemoveEmptyEntries);
        for (var i = 0; i < sanitizedParts.Length; i++)
            AddSegment(SanitizePath(sanitizedParts[i]), buffer, ref count, ref heapBuffer);
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

        return $"pid{System.Environment.ProcessId.ToString(CultureInfo.InvariantCulture)}-start{startTicks.ToString(CultureInfo.InvariantCulture)}";
    }

    private static string CombineCore(bool sanitize, string path1, string path2) => CombineCore(sanitize, path1, path2, null, null, null, null, 2);

    private static string CombineCore(bool sanitize, string path1, string path2, string path3) => CombineCore(sanitize, path1, path2, path3, null, null, null, 3);

    private static string CombineCore(bool sanitize, string path1, string path2, string path3, string path4) => CombineCore(sanitize, path1, path2, path3, path4, null, null, 4);

    private static string CombineCore(bool sanitize, string path1, string path2, string path3, string path4, string path5) =>
        CombineCore(sanitize, path1, path2, path3, path4, path5, null, 5);

    private static string CombineCore(bool sanitize, string path1, string path2, string path3, string path4, string path5, string path6) =>
        CombineCore(sanitize, path1, path2, path3, path4, path5, path6, 6);

    private static string CombineCore(bool sanitize, string? path1, string? path2, string? path3, string? path4, string? path5, string? path6, int pathCount)
    {
        var buffer = ArrayPool<string>.Shared.Rent(MaxSegmentBufferLength);
        var count = 0;
        List<string>? heapBuffer = null;

        try
        {
            if (pathCount >= 1)
                AppendIfNotWhiteSpace(path1, sanitize, buffer, ref count, ref heapBuffer);
            if (pathCount >= 2)
                AppendIfNotWhiteSpace(path2, sanitize, buffer, ref count, ref heapBuffer);
            if (pathCount >= 3)
                AppendIfNotWhiteSpace(path3, sanitize, buffer, ref count, ref heapBuffer);
            if (pathCount >= 4)
                AppendIfNotWhiteSpace(path4, sanitize, buffer, ref count, ref heapBuffer);
            if (pathCount >= 5)
                AppendIfNotWhiteSpace(path5, sanitize, buffer, ref count, ref heapBuffer);
            if (pathCount >= 6)
                AppendIfNotWhiteSpace(path6, sanitize, buffer, ref count, ref heapBuffer);

            return FinishCombine(buffer, count, heapBuffer);
        }
        finally
        {
            ArrayPool<string>.Shared.Return(buffer, true);
        }
    }

    private static string FinishCombine(string[] buffer, int count, List<string>? heapBuffer)
    {
        if (count is 0)
            return string.Empty;

        if (heapBuffer is not null)
            return JoinSegments(CollectionsMarshal.AsSpan(heapBuffer));

        return JoinSegments(buffer.AsSpan(0, count));
    }

    private static string JoinSegments(ReadOnlySpan<string> segments)
    {
        if (segments.Length is 0)
            return string.Empty;

        if (segments.Length is 1)
            return segments[0];

        var result = segments[0];
        for (var i = 1; i < segments.Length; i++)
        {
            var segment = segments[i];
            if (Path.IsPathRooted(segment))
                throw new InvalidOperationException($"Path segment must be relative: '{segment}'.");

            var prefixLen = TrimTrailingSeparatorsLength(result.AsSpan());
            var nextLength = prefixLen + 1 + segment.Length;
            result = string.Create(
                nextLength,
                (Prefix: result, PrefixLen: prefixLen, Segment: segment),
                static (dest, state) =>
                {
                    state.Prefix.AsSpan(0, state.PrefixLen).CopyTo(dest);
                    dest[state.PrefixLen] = Path.DirectorySeparatorChar;
                    state.Segment.CopyTo(dest[(state.PrefixLen + 1)..]);
                });
        }

        return result;
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

        var invalid = Path.GetInvalidFileNameChars();
        foreach (var ch in s)
        {
            if (Array.IndexOf(invalid, ch) < 0)
                continue;
            var sb = new StringBuilder(s.Length);
            foreach (var current in s)
                _ = sb.Append(Array.IndexOf(invalid, current) >= 0 ? '_' : current);
            return sb.ToString();
        }

        return s;
    }

    private static int TrimTrailingSeparatorsLength(ReadOnlySpan<char> span)
    {
        var length = span.Length;
        while (length > 0)
        {
            var last = span[length - 1];
            if (last != Path.DirectorySeparatorChar && last != Path.AltDirectorySeparatorChar)
                break;
            length--;
        }

        return length;
    }
}
