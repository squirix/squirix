using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;

namespace Squirix.TestKit;

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
    private static readonly char[] CrossPlatformInvalidFileNameChars = BuildCrossPlatformInvalidFileNameChars();

    /// <summary>Combines path segments into a single path, sanitizing each segment first.</summary>
    /// <param name="path1">First path segment. Null, empty, or whitespace-only segments are ignored.</param>
    /// <param name="path2">Second path segment. Null, empty, or whitespace-only segments are ignored.</param>
    /// <returns>The combined path, or an empty string when no usable segments are supplied.</returns>
    public static string Combine(string path1, string path2) => CombineCore(true, path1, path2);

    /// <inheritdoc cref="Combine(string,string)" />
    public static string Combine(string path1, string path2, string path3) => CombineCore(true, path1, path2, path3);

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
        if (AppContext.TargetFrameworkName is null)
            return Combine(root, "unknown", ProcessSessionSegment);
        var segment = SanitizePath(AppContext.TargetFrameworkName);
        return Combine(root, segment, ProcessSessionSegment);
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

        ReadOnlySpan<char> remainder;
        if (!string.IsNullOrEmpty(root) && path.StartsWith(root, StringComparison.Ordinal))
        {
            AddSegment(root, buffer, ref count, ref heapBuffer);
            remainder = path.AsSpan(root.Length);
        }
        else
        {
            remainder = path.AsSpan();
        }

        AppendSanitizedSegments(remainder, buffer, ref count, ref heapBuffer);
    }

    private static void AppendSanitizedSegments(ReadOnlySpan<char> remainder, string[] buffer, ref int count, ref List<string>? heapBuffer)
    {
        while (!remainder.IsEmpty)
        {
            var sepIndex = IndexOfDirectorySeparator(remainder);
            ReadOnlySpan<char> segment;
            if (sepIndex < 0)
            {
                segment = remainder;
                remainder = default;
            }
            else
            {
                segment = remainder[..sepIndex];
                remainder = remainder[(sepIndex + 1)..];
            }

            if (segment.IsEmpty)
                continue;

            AddSegment(SanitizePath(segment.ToString()), buffer, ref count, ref heapBuffer);
        }
    }

    private static char[] BuildCrossPlatformInvalidFileNameChars()
    {
        var invalid = new HashSet<char>(Path.GetInvalidFileNameChars())
        {
            '<',
            '>',
            ':',
            '"',
            '/',
            '\\',
            '|',
            '?',
            '*',
        };

        var chars = new char[invalid.Count];
        invalid.CopyTo(chars);
        return chars;
    }

    private static string BuildProcessSessionSegment()
    {
        var startTicks = GetProcessStartTicks();
        return $"pid{InvariantIndexStrings.Format(Environment.ProcessId)}-start{InvariantIndexStrings.Format(startTicks)}";
    }

    private static string CombineCore(bool sanitize, string path1, string path2) => CombineCore(sanitize, path1, path2, null, 2);

    private static string CombineCore(bool sanitize, string path1, string path2, string path3) => CombineCore(sanitize, path1, path2, path3, 3);

    private static string CombineCore(bool sanitize, string? path1, string? path2, string? path3, int pathCount)
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

    private static long GetProcessStartTicks()
    {
        try
        {
            return Process.GetCurrentProcess().StartTime.ToUniversalTime().Ticks;
        }
        catch (Exception ex) when (ex is InvalidOperationException or PlatformNotSupportedException or NotSupportedException)
        {
            return DateTime.UtcNow.Ticks;
        }
    }

    private static int IndexOfDirectorySeparator(ReadOnlySpan<char> value)
    {
        var primary = value.IndexOf(Path.DirectorySeparatorChar);
        var alternate = value.IndexOf(Path.AltDirectorySeparatorChar);
        if (primary < 0)
            return alternate;
        if (alternate < 0)
            return primary;
        return primary < alternate ? primary : alternate;
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
                throw new InvalidOperationException("Path segment must be relative.");

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
    private static string SanitizePath(ReadOnlySpan<char> s)
    {
        var sb = new StringBuilder(s.Length);
        for (var i = 0; i < s.Length; i++)
        {
            if (Array.IndexOf(CrossPlatformInvalidFileNameChars, s[i]) < 0)
                continue;

            _ = sb.Clear();
            for (var j = 0; j < s.Length; j++)
            {
                var current = s[j];
                _ = sb.Append(Array.IndexOf(CrossPlatformInvalidFileNameChars, current) >= 0 ? '_' : current);
            }

            return sb.ToString();
        }

        return s.ToString();
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
