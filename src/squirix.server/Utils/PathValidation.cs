using System;
using System.Globalization;
using System.IO;

namespace Squirix.Server.Utils;

/// <summary>Shared path character and Windows reserved-name checks for file/directory validators.</summary>
internal static class PathValidation
{
    private static readonly char[] InvalidFileNameChars = Path.GetInvalidFileNameChars();
    private static readonly char[] InvalidPathChars = Path.GetInvalidPathChars();

    /// <summary>Returns whether <paramref name="segment" /> is <c>.</c> or <c>..</c>.</summary>
    /// <param name="segment">Path segment to test.</param>
    /// <returns><see langword="true" /> when the segment is a current- or parent-directory token.</returns>
    internal static bool IsDotOrDotDot(ReadOnlySpan<char> segment) => segment is ['.'] or ['.', '.'];

    /// <summary>Rejects invalid path characters and wildcards.</summary>
    /// <param name="path">Path to inspect.</param>
    /// <param name="paramName">Argument name for exceptions.</param>
    /// <exception cref="ArgumentException">Thrown when the path contains invalid characters or wildcards.</exception>
    internal static void ValidateNoInvalidChars(string path, string paramName)
    {
        if (path.AsSpan().IndexOfAny(InvalidPathChars) >= 0)
            throw new ArgumentException("Path contains invalid characters.", paramName);

        if (path.Contains('*', StringComparison.Ordinal) || path.Contains('?', StringComparison.Ordinal))
            throw new ArgumentException("Path must not contain wildcards (* or ?).", paramName);
    }

    /// <summary>Validates a single path segment for emptiness, optional <c>.</c>/<c>..</c>, Windows rules, and file-name characters.</summary>
    /// <param name="segment">Path segment.</param>
    /// <param name="paramName">Argument name for exceptions.</param>
    /// <param name="rejectDotOrDotDot">When <see langword="true" />, rejects <c>.</c> and <c>..</c> segments.</param>
    /// <param name="applyWindowsRules">
    /// When set, forces Windows reserved-name and trailing space/dot checks on or off;
    /// when <see langword="null" />, uses <see cref="OperatingSystem.IsWindows()" />.
    /// </param>
    /// <exception cref="ArgumentException">Thrown when the segment is empty, is <c>.</c>/<c>..</c> when rejected, violates Windows naming rules, or contains invalid file-name characters.</exception>
    internal static void ValidateSegment(ReadOnlySpan<char> segment, string paramName, bool rejectDotOrDotDot, bool? applyWindowsRules = null)
    {
        if (segment.IsEmpty)
            throw new ArgumentException("Empty segment in path.", paramName);

        if (rejectDotOrDotDot && IsDotOrDotDot(segment))
            throw new ArgumentException("Path must not contain '.' or '..' segments.", paramName);

        if (applyWindowsRules ?? OperatingSystem.IsWindows())
            ValidateWindowsSegmentRules(segment, paramName);

        if (segment.IndexOfAny(InvalidFileNameChars) >= 0)
            throw new ArgumentException("Segment contains invalid characters.", paramName);
    }

    private static void ValidateWindowsSegmentRules(ReadOnlySpan<char> segment, string paramName)
    {
        if (segment.EndsWith(' ') || segment.EndsWith('.'))
            throw new ArgumentException("Segment ends with space or dot.", paramName);

        if (IsWindowsReservedName(segment))
            throw new ArgumentException("Segment is a reserved Windows name.", paramName);
    }

    private static bool IsWindowsReservedName(ReadOnlySpan<char> segment)
    {
        var name = segment;
        var dot = segment.IndexOf('.');
        if (dot > 0)
            name = segment[..dot];

        if (name.Equals("CON", StringComparison.OrdinalIgnoreCase) || name.Equals("PRN", StringComparison.OrdinalIgnoreCase) ||
            name.Equals("AUX", StringComparison.OrdinalIgnoreCase) || name.Equals("NUL", StringComparison.OrdinalIgnoreCase))
            return true;

        if (name.Length < 4)
            return false;

        var prefix = name[..3];
        if (!prefix.Equals("COM", StringComparison.OrdinalIgnoreCase) && !prefix.Equals("LPT", StringComparison.OrdinalIgnoreCase))
            return false;

        return int.TryParse(name[3..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var num) && num is >= 0 and <= 9;
    }
}
