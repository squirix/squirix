using System;
using System.Globalization;
using System.IO;

namespace Squirix.Server.TestKit.IO;

/// <summary>Validation helpers for test-controlled path segments and paths.</summary>
public static class PathValidationKit
{
    private static readonly char[] InvalidFileNameChars = Path.GetInvalidFileNameChars();
    private static readonly char[] InvalidPathChars = Path.GetInvalidPathChars();

    /// <summary>Validates a single path segment used to build a test directory or file name.</summary>
    /// <param name="value">Segment value (must not be empty, contain parent-directory or drive separators).</param>
    /// <param name="paramName">Caller parameter name for exception reporting.</param>
    /// <exception cref="ArgumentException">Thrown when the segment is empty or contains traversal separators.</exception>
    public static void ValidateSegmentName(string value, string paramName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);

        if (value.Contains("..", StringComparison.Ordinal))
            throw new ArgumentException($"'{value}' must not contain parent-directory segments ('..').", paramName);

        if (value.Contains(':', StringComparison.Ordinal))
            throw new ArgumentException($"'{value}' must not contain a drive separator (':').", paramName);
    }

    /// <summary>Validates a caller-supplied path before it is used for cleanup or creation.</summary>
    /// <param name="path">Path value (must not contain parent-directory segments).</param>
    /// <param name="paramName">Caller parameter name for exception reporting.</param>
    /// <exception cref="ArgumentException">Thrown when the path contains a parent-directory segment.</exception>
    public static void ValidateNoParentSegments(string path, string paramName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        if (path.Contains("..", StringComparison.Ordinal))
            throw new ArgumentException($"'{path}' must not contain parent-directory segments ('..').", paramName);
    }

    internal static void ValidateNoInvalidChars(string path)
    {
        if (path.AsSpan().IndexOfAny(InvalidPathChars) >= 0)
            throw new ArgumentException("Path contains invalid characters.", nameof(path));

        if (path.Contains('*', StringComparison.Ordinal) || path.Contains('?', StringComparison.Ordinal))
            throw new ArgumentException("Path must not contain wildcards (* or ?).", nameof(path));
    }

    internal static void ValidateSegments(string fullPath)
    {
        var root = Path.GetPathRoot(fullPath) ?? string.Empty;
        var rest = fullPath.AsSpan(root.Length);
        while (TryReadNextSegment(ref rest, out var segment))
            ValidateSegment(segment);
    }

    private static bool IsDirectorySeparator(char c) => c == Path.DirectorySeparatorChar || c == Path.AltDirectorySeparatorChar;

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

    private static bool TryReadNextSegment(ref ReadOnlySpan<char> path, out ReadOnlySpan<char> segment)
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
            segment = path.Trim();
            path = default;
            return !segment.IsEmpty;
        }

        segment = path[..end].Trim();
        path = path[(end + 1)..];
        return !segment.IsEmpty;
    }

    private static void ValidateSegment(ReadOnlySpan<char> segment)
    {
        if (segment.IsEmpty)
            throw new ArgumentException("Empty segment in path.", nameof(segment));

        if (OperatingSystem.IsWindows())
        {
            if (segment.EndsWith(' ') || segment.EndsWith('.'))
                throw new ArgumentException("Segment ends with space or dot.", nameof(segment));

            if (IsWindowsReservedName(segment))
                throw new ArgumentException("Segment is a reserved Windows name.", nameof(segment));
        }

        if (segment.IndexOfAny(InvalidFileNameChars) >= 0)
            throw new ArgumentException("Segment contains invalid characters.", nameof(segment));
    }
}
