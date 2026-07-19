using System;
using System.Globalization;
using System.IO;

namespace Squirix.Server.TestKit.IO;

internal static class PathValidationKit
{
    private static readonly char[] InvalidFileNameChars = Path.GetInvalidFileNameChars();
    private static readonly char[] InvalidPathChars = Path.GetInvalidPathChars();

    internal static void ValidateNoInvalidChars(string path)
    {
        if (path.AsSpan().IndexOfAny(InvalidPathChars) >= 0)
            throw new ArgumentException($"Path contains invalid characters: '{path}'.", nameof(path));

        if (path.Contains('*', StringComparison.Ordinal) || path.Contains('?', StringComparison.Ordinal))
            throw new ArgumentException("Path must not contain wildcards (* or ?).", nameof(path));
    }

    internal static void ValidateSegments(string fullPath)
    {
        var root = Path.GetPathRoot(fullPath) ?? string.Empty;
        var rest = fullPath.AsSpan(root.Length);
        while (TryReadNextSegment(ref rest, out var segment))
            ValidateSegment(segment, fullPath);
    }

    private static bool IsWindowsReservedName(ReadOnlySpan<char> segment)
    {
        var name = segment;
        var dot = segment.IndexOf('.');
        if (dot > 0)
            name = segment[..dot];

        if (name.Equals("CON", StringComparison.OrdinalIgnoreCase) || name.Equals("PRN", StringComparison.OrdinalIgnoreCase) ||
            name.Equals("AUX", StringComparison.OrdinalIgnoreCase) || name.Equals("NUL", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (name.Length < 4)
            return false;

        var prefix = name[..3];
        if (!prefix.Equals("COM", StringComparison.OrdinalIgnoreCase) && !prefix.Equals("LPT", StringComparison.OrdinalIgnoreCase))
            return false;

        return int.TryParse(name[3..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var num) && num is >= 0 and <= 9;
    }

    private static void ValidateSegment(ReadOnlySpan<char> segment, string fullPath)
    {
        if (segment.IsEmpty)
            throw new ArgumentException($"Empty segment in path: '{fullPath}'.", nameof(fullPath));

        if (OperatingSystem.IsWindows())
        {
            if (segment.EndsWith(' ') || segment.EndsWith('.'))
                throw new ArgumentException($"Segment ends with space or dot: '{segment}' in '{fullPath}'.", nameof(fullPath));

            if (IsWindowsReservedName(segment))
                throw new ArgumentException($"Segment is a reserved Windows name: '{segment}' in '{fullPath}'.", nameof(fullPath));
        }

        if (segment.IndexOfAny(InvalidFileNameChars) >= 0)
            throw new ArgumentException($"Segment contains invalid characters: '{segment}' in '{fullPath}'.", nameof(fullPath));
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

    private static bool IsDirectorySeparator(char c) => c == Path.DirectorySeparatorChar || c == Path.AltDirectorySeparatorChar;
}
