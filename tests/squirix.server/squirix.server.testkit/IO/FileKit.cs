using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

namespace Squirix.Server.TestKit.IO;

/// <summary>
/// Lightweight file utilities intended for resilient test and tooling scenarios.
/// </summary>
public static class FileKit
{
    /// <summary>
    /// Determines whether a file exists at the provided path after validating the file path shape.
    /// </summary>
    /// <param name="path">Absolute or relative file path to validate and inspect.</param>
    /// <returns><see langword="true" /> when a regular file exists at the validated path; otherwise <see langword="false" />.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="path" /> is empty/whitespace, contains invalid characters or wildcards,
    /// has empty segments, uses Windows-reserved names, ends with dot/space on Windows, or does not include a file name.
    /// </exception>
    public static bool Exists(string? path)
    {
        var full = ValidateAndGetFullPath(path);
        return File.Exists(full);
    }

    /// <summary>
    /// Attempts to delete a file at the given <paramref name="path" /> and suppresses all exceptions.
    /// </summary>
    /// <param name="path">
    /// Absolute or relative path to the file to delete. If <see langword="null" /> or empty,
    /// the method returns without performing any action.
    /// </param>
    /// <remarks>
    /// This is a best-effort cleanup helper that intentionally ignores any errors
    /// (e.g., <see cref="IOException" />, <see cref="UnauthorizedAccessException" />).
    /// Prefer this in test teardown code where failures during cleanup should not fail the test.
    /// For strict deletion semantics, use <see cref="File.Delete(string)" /> directly.
    /// </remarks>
    public static void TryDelete(string? path)
    {
        try
        {
            if (!string.IsNullOrEmpty(path))
                File.Delete(path);
        }
        catch (IOException)
        {
            /* ignore */
        }
        catch (UnauthorizedAccessException)
        {
            /* ignore */
        }
    }

    /// <summary>
    /// Writes the specified text to a file after validating the file path and ensuring the parent directory exists.
    /// </summary>
    /// <param name="path">Absolute or relative file path to create or overwrite.</param>
    /// <param name="contents">Text content to write.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="contents" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="path" /> is invalid or does not include a file name.</exception>
    public static void WriteAllText(string path, string contents)
    {
        ArgumentNullException.ThrowIfNull(contents);

        var full = ValidateAndGetFullPath(path);
        var directory = Path.GetDirectoryName(full);
        if (!string.IsNullOrWhiteSpace(directory))
            DirectoryKit.CreateDirectory(directory);

        File.WriteAllText(full, contents);
    }

    private static bool IsWindowsReservedName(string seg)
    {
        var name = seg;
        var dot = seg.IndexOf('.', StringComparison.Ordinal);
        if (dot > 0)
            name = seg[..dot];

        string[] fixedNames = ["CON", "PRN", "AUX", "NUL"];
        if (fixedNames.Any(n => string.Equals(name, n, StringComparison.OrdinalIgnoreCase)))
            return true;

        if (name.Length < 4)
            return false;

        var prefix = name[..3].ToUpperInvariant();
        return prefix is "COM" or "LPT" && int.TryParse(name.AsSpan(3), out var num) && num is >= 0 and <= 9;
    }

    private static string ValidateAndGetFullPath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("Path must be a non-empty string.", nameof(path));

        ValidateNoInvalidChars(path);

        var full = Path.GetFullPath(path);
        ValidateSegments(full);

        var fileName = Path.GetFileName(full);
        return string.IsNullOrWhiteSpace(fileName) ? throw new ArgumentException("Path must include a file name.", nameof(path)) : full;
    }

    private static void ValidateNoInvalidChars(string path)
    {
        if (path.IndexOfAny(Path.GetInvalidPathChars()) >= 0)
            throw new ArgumentException($"Path contains invalid characters: '{path}'.", nameof(path));

        if (path.Contains('*', StringComparison.Ordinal) || path.Contains('?', StringComparison.Ordinal))
            throw new ArgumentException("Path must not contain wildcards (* or ?).", nameof(path));
    }

    private static void ValidateSegments(string fullPath)
    {
        var root = Path.GetPathRoot(fullPath) ?? string.Empty;
        var rest = fullPath[root.Length..];
        var segments = rest.Split([Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar], StringSplitOptions.RemoveEmptyEntries);

        foreach (var rawSeg in segments)
        {
            var seg = rawSeg.Trim();
            if (seg.Length == 0)
                throw new ArgumentException($"Empty segment in path: '{fullPath}'.");

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                if (seg.EndsWith(' ') || seg.EndsWith('.'))
                    throw new ArgumentException($"Segment ends with space or dot: '{seg}' in '{fullPath}'.");

                if (IsWindowsReservedName(seg))
                    throw new ArgumentException($"Segment is a reserved Windows name: '{seg}' in '{fullPath}'.");
            }

            if (seg.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
                throw new ArgumentException($"Segment contains invalid characters: '{seg}' in '{fullPath}'.");
        }
    }
}
