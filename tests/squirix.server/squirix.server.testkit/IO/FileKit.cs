using System;
using System.IO;

namespace Squirix.Server.TestKit.IO;

/// <summary>Lightweight file utilities intended for resilient test and tooling scenarios.</summary>
public static class FileKit
{
    /// <summary>Determines whether a file exists at the provided path after validating the file path shape.</summary>
    /// <param name="path">Absolute or relative file path to validate and inspect.</param>
    /// <returns><see langword="true" /> when a regular file exists at the validated path; otherwise <see langword="false" />.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="path" /> is empty/whitespace, contains invalid characters or wildcards,
    /// has empty segments, uses Windows-reserved names, ends with dot/space on Windows, or does not include a file name.
    /// </exception>
    public static bool Exists(string? path) => File.Exists(ValidateAndGetFullPath(path));

    /// <summary>Writes the specified text to a file after validating the file path and ensuring the parent directory exists.</summary>
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
        foreach (var reserved in fixedNames)
        {
            if (string.Equals(name, reserved, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        if (name.Length < 4)
            return false;

        var prefix = name[..3].ToUpperInvariant();
        return (string.Equals(prefix, "COM", StringComparison.Ordinal) || string.Equals(prefix, "LPT", StringComparison.Ordinal)) &&
               int.TryParse(name.AsSpan(3), CultureInfo.InvariantCulture, out var num) && num is >= 0 and <= 9;
    }

    private static string ValidateAndGetFullPath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("Path must be a non-empty string.", nameof(path));

        PathValidationKit.ValidateNoInvalidChars(path);

        var full = Path.GetFullPath(path);
        PathValidationKit.ValidateSegments(full);

        var fileName = Path.GetFileName(full);
        return string.IsNullOrWhiteSpace(fileName) ? throw new ArgumentException("Path must include a file name.", nameof(path)) : full;
    }
}
