using System;
using System.IO;

namespace Squirix.Server.TestKit.IO;

/// <summary>Lightweight file utilities intended for resilient test and tooling scenarios.</summary>
public static class FileKit
{
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
            Directory.CreateDirectory(directory);

        File.WriteAllText(full, contents);
    }

    private static string ValidateAndGetFullPath(string? path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        PathValidationKit.ValidateNoInvalidChars(path);

        var full = Path.GetFullPath(path);
        PathValidationKit.ValidateSegments(full);

        var fileName = Path.GetFileName(full);
        return string.IsNullOrWhiteSpace(fileName) ? throw new ArgumentException("Path must include a file name.", nameof(path)) : full;
    }
}
