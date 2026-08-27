using System;
using System.IO;

namespace Squirix.Server.Utils;

/// <summary>
/// Validates operator-supplied file and directory paths before I/O.
/// Rejects <c language="csharp">.</c> / <c language="csharp">..</c> segments and invalid characters, then returns a canonical absolute path.
/// </summary>
internal static class FilePathValidator
{
    /// <summary>Validates and canonicalizes an operator-supplied directory path.</summary>
    /// <param name="path">Absolute or relative directory path.</param>
    /// <returns>Normalized absolute path.</returns>
    /// <exception cref="ArgumentException">Thrown when the path is empty, contains invalid characters, or has <c language="csharp">.</c> / <c language="csharp">..</c> segments.</exception>
    internal static string ResolveValidatedDirectoryPath(string path) => ResolveValidatedPath(path, nameof(path));

    /// <summary>Validates and canonicalizes an operator-supplied file path.</summary>
    /// <param name="path">Absolute or relative file path.</param>
    /// <returns>Normalized absolute path.</returns>
    /// <exception cref="ArgumentException">Thrown when the path is empty, contains invalid characters, or has <c language="csharp">.</c> / <c language="csharp">..</c> segments.</exception>
    internal static string ResolveValidatedFilePath(string path) => ResolveValidatedPath(path, nameof(path));

    private static string ResolveValidatedPath(string path, string paramName)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("Path must be a non-empty string.", paramName);

        PathValidation.ValidateNoInvalidChars(path, paramName);
        ValidateRawSegments(path, paramName);
        return Path.GetFullPath(path);
    }

    private static void ValidateRawSegments(string path, string paramName)
    {
        var root = Path.GetPathRoot(path) ?? string.Empty;
        var rest = path.AsSpan(root.Length);
        while (PathEx.TryReadNextSegment(ref rest, out var segment))
            PathValidation.ValidateSegment(segment, paramName, true);
    }
}
