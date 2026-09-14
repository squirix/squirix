using System.IO;

namespace Squirix.Server.Utils;

/// <summary>Provides shared helpers for normalized directory path processing.</summary>
internal static class DirectoryPathHelpers
{
    /// <summary>Returns whether <paramref name="value" /> is a directory separator.</summary>
    /// <param name="value">Character to test.</param>
    /// <returns><see langword="true" /> when the character is a directory separator.</returns>
    internal static bool IsDirectorySeparator(char value) => value == Path.DirectorySeparatorChar || value == Path.AltDirectorySeparatorChar;

    /// <summary>Removes trailing directory separators without allocating a separator <see cref="char" /> array.</summary>
    /// <param name="path">Path that may end with separators.</param>
    /// <returns>The original string when no trailing separators exist; otherwise a trimmed copy.</returns>
    internal static string TrimTrailingSeparators(string path)
    {
        var length = path.Length;
        var rootLength = Path.GetPathRoot(path)?.Length ?? 0;
        while (length > rootLength && IsDirectorySeparator(path[length - 1]))
            length--;

        return length == path.Length ? path : path[..length];
    }
}
