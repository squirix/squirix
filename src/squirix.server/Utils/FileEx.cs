using System;
using System.IO;

namespace Squirix.Server.Utils;

internal static class FileEx
{
    /// <summary>
    /// Publishes a temp file as the final durable file, replacing an existing destination when present.
    /// </summary>
    /// <param name="tempPath">Path to the fully written temp file.</param>
    /// <param name="finalPath">Destination path that should reference <paramref name="tempPath" /> after completion.</param>
    /// <param name="backupPath">Optional backup path used when <paramref name="finalPath" /> already exists.</param>
    /// <param name="ignoreMetadataErrors">
    /// When <see langword="true" />, metadata differences between source and destination are ignored during
    /// <see cref="File.Replace(string, string, string?, bool)" />.
    /// </param>
    public static void PublishFile(string tempPath, string finalPath, string? backupPath = null, bool ignoreMetadataErrors = false)
    {
        if (File.Exists(finalPath))
            File.Replace(tempPath, finalPath, backupPath, ignoreMetadataErrors);
        else
            File.Move(tempPath, finalPath);
    }

    /// <summary>
    /// Attempts to delete a file at the given <paramref name="path" />.
    /// </summary>
    /// <param name="path">
    /// Absolute or relative path to the file to delete. If <see langword="null" />, empty, or whitespace-only,
    /// the method succeeds without performing any action. If the string contains any character from
    /// <see cref="Path.GetInvalidPathChars" />, the method succeeds without calling file APIs.
    /// </param>
    /// <returns>
    /// <see langword="true" /> when the path is skipped as invalid, the file did not exist, or deletion completed;
    /// <see langword="false" /> when deletion was attempted but failed.
    /// </returns>
    /// <remarks>
    /// Best-effort cleanup helper for teardown paths where callers ignore failures.
    /// For strict deletion semantics, use <see cref="File.Delete(string)" /> directly.
    /// </remarks>
    public static bool TryDeleteFile(string? path) => string.IsNullOrWhiteSpace(path) || path.IndexOfAny(Path.GetInvalidPathChars()) >= 0 || TryDeleteExistingFile(path);

    internal static string? FindFile(string[] paths)
    {
        var cwd = Directory.GetCurrentDirectory();
        foreach (var name in paths)
        {
            var p = PathEx.Combine(cwd, name);
            if (File.Exists(p))
                return p;
        }

        var baseDir = AppContext.BaseDirectory;
        foreach (var name in paths)
        {
            var p = PathEx.Combine(baseDir, name);
            if (File.Exists(p))
                return p;
        }

        return null;
    }

    private static bool TryDeleteExistingFile(string validatedPath)
    {
        try
        {
            if (!File.Exists(validatedPath))
                return true;

            File.Delete(validatedPath);
            return true;
        }
        catch (IOException)
        {
            return false;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }
    }
}
