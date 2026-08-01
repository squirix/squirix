using System;
using System.IO;

namespace Squirix.Server.Utils;

/// <summary>Rejects unexpected symlinks and junctions in directory path chains.</summary>
internal static class DirectorySymlinkGuard
{
    /// <summary>Walks from <paramref name="baseFull" /> (or the drive root) toward <paramref name="full" /> and rejects forbidden links.</summary>
    /// <param name="full">Absolute target path.</param>
    /// <param name="baseFull">Optional absolute base path already validated.</param>
    /// <exception cref="IOException">Thrown when a non-allowlisted symlink or junction is found in the chain.</exception>
    internal static void EnsureNoSymlinksInChain(string full, string? baseFull)
    {
        if (!TryPrepareChainWalk(full, baseFull, out var cur, out var relative))
            return;

        while (DirectoryPathValidator.TryReadNextSegment(ref relative, out var segment))
        {
            if (!TryAdvancePastExistingSegment(ref cur, segment))
                break;
        }
    }

    /// <summary>Throws when <paramref name="full" /> is a symlink/junction and <paramref name="forbidSymlinks" /> is <see langword="true" />.</summary>
    /// <param name="full">Absolute directory path.</param>
    /// <param name="created"><see langword="true" /> when the directory was just created.</param>
    /// <param name="forbidSymlinks">When <see langword="false" />, the check is skipped.</param>
    /// <exception cref="IOException">Thrown when the target is a symlink or junction.</exception>
    internal static void EnsureRegularDirectory(string full, bool created, bool forbidSymlinks)
    {
        if (!forbidSymlinks)
            return;

        var info = new DirectoryInfo(full);
        if (!IsSymlink(info))
            return;

        throw new IOException(created ? "Created directory resolved to a symlink/junction." : "Target directory is a symlink/junction.");
    }

    /// <summary>Returns whether <paramref name="fsi" /> is a symbolic link or reparse point.</summary>
    /// <param name="fsi">File-system entry to inspect.</param>
    /// <returns><see langword="true" /> when the entry appears to be a link.</returns>
    internal static bool IsSymlink(FileSystemInfo fsi)
    {
        try
        {
            // .NET 6+ cross-platform symlink test
            if (fsi.LinkTarget is not null)
                return true;
        }
        catch (IOException)
        {
            // Some FS/providers may throw; fall back to attributes
        }
        catch (UnauthorizedAccessException)
        {
            // Some FS/providers may throw; fall back to attributes
        }
        catch (NotSupportedException)
        {
            // LinkTarget may be unsupported on some providers; fall back to attributes
        }

        try
        {
            return (fsi.Attributes & FileAttributes.ReparsePoint) is not FileAttributes.None;
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

    private static bool TryPrepareChainWalk(string full, string? baseFull, out string cur, out ReadOnlySpan<char> relative)
    {
        var start = baseFull ?? Path.GetPathRoot(full)!;
        relative = full.AsSpan(start.Length);
        while (relative.Length > 0 && DirectoryPathValidator.IsDirectorySeparator(relative[0]))
            relative = relative[1..];

        if (relative.IsEmpty)
        {
            cur = string.Empty;
            return false;
        }

        // Trimming trailing separators can turn a root-only path into an empty string
        // (for example "/" on Unix). Preserve the original root as the seed when that happens.
        var trimmedStart = DirectoryPathValidator.TrimTrailingSeparators(start);
        cur = trimmedStart.Length is 0 && start.Length > 0 ? start : trimmedStart;
        return true;
    }

    private static bool TryAdvancePastExistingSegment(ref string cur, ReadOnlySpan<char> segment)
    {
        cur = Path.Join(cur.AsSpan(), segment);
        var di = new DirectoryInfo(cur);
        if (!di.Exists)
            return false;

        if (!IsSymlink(di))
            return true;

        // macOS ships compatibility symlinks (/var -> /private/var, /tmp -> /private/tmp, /etc -> /private/etc).
        // Follow only those well-known OS links; any other symlink/junction remains forbidden.
        if (!MacOsCompatibilitySymlink.TryFollow(di, out var resolved))
            throw new IOException("Symlink/junction detected in path.");

        cur = resolved;
        return true;
    }
}
