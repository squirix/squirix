using System;
using System.IO;

namespace Squirix.Server.Utils;

/// <summary>Rejects unexpected symlinks and junctions in directory path chains.</summary>
internal static class DirectorySymlinkGuard
{
    /// <summary>Walks from <paramref name="baseFull" /> (or the drive root) toward <paramref name="full" /> and rejects forbidden links.</summary>
    /// <param name="full">Absolute target path.</param>
    /// <param name="baseFull">Optional absolute base path already validated.</param>
    /// <exception cref="IOException">
    /// Thrown when a non-allowlisted symlink or junction is found in the chain, or when the link status of an existing segment cannot be determined
    /// (fail closed).
    /// </exception>
    internal static void EnsureNoSymlinksInChain(string full, string? baseFull) => EnsureNoSymlinksInChain(full, baseFull, IsSymlink);

    /// <summary>Same as <see cref="EnsureNoSymlinksInChain(string, string?)" /> with an injectable link-status probe.</summary>
    /// <param name="full">Absolute target path.</param>
    /// <param name="baseFull">Optional absolute base path already validated.</param>
    /// <param name="isSymlink">Probe that reports whether an existing segment is a link and throws <see cref="IOException" /> when undeterminable.</param>
    /// <exception cref="IOException">Thrown when a forbidden link is found or the probe cannot determine the link status.</exception>
    internal static void EnsureNoSymlinksInChain(string full, string? baseFull, Func<FileSystemInfo, bool> isSymlink)
    {
        if (!TryPrepareChainWalk(full, baseFull, out var cur, out var relative))
            return;

        while (PathEx.TryReadNextSegment(ref relative, out var segment))
        {
            if (!TryAdvancePastExistingSegment(segment, ref cur, isSymlink))
                break;
        }
    }

    /// <summary>Throws when <paramref name="full" /> is a symlink/junction and <paramref name="forbidSymlinks" /> is <see langword="true" />.</summary>
    /// <param name="full">Absolute directory path.</param>
    /// <param name="created"><see langword="true" /> when the directory was just created.</param>
    /// <param name="forbidSymlinks">When <see langword="false" />, the check is skipped.</param>
    /// <exception cref="IOException">Thrown when the target is a symlink or junction, or when its link status cannot be determined (fail closed).</exception>
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
    /// <returns><see langword="true" /> when the entry is a link; <see langword="false" /> otherwise.</returns>
    /// <exception cref="IOException">
    /// Thrown when an existing entry's link status cannot be determined because its attributes are unreadable (fail closed: an undeterminable entry is
    /// never treated as regular).
    /// </exception>
    internal static bool IsSymlink(FileSystemInfo fsi) => IsSymlink(fsi, static entry => entry.LinkTarget, static entry => entry.Attributes);

    /// <summary>Same as <see cref="IsSymlink(FileSystemInfo)" /> with injectable link-target and attribute probes.</summary>
    /// <param name="fsi">File-system entry to inspect.</param>
    /// <param name="linkTargetProbe">Reads the link target; failures fall back to the attribute probe.</param>
    /// <param name="attributesProbe">Reads the entry attributes; failures other than a missing entry fail closed.</param>
    /// <returns><see langword="true" /> when the entry is a link; <see langword="false" /> otherwise.</returns>
    /// <exception cref="IOException">Thrown when the attributes are unreadable.</exception>
    internal static bool IsSymlink(FileSystemInfo fsi, Func<FileSystemInfo, string?> linkTargetProbe, Func<FileSystemInfo, FileAttributes> attributesProbe)
    {
        try
        {
            // .NET 6+ cross-platform symlink test
            if (linkTargetProbe(fsi) != null)
                return true;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or NotSupportedException)
        {
            // Some FS/providers throw or do not support LinkTarget; fall back to attributes.
        }

        try
        {
            return (attributesProbe(fsi) & FileAttributes.ReparsePoint) != FileAttributes.None;
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            // The entry does not exist, so it is not a link.
            return false;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new IOException($"Unable to determine whether '{fsi.FullName}' is a symlink/junction.", ex);
        }
    }

    private static bool TryAdvancePastExistingSegment(ReadOnlySpan<char> segment, ref string cur, Func<FileSystemInfo, bool> isSymlink)
    {
        cur = Path.Join(cur.AsSpan(), segment);
        var di = new DirectoryInfo(cur);
        if (!di.Exists)
            return false;

        if (!isSymlink(di))
            return true;

        // macOS ships compatibility symlinks (/var -> /private/var, /tmp -> /private/tmp, /etc -> /private/etc).
        // Follow only those well-known OS links; any other symlink/junction remains forbidden.
        if (!MacOsCompatibilitySymlink.TryFollow(di, out var resolved))
            throw new IOException("Symlink/junction detected in path.");

        cur = resolved;
        return true;
    }

    private static bool TryPrepareChainWalk(string full, string? baseFull, out string cur, out ReadOnlySpan<char> relative)
    {
        var start = baseFull ?? Path.GetPathRoot(full)!;
        relative = full.AsSpan(start.Length);
        while (relative.Length > 0 && DirectoryPathHelpers.IsDirectorySeparator(relative[0]))
            relative = relative[1..];

        if (relative.IsEmpty)
        {
            cur = string.Empty;
            return false;
        }

        // Trimming trailing separators can turn a root-only path into an empty string
        // (for example "/" on Unix). Preserve the original root as the seed when that happens.
        var trimmedStart = DirectoryPathHelpers.TrimTrailingSeparators(start);
        cur = trimmedStart.Length == 0 && start.Length > 0 ? start : trimmedStart;
        return true;
    }
}
