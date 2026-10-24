using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Versioning;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage;

/// <summary>Restricts the entries a data directory already holds to the owner, so data written by an earlier version is not left readable by other users.</summary>
internal static class DataDirectoryPermissions
{
    private const string DirectoryModeText = "0700";

    private const string FileModeText = "0600";

    private const UnixFileMode GroupAndOtherBits =
        UnixFileMode.GroupRead | UnixFileMode.GroupWrite | UnixFileMode.GroupExecute |
        UnixFileMode.OtherRead | UnixFileMode.OtherWrite | UnixFileMode.OtherExecute;

    /// <summary>Restricts every file below <paramref name="directory" /> to 0600 and every directory, including it, to 0700 where group or other bits are set.</summary>
    /// <param name="directory">The data directory; its lock must already be held.</param>
    /// <returns>The number of entries whose mode was changed; always zero on Windows, which has no Unix modes.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the mode of an entry cannot be read or changed, for example because another user owns it.
    /// Startup fails closed: continuing would keep serving a cache from files other users can read.
    /// </exception>
    /// <remarks>Symbolic links are skipped and never followed, so a link cannot redirect the change to a file outside the data directory.</remarks>
    internal static int TightenExisting(string directory)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        if (OperatingSystem.IsWindows())
            return 0;

        var tightened = 0;
        var pending = new Stack<DirectoryInfo>();
        pending.Push(new DirectoryInfo(directory));
        while (pending.Count > 0)
        {
            var current = pending.Pop();
            if (TightenEntry(current, DirectoryEx.PrivateDirectoryMode, DirectoryModeText))
                tightened++;

            tightened += TightenFiles(current, pending);
        }

        return tightened;
    }

    [UnsupportedOSPlatform("windows")]
    private static int TightenFiles(DirectoryInfo directory, Stack<DirectoryInfo> pending)
    {
        var tightened = 0;
        try
        {
            foreach (var entry in directory.EnumerateFileSystemInfos())
            {
                if (entry.LinkTarget != null)
                    continue;

                if (entry is DirectoryInfo child)
                    pending.Push(child);
                else if (TightenEntry(entry, FileEx.PrivateFileMode, FileModeText))
                    tightened++;
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new InvalidOperationException($"Cannot list data directory entry '{directory.FullName}' to restrict its permissions: {ex.Message}", ex);
        }

        return tightened;
    }

    [UnsupportedOSPlatform("windows")]
    private static bool TightenEntry(FileSystemInfo entry, UnixFileMode privateMode, string modeText)
    {
        try
        {
            if ((entry.UnixFileMode & GroupAndOtherBits) == UnixFileMode.None)
                return false;

            entry.UnixFileMode = privateMode;
            return true;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new InvalidOperationException(
                $"Cannot restrict '{entry.FullName}' to its owner (mode {modeText}): {ex.Message} " +
                "Run the node as the user that owns the data directory, or fix its ownership and permissions.",
                ex);
        }
    }
}
