using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Versioning;
using Squirix.Server.Node.Replication;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

/// <summary>Restricts the entries the node itself created in a data directory to its owner, so data written by an earlier version is not left readable by other users.</summary>
/// <remarks>
/// Only entries with the names the node creates are touched: journal segments and their temp files, snapshots and their temp files, the manifest,
/// the lock file, the activated-topology stamp and its temp file, the replication directory, the group directories, and the group store files.
/// Everything else in the directory, such as operator files or <c language="csharp">lost+found</c>, is never opened, listed or changed.
/// </remarks>
internal static class DataDirectoryPermissions
{
    private const string DirectoryModeText = "0700";

    private const string FileModeText = "0600";

    private const UnixFileMode GroupAndOtherBits =
        UnixFileMode.GroupRead | UnixFileMode.GroupWrite | UnixFileMode.GroupExecute |
        UnixFileMode.OtherRead | UnixFileMode.OtherWrite | UnixFileMode.OtherExecute;

    private static readonly FrozenSet<string> TopLevelFileNames = FrozenSet.ToFrozenSet(
        [DataDirectoryLock.LockFileName, Ledger.CurrentFileName, ActivatedTopologyStampStore.StampFileName, ActivatedTopologyStampStore.StampTempFileName],
        StringComparer.Ordinal);

    /// <summary>Restricts the node's own entries in <paramref name="directory" /> to 0600 for files and 0700 for directories where group or other bits are set.</summary>
    /// <param name="directory">The data directory; its lock must already be held.</param>
    /// <returns>The outcome; always an empty one on Windows, which has no Unix modes.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the mode of one of the node's own entries cannot be read or changed, for example because another user owns it.
    /// Startup fails closed: continuing would keep serving a cache from files other users can read.
    /// </exception>
    /// <remarks>
    /// A refused change of the data directory itself does not fail: the directory may belong to another user, as a mounted volume often does,
    /// and the node's own entries inside it are still restricted. The refusal is reported in the result.
    /// Symbolic links under a matched name are skipped and never followed. An entry that disappears during the walk is ignored.
    /// </remarks>
    internal static Outcome TightenExisting(string directory)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        if (OperatingSystem.IsWindows())
            return default;

        var root = new DirectoryInfo(directory);
        var rootRefused = !TryTightenRoot(root, out var rootTightened);
        var tightened = rootTightened ? 1 : 0;
        var pending = new Stack<(DirectoryInfo Directory, bool IsRoot)>();
        pending.Push((root, true));
        while (pending.Count > 0)
        {
            var (current, isRoot) = pending.Pop();
            if (!isRoot && TightenEntry(current, DirectoryEx.PrivateDirectoryMode, DirectoryModeText))
                tightened++;

            tightened += TightenChildren(current, isRoot, pending);
        }

        return new Outcome(tightened, rootRefused);
    }

    private static bool IsOwnDirectory(DirectoryInfo parent, bool parentIsRoot, string name) =>
        parentIsRoot
            ? string.Equals(name, GroupStoragePaths.RootSegment, StringComparison.Ordinal)
            : string.Equals(parent.Name, GroupStoragePaths.RootSegment, StringComparison.Ordinal) && GroupStoragePaths.IsGroupDirectoryName(name);

    private static bool IsOwnFile(DirectoryInfo parent, bool parentIsRoot, string name) =>
        parentIsRoot ? IsOwnTopLevelFile(name) : GroupStoragePaths.IsGroupFileName(name) && GroupStoragePaths.IsGroupDirectoryName(parent.Name);

    private static bool IsOwnTopLevelFile(string name) =>
        TopLevelFileNames.Contains(name) ||
        IsStagedOrFinal(name, FilePrefixes.Journal, FileExtensions.Journal) ||
        IsStagedOrFinal(name, FilePrefixes.Snapshot, FileExtensions.Snapshot);

    private static bool IsStagedOrFinal(string name, string prefix, string extension) =>
        name.StartsWith(prefix, StringComparison.Ordinal) &&
        (name.EndsWith(extension, StringComparison.Ordinal) || name.EndsWith(FileExtensions.Temp, StringComparison.Ordinal));

    [UnsupportedOSPlatform("windows")]
    private static int TightenChildren(DirectoryInfo directory, bool isRoot, Stack<(DirectoryInfo Directory, bool IsRoot)> pending)
    {
        var tightened = 0;
        try
        {
            foreach (var entry in directory.EnumerateFileSystemInfos())
            {
                if (entry.LinkTarget != null)
                    continue;

                if (entry is DirectoryInfo child)
                {
                    if (IsOwnDirectory(directory, isRoot, child.Name))
                        pending.Push((child, false));
                }
                else if (IsOwnFile(directory, isRoot, entry.Name) && TightenEntry(entry, FileEx.PrivateFileMode, FileModeText))
                {
                    tightened++;
                }
            }
        }
        catch (DirectoryNotFoundException)
        {
            // The directory was removed while it was being listed.
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
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            return false;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new InvalidOperationException(
                $"Cannot restrict '{entry.FullName}' to its owner (mode {modeText}): {ex.Message} " +
                "Run the node as the user that owns the data directory, or fix its ownership and permissions.",
                ex);
        }
    }

    [UnsupportedOSPlatform("windows")]
    private static bool TryTightenRoot(DirectoryInfo root, out bool tightened)
    {
        tightened = false;
        try
        {
            tightened = TightenEntry(root, DirectoryEx.PrivateDirectoryMode, DirectoryModeText);
            return true;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    /// <summary>The result of tightening a data directory.</summary>
    /// <param name="Tightened">The number of entries whose mode was changed, including the data directory itself when it was changed.</param>
    /// <param name="RootRefused">Whether the data directory itself has group or other access and could not be restricted.</param>
    internal readonly record struct Outcome(int Tightened, bool RootRefused);
}
