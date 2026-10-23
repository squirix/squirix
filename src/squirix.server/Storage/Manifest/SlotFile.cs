using System;
using System.IO;
using Microsoft.Win32.SafeHandles;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Shared-friendly reads for the two-slot <c language="csharp">man-current</c> manifest file.</summary>
/// <remarks>
/// The slot writer holds <c language="csharp">man-current</c> open for the life of the ledger with <see cref="FileShare.Read" />. Readers must
/// open it for reading only, with a compatible share mode: <c language="csharp">File.ReadAllBytesAsync</c> defaults to
/// <see cref="FileShare.Read" />, which fails on Windows with <see cref="IOException" /> while that writer handle is open.
/// </remarks>
internal static class SlotFile
{
    internal const FileShare CompatibleShare = FileShare.ReadWrite | FileShare.Delete;

    private const UnixFileMode SearchPermission = UnixFileMode.UserExecute | UnixFileMode.GroupExecute | UnixFileMode.OtherExecute;

    private static readonly EnumerationOptions ListingOptions = new()
    {
        MatchType = MatchType.Simple,
        IgnoreInaccessible = false,
        RecurseSubdirectories = false,
    };

    /// <summary>Reads the manifest at <paramref name="path" /> and selects the newest valid slot.</summary>
    /// <param name="path">Absolute path to <c language="csharp">man-current</c>.</param>
    /// <returns>The selected record; a missing file is a never published manifest.</returns>
    /// <exception cref="InvalidDataException">The file was written by an earlier build or holds no valid slot.</exception>
    internal static SlotSelection Read(string path)
    {
        if (IsDefinitelyMissing(path))
            return NeverPublished();

        try
        {
            using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, CompatibleShare);
            return Read(handle, path);
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            return NeverPublished();
        }
    }

    /// <summary>Reads the manifest through an open handle.</summary>
    /// <param name="handle">A handle with read access.</param>
    /// <param name="path">The file path, for messages.</param>
    /// <returns>The selected record.</returns>
    /// <exception cref="InvalidDataException">The file was written by an earlier build or holds no valid slot.</exception>
    internal static SlotSelection Read(SafeFileHandle handle, string path)
    {
        var length = RandomAccess.GetLength(handle);
        Span<byte> first = stackalloc byte[SlotFormat.RecordSize];
        Span<byte> second = stackalloc byte[SlotFormat.RecordSize];
        var firstCount = ReadRecord(handle, 0, length, first);
        var secondCount = ReadRecord(handle, SlotFormat.SlotSize, length, second);
        return SlotFormat.Select(first[..firstCount], second[..secondCount], length, path);
    }

    private static SlotSelection NeverPublished() => new(new State(), 1, 0UL);

    /// <summary>Tells whether the open can be skipped because the file is absent from an existing parent directory.</summary>
    /// <remarks>
    /// The path must be absent and the directory listing must succeed and hold no entry with the file name. Every other outcome,
    /// including a directory at the path, a parent that cannot be listed or traversed, or an invalid path, goes through the open so it
    /// fails as before.
    /// </remarks>
    /// <param name="path">Path to <c language="csharp">man-current</c>.</param>
    /// <returns><see langword="true" /> when the file is definitely absent.</returns>
    private static bool IsDefinitelyMissing(string path)
    {
        var parent = Path.GetDirectoryName(path);
        var name = Path.GetFileName(path);
        if (string.IsNullOrEmpty(parent) || name.Length == 0 || name.AsSpan().IndexOfAny('*', '?') >= 0)
            return false;

        if (!Directory.Exists(parent) || Path.Exists(path))
            return false;

        try
        {
            // A parent without search permission fails the open with an access error even when the file is absent, so on Unix only
            // a parent that grants search to owner, group and others (permission bits cannot tell which class applies) skips the open.
            if (!OperatingSystem.IsWindows() && (File.GetUnixFileMode(parent) & SearchPermission) != SearchPermission)
                return false;

            using var entries = Directory.EnumerateFileSystemEntries(parent, name, ListingOptions).GetEnumerator();
            return !entries.MoveNext();
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return false;
        }
    }

    private static int ReadRecord(SafeFileHandle handle, long offset, long length, Span<byte> destination)
    {
        var wanted = int.CreateTruncating(Math.Clamp(length - offset, 0L, destination.Length));
        var read = 0;
        while (read < wanted)
        {
            var count = RandomAccess.Read(handle, destination.Slice(read, wanted - read), offset + read);
            if (count == 0)
                break;

            read += count;
        }

        return read;
    }
}
