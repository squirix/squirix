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

    /// <summary>Reads the manifest at <paramref name="path" /> and selects the newest valid slot.</summary>
    /// <param name="path">Absolute path to <c language="csharp">man-current</c>.</param>
    /// <returns>The selected record; a missing file is a never published manifest.</returns>
    /// <exception cref="InvalidDataException">The file was written by an earlier build or holds no valid slot.</exception>
    internal static SlotSelection Read(string path)
    {
        try
        {
            using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, CompatibleShare);
            return Read(handle, path);
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            return new SlotSelection(new State(), 1, 0UL);
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
