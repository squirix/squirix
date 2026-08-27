using System;
using System.IO;
using Microsoft.Win32.SafeHandles;

namespace Squirix.Server.Storage.Manifest;

/// <summary>
/// Shared-friendly reads for the fixed-size <c language="csharp">man-current</c> pointer.
/// </summary>
/// <remarks>
/// The journal-roll writer opens <c language="csharp">man-current</c> with
/// <see cref="FileShare.ReadWrite" /> | <see cref="FileShare.Delete" />. Readers must use a compatible
/// share mode: <c language="csharp">File.ReadAllBytesAsync</c> defaults to
/// <see cref="FileShare.Read" />, which can fail on Windows with <see cref="IOException" /> when a
/// writer handle is still draining after abrupt host disposal.
/// </remarks>
internal static class PointerFile
{
    internal const FileShare CompatibleShare = FileShare.ReadWrite | FileShare.Delete;

    /// <summary>Reads and validates the SQMC pointer at <paramref name="path" />, returning the manifest index.</summary>
    /// <param name="path">Absolute path to <c language="csharp">man-current</c>.</param>
    /// <returns>Manifest index encoded in the pointer.</returns>
    /// <exception cref="InvalidDataException">Thrown when the pointer is truncated or fails validation.</exception>
    internal static int ReadIndex(string path)
    {
        using var handle = Open(path);
        return ReadIndex(handle, path);
    }

    private static SafeFileHandle Open(string path) =>
        File.OpenHandle(path, FileMode.Open, FileAccess.Read, CompatibleShare, FileOptions.SequentialScan);

    private static int ReadIndex(SafeFileHandle handle, string path)
    {
        var length = RandomAccess.GetLength(handle);
        if (length != Pointer.Size)
            throw new InvalidDataException($"Manifest current pointer has invalid length {length}: {path}");

        Span<byte> local = stackalloc byte[Pointer.Size];
        var read = RandomAccess.Read(handle, local, 0);
        if (read != Pointer.Size)
            throw new InvalidDataException($"Manifest current pointer is truncated ({read} bytes): {path}");

        if (!Pointer.IsValidPointer(local))
            throw new InvalidDataException($"Manifest current pointer is invalid: {path}");

        return Pointer.Read(local);
    }
}
