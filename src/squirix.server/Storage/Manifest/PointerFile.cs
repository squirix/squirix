using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;

namespace Squirix.Server.Storage.Manifest;

/// <summary>
/// Shared-friendly reads for the fixed-size <c>man-current</c> pointer.
/// </summary>
/// <remarks>
/// The journal-roll writer opens <c>man-current</c> with
/// <see cref="FileShare.ReadWrite" /> | <see cref="FileShare.Delete" />. Readers must use a compatible
/// share mode: <see cref="File.ReadAllBytesAsync(string, CancellationToken)" /> defaults to
/// <see cref="FileShare.Read" />, which can fail on Windows with <see cref="IOException" /> when a
/// writer handle is still draining after abrupt host disposal.
/// </remarks>
internal static class PointerFile
{
    internal const FileShare CompatibleShare = FileShare.ReadWrite | FileShare.Delete;

    /// <summary>Reads the SQMC pointer bytes from <paramref name="path" /> with a writer-compatible share mode.</summary>
    /// <param name="path">Absolute path to <c>man-current</c>.</param>
    /// <returns>Exactly <see cref="Pointer.Size" /> bytes.</returns>
    /// <exception cref="InvalidDataException">Thrown when the file length is not <see cref="Pointer.Size" />.</exception>
    internal static byte[] ReadAllBytes(string path)
    {
        using var handle = Open(path);
        return ReadExact(handle, path);
    }

    /// <summary>Asynchronously reads the SQMC pointer bytes from <paramref name="path" /> with a writer-compatible share mode.</summary>
    /// <param name="path">Absolute path to <c>man-current</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Exactly <see cref="Pointer.Size" /> bytes.</returns>
    /// <exception cref="InvalidDataException">Thrown when the file length is not <see cref="Pointer.Size" />.</exception>
    internal static async Task<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken)
    {
        // OpenHandle has no async counterpart; the subsequent RandomAccess read is a tiny fixed-size payload.
#pragma warning disable MA0045
        using var handle = Open(path);
        cancellationToken.ThrowIfCancellationRequested();
        return ReadExact(handle, path);
#pragma warning restore MA0045
    }

    private static SafeFileHandle Open(string path) =>
        File.OpenHandle(path, FileMode.Open, FileAccess.Read, CompatibleShare, FileOptions.SequentialScan);

    private static byte[] ReadExact(SafeFileHandle handle, string path)
    {
        var length = RandomAccess.GetLength(handle);
        if (length != Pointer.Size)
            throw new InvalidDataException($"Manifest current pointer has invalid length {length}: {path}");

        Span<byte> local = stackalloc byte[Pointer.Size];
        var read = RandomAccess.Read(handle, local, 0);
        if (read != Pointer.Size)
            throw new InvalidDataException($"Manifest current pointer is truncated ({read} bytes): {path}");

        // Callers (Pointer.IsValidPointer / cache seeding) require a durable byte[] owner.
#pragma warning disable ZA0301
        var buffer = new byte[Pointer.Size];
#pragma warning restore ZA0301
        local.CopyTo(buffer);
        return buffer;
    }
}
