using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.TestKit.Replication;

/// <summary>Reads and hand-writes a replica group metadata file, so tests do not depend on its byte layout.</summary>
internal static class GroupMetadataFile
{
    /// <summary>Reads the newest valid record of a metadata file, also while a log holds the file open.</summary>
    /// <param name="path">The metadata file path.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The record.</returns>
    /// <exception cref="InvalidDataException">The file holds no valid record.</exception>
    internal static async Task<GroupLogMetadata> ReadAsync(string path, CancellationToken cancellationToken)
    {
        var bytes = await ReadBytesAsync(path, cancellationToken).ConfigureAwait(false);
        var read = GroupMetadataSlots.Select(bytes);
        return read.Status == GroupMetadataReadStatus.Valid
            ? read.Metadata
            : throw new InvalidDataException($"The metadata file '{path}' holds no valid record ({read.Status}).");
    }

    /// <summary>Reads the raw bytes of a metadata file, also while a log holds the file open; for comparing a file before and after an operation.</summary>
    /// <param name="path">The metadata file path.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The bytes of the file.</returns>
    /// <exception cref="EndOfStreamException">The file shrank while it was read.</exception>
    internal static async Task<byte[]> ReadBytesAsync(string path, CancellationToken cancellationToken)
    {
        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.Asynchronous);
        var bytes = new byte[RandomAccess.GetLength(handle)];
        var read = 0;
        while (read < bytes.Length)
        {
            var count = await RandomAccess.ReadAsync(handle, bytes.AsMemory(read), read, cancellationToken).ConfigureAwait(false);
            if (count == 0)
                throw new EndOfStreamException($"The metadata file '{path}' ended after {read} bytes.");

            read += count;
        }

        return bytes;
    }

    /// <summary>Writes a metadata file holding exactly one record, replacing any file at the path.</summary>
    /// <param name="path">The metadata file path.</param>
    /// <param name="meta">The record.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the file is written.</returns>
    internal static Task WriteAsync(string path, in GroupLogMetadata meta, CancellationToken cancellationToken)
    {
        var image = new byte[GroupMetadataSlots.FileSize];
        GroupMetadataSlots.Encode(image.AsSpan(0, GroupMetadataSlots.SlotSize), 1UL, in meta);
        return File.WriteAllBytesAsync(path, image, cancellationToken);
    }
}
