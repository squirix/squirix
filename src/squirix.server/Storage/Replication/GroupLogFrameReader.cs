using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Reads committed frames back from a group log file at their recorded offsets.</summary>
internal static class GroupLogFrameReader
{
    private const int FrameHeaderByteCount = GroupLogCodec.FramePreambleByteCount + sizeof(int);

    /// <summary>Reads the frames that start at <paramref name="frames" />, in the given order, handing each entry to <paramref name="visit" />.</summary>
    /// <param name="logPath">The group log file.</param>
    /// <param name="frames">The index and file offset of each frame to read.</param>
    /// <param name="visit">Called with each entry; one entry is held at a time, and returning <see langword="false" /> stops the read.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of entries read.</returns>
    /// <exception cref="InvalidDataException">A frame is torn, fails its checksum, or carries another index: committed frames never do.</exception>
    /// <remarks>The file is opened for reading next to the writer, which shares reads; callers hold the log gate so no frame moves meanwhile.</remarks>
    internal static async Task<int> ReadAsync(string logPath, IReadOnlyList<(ulong LogIndex, long Offset)> frames, Func<FollowerLogEntry, bool> visit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(frames);
        ArgumentNullException.ThrowIfNull(visit);
        if (frames.Count == 0)
            return 0;

        using var handle = File.OpenHandle(logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.Asynchronous);
        var header = ArrayPool<byte>.Shared.Rent(FrameHeaderByteCount);
        var read = 0;
        try
        {
            for (var i = 0; i < frames.Count; i++)
            {
                var (logIndex, offset) = frames[i];
                var headerEnd = await HandleEx.ReadExactAsync(handle, header.AsMemory(0, FrameHeaderByteCount), offset, cancellationToken).ConfigureAwait(false);
                if (headerEnd == null || !GroupLogCodec.TryReadFrameHeaderLength(header.AsSpan(0, FrameHeaderByteCount), out var frameLength))
                    throw new InvalidDataException($"Committed group log frame {logIndex} at offset {offset} is torn.");

                read++;
                if (!visit(await ReadFrameAsync(handle, header, frameLength, logIndex, offset, cancellationToken).ConfigureAwait(false)))
                    break;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(header);
        }

        return read;
    }

    private static async Task<FollowerLogEntry> ReadFrameAsync(
        SafeFileHandle handle,
        byte[] header,
        int frameLength,
        ulong logIndex,
        long offset,
        CancellationToken cancellationToken)
    {
        var frame = ArrayPool<byte>.Shared.Rent(frameLength);
        try
        {
            header.AsSpan(0, FrameHeaderByteCount).CopyTo(frame);
            var frameEnd = await HandleEx.ReadExactAsync(handle, frame.AsMemory(FrameHeaderByteCount, frameLength - FrameHeaderByteCount), offset + FrameHeaderByteCount, cancellationToken)
                                         .ConfigureAwait(false);
            return frameEnd != null && GroupLogCodec.TryReadFrame(frame.AsSpan(0, frameLength), out var entry) && entry.LogIndex == logIndex ? entry
                : throw new InvalidDataException($"Committed group log frame {logIndex} at offset {offset} is corrupt.");
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(frame);
        }
    }
}
