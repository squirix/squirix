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
    internal static async Task<int> ReadAsync(
        string logPath,
        IEnumerable<(ulong LogIndex, long Offset)> frames,
        Func<FollowerLogEntry, bool> visit,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(frames);
        ArgumentNullException.ThrowIfNull(visit);
        using var locations = frames.GetEnumerator();
        if (!locations.MoveNext())
            return 0;

        using var handle = File.OpenHandle(logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.Asynchronous);
        var fileLength = RandomAccess.GetLength(handle);
        var header = ArrayPool<byte>.Shared.Rent(FrameHeaderByteCount);
        byte[]? frame = null;
        var read = 0;
        try
        {
            do
            {
                var (logIndex, offset) = locations.Current;
                var frameLength = await ReadFrameLengthAsync(handle, header, logIndex, offset, fileLength, cancellationToken).ConfigureAwait(false);
                frame = EnsureFrameCapacity(frame, frameLength);
                read++;
                if (!visit(await ReadFrameAsync(handle, header, frame, frameLength, logIndex, offset, cancellationToken).ConfigureAwait(false)))
                    break;
            }
            while (locations.MoveNext());
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(header);
            if (frame != null)
                ArrayPool<byte>.Shared.ReturnCleared(frame);
        }

        return read;
    }

    /// <summary>Reads every frame that starts at <paramref name="frames" />, in the given order.</summary>
    /// <param name="logPath">The group log file.</param>
    /// <param name="frames">The index and file offset of each frame to read.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>One entry per frame, in the order of <paramref name="frames" />.</returns>
    /// <exception cref="InvalidDataException">A frame is torn, fails its checksum, or carries another index: committed frames never do.</exception>
    /// <remarks>The file is opened for reading next to the writer, which shares reads; callers hold the log gate so no frame moves meanwhile.</remarks>
    internal static async Task<FollowerLogEntry[]> ReadAllAsync(string logPath, ArraySegment<(ulong LogIndex, long Offset)> frames, CancellationToken cancellationToken)
    {
        if (frames.Count == 0)
            return [];

        var entries = new FollowerLogEntry[frames.Count];
        using var handle = File.OpenHandle(logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.Asynchronous);
        var fileLength = RandomAccess.GetLength(handle);
        var header = ArrayPool<byte>.Shared.Rent(FrameHeaderByteCount);
        byte[]? frame = null;
        try
        {
            for (var i = 0; i < entries.Length; i++)
            {
                var (logIndex, offset) = frames[i];
                var frameLength = await ReadFrameLengthAsync(handle, header, logIndex, offset, fileLength, cancellationToken).ConfigureAwait(false);
                frame = EnsureFrameCapacity(frame, frameLength);
                entries[i] = await ReadFrameAsync(handle, header, frame, frameLength, logIndex, offset, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(header);
            if (frame != null)
                ArrayPool<byte>.Shared.ReturnCleared(frame);
        }

        return entries;
    }

    private static async Task<int> ReadFrameLengthAsync(SafeFileHandle handle, byte[] header, ulong logIndex, long offset, long fileLength, CancellationToken cancellationToken)
    {
        var headerEnd = await HandleEx.ReadExactAsync(handle, header.AsMemory(0, FrameHeaderByteCount), offset, cancellationToken).ConfigureAwait(false);
        var frameLength = headerEnd != null && GroupLogCodec.TryReadFrameHeaderLength(header.AsSpan(0, FrameHeaderByteCount), out var length)
            ? length
            : throw new InvalidDataException($"Committed group log frame {logIndex} at offset {offset} is torn.");

        return fileLength - (offset + FrameHeaderByteCount) < frameLength - FrameHeaderByteCount
            ? throw new InvalidDataException($"Committed group log frame {logIndex} at offset {offset} is corrupt.")
            : frameLength;
    }

    private static byte[] EnsureFrameCapacity(byte[]? frame, int frameLength)
    {
        if (frame != null && frame.Length >= frameLength)
            return frame;

        var larger = ArrayPool<byte>.Shared.Rent(frameLength);
        if (frame != null)
            ArrayPool<byte>.Shared.ReturnCleared(frame);
        return larger;
    }

    private static async Task<FollowerLogEntry> ReadFrameAsync(
        SafeFileHandle handle,
        byte[] header,
        byte[] frame,
        int frameLength,
        ulong logIndex,
        long offset,
        CancellationToken cancellationToken)
    {
        header.AsSpan(0, FrameHeaderByteCount).CopyTo(frame);
        var frameEnd = await HandleEx.ReadExactAsync(
            handle,
            frame.AsMemory(FrameHeaderByteCount, frameLength - FrameHeaderByteCount),
            offset + FrameHeaderByteCount,
            cancellationToken).ConfigureAwait(false);
        return frameEnd != null && GroupLogCodec.TryReadFrame(frame.AsSpan(0, frameLength), out var entry) && entry.LogIndex == logIndex
            ? entry
            : throw new InvalidDataException($"Committed group log frame {logIndex} at offset {offset} is corrupt.");
    }
}
