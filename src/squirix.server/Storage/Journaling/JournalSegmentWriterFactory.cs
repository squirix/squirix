using System;
using System.IO;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Creates <see cref="IJournalSegmentWriter" /> instances for Pipelined.</summary>
internal static class JournalSegmentWriterFactory
{
    /// <summary>Creates the memory-safe <see cref="RandomAccess" /> segment writer.</summary>
    /// <returns>A new segment writer.</returns>
    /// <remarks>
    /// On Linux, io_uring batching could make segment writes and fsync faster: stage the submission entries and issue one ring-enter
    /// per group commit instead of one flush per op. That raw ring was archived after it crashed with a fatal access violation; the
    /// code now lives in the private repo squirix-linux-iouring. Reintroduce it from there once it is proven safe.
    /// </remarks>
    internal static IJournalSegmentWriter Create() => new RandomAccessJournalSegmentWriter();

    /// <summary><see cref="RandomAccess" />-based segment writer with write-through on Windows.</summary>
    private sealed class RandomAccessJournalSegmentWriter : IJournalSegmentWriter
    {
        private const string SegmentNotOpenMessage = "segment is not open.";

        private SafeFileHandle? _handle;

        public long Length
        {
            get
            {
                var handle = ThrowHelper.Required(_handle, SegmentNotOpenMessage);
                return RandomAccess.GetLength(handle);
            }
        }

        public void Dispose()
        {
            _handle?.Dispose();
            _handle = null;
        }

        public void FlushToDisk()
        {
            var handle = ThrowHelper.Required(_handle, SegmentNotOpenMessage);

            // FileOptions.WriteThrough on OpenSegment: each Write is durable without FlushToDisk.
            // WriteThrough + FlushAsync per append (not full disk flush per op).
            if (OperatingSystem.IsWindows())
                return;

            RandomAccess.FlushToDisk(handle);
        }

        public void OpenSegment(string path, bool append)
        {
            ArgumentException.ThrowIfNullOrEmpty(path);
            var mode = append ? FileMode.OpenOrCreate : FileMode.Create;
            var options = FileOptions.Asynchronous;
            if (OperatingSystem.IsWindows())
                options |= FileOptions.WriteThrough;

            _handle?.Dispose();
            _handle = File.OpenHandle(path, mode, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete, options);
        }

        public void Truncate(long length)
        {
            var handle = ThrowHelper.Required(_handle, SegmentNotOpenMessage);
            RandomAccess.SetLength(handle, length);
        }

        public void Write(ReadOnlySpan<byte> buffer, long fileOffset)
        {
            var handle = ThrowHelper.Required(_handle, SegmentNotOpenMessage);
            RandomAccess.Write(handle, buffer, fileOffset);
        }
    }
}
