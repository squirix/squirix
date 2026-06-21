using System;
using System.IO;
using System.Runtime.Versioning;
using System.Threading.Tasks;
using Squirix.Server.Storage.Journaling.Pipelined.Platform.IoUring;

namespace Squirix.Server.Storage.Journaling.Pipelined.Platform;

/// <summary>
/// Linux io_uring segment writer. Uses a raw io_uring ring when the kernel supports it;
/// otherwise falls back to <see cref="RandomAccessJournalSegmentWriter"/>.
/// </summary>
[SupportedOSPlatform("linux")]
internal sealed class UringJournalSegmentWriter : IJournalSegmentWriter
{
    private readonly RandomAccessJournalSegmentWriter _fallback = new();
    private IoUringJournalRing? _ring;
    private int _fileDescriptor = -1;

    public long Length
    {
        get
        {
            if (!UsesIoUring)
                return _fallback.Length;
            EnsureIoUringOpen();
            return LinuxSegmentFile.GetLength(_fileDescriptor);
        }
    }

    internal bool UsesIoUring { get; private set; }

    public void OpenSegment(string path, bool append)
    {
        ResetActiveWriter();

        if (IoUringAvailability.IsSupported)
        {
            try
            {
                _ring = new IoUringJournalRing(32);
                _fileDescriptor = LinuxSegmentFile.Open(path, append);
                UsesIoUring = true;
                return;
            }
            catch (IOException)
            {
                ResetActiveWriter();
            }
        }

        _fallback.OpenSegment(path, append);
    }

    public void Write(ReadOnlySpan<byte> buffer, long fileOffset)
    {
        if (UsesIoUring)
        {
            EnsureIoUringOpen();
            _ring!.Write(_fileDescriptor, buffer, fileOffset);
            return;
        }

        _fallback.Write(buffer, fileOffset);
    }

    public void Fsync()
    {
        if (UsesIoUring)
        {
            EnsureIoUringOpen();
            _ring!.Fsync(_fileDescriptor);
            return;
        }

        _fallback.Fsync();
    }

    public void Truncate(long length)
    {
        if (UsesIoUring)
        {
            EnsureIoUringOpen();
            LinuxSegmentFile.Truncate(_fileDescriptor, length);
            return;
        }

        _fallback.Truncate(length);
    }

    public async ValueTask DisposeAsync()
    {
        ResetActiveWriter();
        await _fallback.DisposeAsync().ConfigureAwait(false);
    }

    private void EnsureIoUringOpen()
    {
        if (!UsesIoUring || _ring is null || _fileDescriptor < 0)
            throw new InvalidOperationException("segment is not open.");
    }

    private void ResetActiveWriter()
    {
        UsesIoUring = false;
        _ring?.Dispose();
        _ring = null;
        LinuxSegmentFile.Close(_fileDescriptor);
        _fileDescriptor = -1;
    }
}
