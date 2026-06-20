using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;

namespace Squirix.Server.Storage.Journaling.Pipelined.Platform;

/// <summary><see cref="RandomAccess"/>-based segment writer with write-through on Windows.</summary>
internal sealed class RandomAccessJournalSegmentWriter : IJournalSegmentWriter
{
    private SafeFileHandle? _handle;

    public long Length
    {
        get
        {
            var handle = _handle ?? throw new InvalidOperationException("segment is not open.");
            return RandomAccess.GetLength(handle);
        }
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

    public void Write(ReadOnlySpan<byte> buffer, long fileOffset)
    {
        var handle = _handle ?? throw new InvalidOperationException("segment is not open.");
        RandomAccess.Write(handle, buffer, fileOffset);
    }

    public void Fsync()
    {
        var handle = _handle ?? throw new InvalidOperationException("segment is not open.");
        RandomAccess.FlushToDisk(handle);
    }

    public void Truncate(long length)
    {
        var handle = _handle ?? throw new InvalidOperationException("segment is not open.");
        RandomAccess.SetLength(handle, length);
    }

    public ValueTask DisposeAsync()
    {
        _handle?.Dispose();
        _handle = null;
        return ValueTask.CompletedTask;
    }
}
