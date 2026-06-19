using System;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling.PipelinedWal.Platform;

/// <summary>
/// Linux io_uring segment writer. Falls back to <see cref="RandomAccessWalSegmentWriter"/> until native io_uring is wired.
/// </summary>
internal sealed class UringWalSegmentWriter : IWalSegmentWriter
{
    private readonly RandomAccessWalSegmentWriter _fallback = new();

    public long Length => _fallback.Length;

    public void OpenSegment(string path, bool append) => _fallback.OpenSegment(path, append);

    public void Write(ReadOnlySpan<byte> buffer, long fileOffset) => _fallback.Write(buffer, fileOffset);

    public void Fsync() => _fallback.Fsync();

    public void Truncate(long length) => _fallback.Truncate(length);

    public ValueTask DisposeAsync() => _fallback.DisposeAsync();
}
