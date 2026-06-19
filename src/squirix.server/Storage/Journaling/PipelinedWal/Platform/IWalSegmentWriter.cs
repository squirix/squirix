using System;
using System.IO;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling.PipelinedWal.Platform;

/// <summary>Single-writer segment file I/O surface for PipelinedWal.</summary>
internal interface IWalSegmentWriter : IAsyncDisposable
{
    long Length { get; }

    void OpenSegment(string path, bool append);

    void Write(ReadOnlySpan<byte> buffer, long fileOffset);

    void Fsync();

    void Truncate(long length);
}
