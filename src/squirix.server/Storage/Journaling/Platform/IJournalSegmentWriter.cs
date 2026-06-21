using System;

namespace Squirix.Server.Storage.Journaling.Platform;

/// <summary>Single-writer segment file I/O surface for Pipelined.</summary>
internal interface IJournalSegmentWriter : IAsyncDisposable
{
    long Length { get; }

    void OpenSegment(string path, bool append);

    void Write(ReadOnlySpan<byte> buffer, long fileOffset);

    void Fsync();

    void Truncate(long length);
}
