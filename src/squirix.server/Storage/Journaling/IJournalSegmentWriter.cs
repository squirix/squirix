using System;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Single-writer segment file I/O surface for Pipelined.</summary>
internal interface IJournalSegmentWriter : IDisposable
{
    long Length { get; }

    void Fsync();

    void OpenSegment(string path, bool append);

    void Truncate(long length);

    void Write(ReadOnlySpan<byte> buffer, long fileOffset);
}
