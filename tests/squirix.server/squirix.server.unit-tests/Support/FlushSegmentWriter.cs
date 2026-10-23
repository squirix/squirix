using System;
using System.IO;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Segment writer double that accepts every write and either flushes successfully or fails every flush with a fixed failure.</summary>
[Immutable]
internal sealed class FlushSegmentWriter : IJournalSegmentWriter
{
    private readonly IOException? _flushFailure;

    /// <summary>Initializes a new instance of the <see cref="FlushSegmentWriter" /> class.</summary>
    /// <param name="flushFailure">Failure every flush throws, or <see langword="null" /> when flushes succeed.</param>
    internal FlushSegmentWriter(IOException? flushFailure = null)
    {
        _flushFailure = flushFailure;
    }

    long IJournalSegmentWriter.Length => 0;

    /// <summary>Releases test resources.</summary>
    public void Dispose()
    {
    }

    void IJournalSegmentWriter.FlushToDisk()
    {
        if (_flushFailure != null)
            throw _flushFailure;
    }

    void IJournalSegmentWriter.OpenSegment(string path, bool append)
    {
    }

    void IJournalSegmentWriter.Truncate(long length)
    {
    }

    void IJournalSegmentWriter.Write(ReadOnlySpan<byte> buffer, long fileOffset)
    {
    }
}
