using System;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Records every segment I/O call in a <see cref="JournalStallProbe" /> so a stalled journal thread is visible while it is blocked.</summary>
internal sealed class ProbedJournalSegmentWriter : IJournalSegmentWriter
{
    private readonly IJournalSegmentWriter _inner;
    private readonly JournalStallProbe _probe;

    internal ProbedJournalSegmentWriter(IJournalSegmentWriter inner, JournalStallProbe probe)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(probe);
        _inner = inner;
        _probe = probe;
    }

    public long Length => _inner.Length;

    public void Dispose() => _inner.Dispose();

    public void FlushToDisk()
    {
        _probe.IoStarted(nameof(FlushToDisk));
        try
        {
            _inner.FlushToDisk();
        }
        finally
        {
            _probe.IoFinished();
        }
    }

    public void OpenSegment(string path, bool append)
    {
        _probe.IoStarted(nameof(OpenSegment));
        try
        {
            _inner.OpenSegment(path, append);
        }
        finally
        {
            _probe.IoFinished();
        }
    }

    public void Truncate(long length)
    {
        _probe.IoStarted(nameof(Truncate));
        try
        {
            _inner.Truncate(length);
        }
        finally
        {
            _probe.IoFinished();
        }
    }

    public void Write(ReadOnlySpan<byte> buffer, long fileOffset)
    {
        _probe.IoStarted(nameof(Write));
        try
        {
            _inner.Write(buffer, fileOffset);
        }
        finally
        {
            _probe.IoFinished();
        }
    }
}
