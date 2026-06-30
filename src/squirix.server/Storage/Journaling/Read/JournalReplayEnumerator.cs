using System;
using System.IO;
using System.Threading;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Storage.Journaling.Read;

/// <summary>Enumerates journal records across segment files.</summary>
internal sealed class JournalReplayEnumerator : IDisposable
{
    private readonly JournalSegment[] _segments;
    private readonly CancellationToken _cancellationToken;
    private int _segmentIndex;
    private BinaryJournalSegmentReader.Enumerator? _segmentEnumerator;

    internal JournalReplayEnumerator(JournalSegment[] segments, CancellationToken cancellationToken)
    {
        _segments = segments;
        _cancellationToken = cancellationToken;
        _segmentIndex = -1;
        _segmentEnumerator = null;
    }

    public JournalRecord Current => _segmentEnumerator!.Current;

    public bool MoveNext()
    {
        if (TryMoveCurrentSegment())
            return true;

        while (OpenNextSegment())
        {
            if (TryMoveCurrentSegment())
                return true;
        }

        return false;
    }

    public void Dispose() => DisposeSegmentEnumerator();

    private bool TryMoveCurrentSegment()
    {
        if (_segmentEnumerator is null)
            return false;

        bool segmentHasNext;
        try
        {
            segmentHasNext = MoveNextSegmentRecord();
        }
        catch
        {
            DisposeSegmentEnumerator();
            throw;
        }

        if (segmentHasNext)
            return true;

        DisposeSegmentEnumerator();
        return false;
    }

    private bool OpenNextSegment()
    {
        _segmentIndex++;
        if (_segmentIndex >= _segments.Length)
            return false;

        var segment = _segments[_segmentIndex];
        var tolerateTruncatedTail = _segmentIndex == _segments.Length - 1;
        try
        {
            _segmentEnumerator = new BinaryJournalSegmentReader.Enumerator(segment.Path, tolerateTruncatedTail, _cancellationToken);
            return true;
        }
        catch (Exception ex) when (ex is InvalidDataException or IOException)
        {
            throw JournalReadPath.CreateSegmentReadFailure(segment.Path, tolerateTruncatedTail, ex);
        }
    }

    private void DisposeSegmentEnumerator()
    {
        var enumerator = _segmentEnumerator;
        _segmentEnumerator = null;
        enumerator?.Dispose();
    }

    private bool MoveNextSegmentRecord()
    {
        var segment = _segments[_segmentIndex];
        var tolerateTruncatedTail = _segmentIndex == _segments.Length - 1;
        try
        {
            return _segmentEnumerator!.MoveNext();
        }
        catch (Exception ex) when (ex is InvalidDataException or IOException)
        {
            throw JournalReadPath.CreateSegmentReadFailure(segment.Path, tolerateTruncatedTail, ex);
        }
    }
}
