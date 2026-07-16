using System;
using System.IO;
using System.Threading;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Storage.Journaling.Read;

/// <summary>Pattern-based replay over journal segments without <see cref="System.Collections.Generic.IEnumerable{T}" />.</summary>
internal sealed record JournalReplaySequence
{
    private readonly CancellationToken _cancellationToken;
    private readonly JournalSegment[] _segments;

    public JournalReplaySequence(string dataDir, int fromSegment, CancellationToken cancellationToken)
    {
        _segments = JournalReadPath.EnumerateSegments(dataDir, fromSegment);
        _cancellationToken = cancellationToken;
    }

    public IJournalRecordEnumerator GetEnumerator() => new JournalReplayEnumerator(_segments, _cancellationToken);

    /// <summary>Enumerates journal records across segment files.</summary>
    private sealed class JournalReplayEnumerator : IJournalRecordEnumerator
    {
        private readonly CancellationToken _cancellationToken;
        private readonly JournalSegment[] _segments;
        private IJournalRecordEnumerator? _segmentEnumerator;
        private int _segmentIndex;

        public JournalReplayEnumerator(JournalSegment[] segments, CancellationToken cancellationToken)
        {
            _segments = segments;
            _cancellationToken = cancellationToken;
            _segmentIndex = -1;
            _segmentEnumerator = null;
        }

        JournalRecord IJournalRecordEnumerator.Current => _segmentEnumerator!.Current;

        bool IJournalRecordEnumerator.MoveNext()
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
    }
}
