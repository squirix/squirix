using System;
using System.IO;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Read;

/// <summary>Journal segment enumeration and replay.</summary>
internal static class JournalReadPath
{
    internal static string BuildSegmentPath(string dataDir, int segmentIndex) => JournalPaths.BuildSegmentPath(dataDir, segmentIndex);

    internal static string BuildRollTempPath(string dataDir, int segmentIndex) => JournalPaths.BuildRollTempPath(dataDir, segmentIndex);

    internal static JournalSegment[] EnumerateSegments(string dataDir, int fromSegment) => JournalReader.EnumerateSegments(dataDir, fromSegment);

    internal static IJournalRecordEnumerator ReadAll(string dataDir, int fromSegment, CancellationToken cancellationToken) =>
        new JournalReplaySequence(dataDir, fromSegment, cancellationToken).CreateEnumerator();

    /// <summary>
    /// Walks the same segments and frames as <see cref="ReadAll" /> with the same checksum and failure handling, but yields only each frame's
    /// sequence, without building records. The frame prefix is validated as decoding does; operation payload contents are not parsed.
    /// </summary>
    /// <param name="dataDir">The persistence directory.</param>
    /// <param name="fromSegment">The first segment index to read.</param>
    /// <param name="cancellationToken">Cancels the walk.</param>
    /// <returns>The sequence enumerator; the caller disposes it.</returns>
    internal static IJournalSequenceEnumerator ReadSequences(string dataDir, int fromSegment, CancellationToken cancellationToken) =>
        new JournalReplaySequence(dataDir, fromSegment, cancellationToken).CreateSequenceEnumerator();

    /// <summary>Journal segment replay factory without <see cref="System.Collections.Generic.IEnumerable{T}" />.</summary>
    [Immutable]
    private sealed class JournalReplaySequence
    {
        private readonly CancellationToken _cancellationToken;
        private readonly JournalSegment[] _segments;

        internal JournalReplaySequence(string dataDir, int fromSegment, CancellationToken cancellationToken)
        {
            _segments = EnumerateSegments(dataDir, fromSegment);
            _cancellationToken = cancellationToken;
        }

        internal IJournalRecordEnumerator CreateEnumerator() => new JournalReplayEnumerator(_segments, false, _cancellationToken);

        internal IJournalSequenceEnumerator CreateSequenceEnumerator() => new JournalReplayEnumerator(_segments, true, _cancellationToken);

        private sealed class BinaryJournalSegmentEnumerator : IDisposable
        {
            private readonly CancellationToken _cancellationToken;
            private readonly SafeFileHandle? _handle;
            private readonly long _length;
            private readonly JournalFrameWindowReader? _reader;
            private readonly bool _sequenceOnly;
            private readonly bool _tolerateTruncatedTail;
            private JournalRecord? _current;
            private ulong _currentSequence;
            private int _disposed;
            private long _offset;
            private bool _valid;

            internal BinaryJournalSegmentEnumerator(string path, bool tolerateTruncatedTail, bool sequenceOnly, CancellationToken cancellationToken)
            {
                _tolerateTruncatedTail = tolerateTruncatedTail;
                _sequenceOnly = sequenceOnly;
                _cancellationToken = cancellationToken;
                _length = new FileInfo(path).Length;
                switch (_length)
                {
                    case 0:
                        return;
                    case < JournalFraming.FileHeaderSize:
                        throw JournalFraming.CreateTruncatedHeaderException();
                    default:
                        _handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.SequentialScan);
                        try
                        {
                            JournalFraming.ReadAndValidateSegmentHeader(_handle, _offset);
                        }
                        catch
                        {
                            // A throwing constructor hands the caller nothing to dispose, so the handle is released here.
                            _handle.Dispose();
                            throw;
                        }

                        _reader = new JournalFrameWindowReader(_handle, _length);
                        _valid = true;
                        _offset = JournalFraming.FileHeaderSize;
                        return;
                }
            }

            internal JournalRecord CurrentRecord => ThrowHelper.Required(_current, "Enumerator is not positioned on a valid record.");

            internal ulong CurrentSequence => _currentSequence;

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) != 0)
                    return;

                _reader?.Dispose();
                _handle?.Dispose();
            }

            internal bool MoveNext()
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
                if (!_valid || _reader == null)
                    return false;

                _cancellationToken.ThrowIfCancellationRequested();
                return _offset < _length && MoveNextFrame();
            }

            private bool MoveNextFrame()
            {
                // The payload buffer is owned by the reader and the decoded record aliases it until the next read.
                var read = _reader!.ReadNext(_offset, out var buffer, out var payloadLength);
                if (read.Status is JournalFrameReadStatus.EndOfFile)
                    return false;

                if (read.Status != JournalFrameReadStatus.Success)
                    return ShouldThrowOnReadFailure(read.Status) ? throw new InvalidDataException("journal segment corruption.") : Stop();

                var frameBuffer = buffer ?? ThrowHelper.Throw<byte[]>(new InvalidDataException("journal segment missing payload buffer."));
                if (_sequenceOnly)
                    _currentSequence = BinaryJournalCodec.ReadSequence(frameBuffer, payloadLength);
                else
                    _current = BinaryJournalCodec.Decode(frameBuffer, payloadLength);

                _offset = read.NextFrameOffset;
                return true;
            }

            private bool ShouldThrowOnReadFailure(JournalFrameReadStatus status) =>
                !_tolerateTruncatedTail || status == JournalFrameReadStatus.ChecksumMismatch || status == JournalFrameReadStatus.OversizedFrame;

            private bool Stop()
            {
                _valid = false;
                return false;
            }
        }

        /// <summary>Lists journal records across segment files.</summary>
        private sealed class JournalReplayEnumerator : IJournalRecordEnumerator, IJournalSequenceEnumerator
        {
            private readonly CancellationToken _cancellationToken;
            private readonly JournalSegment[] _segments;
            private readonly bool _sequenceOnly;
            private int _disposed;
            private BinaryJournalSegmentEnumerator? _segmentEnumerator;
            private int _segmentIndex;

            internal JournalReplayEnumerator(JournalSegment[] segments, bool sequenceOnly, CancellationToken cancellationToken)
            {
                _segments = segments;
                _sequenceOnly = sequenceOnly;
                _cancellationToken = cancellationToken;
                _segmentIndex = -1;
                _segmentEnumerator = null;
            }

            JournalRecord IJournalRecordEnumerator.Current => PositionedSegment().CurrentRecord;

            ulong IJournalSequenceEnumerator.Current => PositionedSegment().CurrentSequence;

            void IDisposable.Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) != 0)
                    return;

                DisposeSegmentEnumerator();
            }

            bool IJournalRecordEnumerator.MoveNext() => MoveNext();

            bool IJournalSequenceEnumerator.MoveNext() => MoveNext();

            private static BinaryJournalSegmentEnumerator CreateSegmentEnumerator(string path, bool tolerateTruncatedTail, bool sequenceOnly, CancellationToken cancellationToken) =>
                new(path, tolerateTruncatedTail, sequenceOnly, cancellationToken);

            private static InvalidDataException CreateSegmentReadFailure(string path, bool tolerateTruncatedTail, Exception ex) => new(
                $"failed reading journal segment '{path}' (tolerateTruncatedTail={tolerateTruncatedTail}): {ex.Message}",
                ex);

            private BinaryJournalSegmentEnumerator PositionedSegment()
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
                return ThrowHelper.Required(_segmentEnumerator, "Enumerator is not positioned on a valid record.");
            }

            private bool MoveNext()
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
                if (TryMoveCurrentSegment())
                    return true;

                while (OpenNextSegment())
                {
                    if (TryMoveCurrentSegment())
                        return true;
                }

                return false;
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
                    throw CreateSegmentReadFailure(segment.Path, tolerateTruncatedTail, ex);
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
                    _segmentEnumerator = CreateSegmentEnumerator(segment.Path, tolerateTruncatedTail, _sequenceOnly, _cancellationToken);
                    return true;
                }
                catch (Exception ex) when (ex is InvalidDataException or IOException)
                {
                    throw CreateSegmentReadFailure(segment.Path, tolerateTruncatedTail, ex);
                }
            }

            private bool TryMoveCurrentSegment()
            {
                if (_segmentEnumerator == null)
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
}
