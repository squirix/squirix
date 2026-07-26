using System;
using System.Buffers;
using System.IO;
using System.Threading;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Read;

/// <summary>Journal segment enumeration and replay.</summary>
internal static class JournalReadPath
{
    internal static string BuildSegmentPath(string dataDir, int segmentIndex) => JournalPaths.BuildSegmentPath(dataDir, segmentIndex);

    internal static JournalSegment[] EnumerateSegments(string dataDir, int fromSegment) => JournalReader.EnumerateSegments(dataDir, fromSegment);

    internal static IJournalRecordEnumerator ReadAll(string dataDir, int fromSegment, CancellationToken cancellationToken) =>
        new JournalReplaySequence(dataDir, fromSegment, cancellationToken).CreateEnumerator();

    /// <summary>Journal segment replay factory without <see cref="System.Collections.Generic.IEnumerable{T}" />.</summary>
    private sealed class JournalReplaySequence
    {
        private readonly CancellationToken _cancellationToken;
        private readonly JournalSegment[] _segments;

        internal JournalReplaySequence(string dataDir, int fromSegment, CancellationToken cancellationToken)
        {
            _segments = EnumerateSegments(dataDir, fromSegment);
            _cancellationToken = cancellationToken;
        }

        internal IJournalRecordEnumerator CreateEnumerator() => new JournalReplayEnumerator(_segments, _cancellationToken);

        private sealed class BinaryJournalSegmentEnumerator : IJournalRecordEnumerator
        {
            private readonly CancellationToken _cancellationToken;
            private readonly long _length;
            private readonly FileStream? _stream;
            private readonly bool _tolerateTruncatedTail;
            private JournalRecord? _current;
            private bool _disposed;
            private long _offset;
            private byte[]? _rentedFrameBuffer;
            private bool _valid;

            internal BinaryJournalSegmentEnumerator(string path, bool tolerateTruncatedTail, CancellationToken cancellationToken)
            {
                _tolerateTruncatedTail = tolerateTruncatedTail;
                _cancellationToken = cancellationToken;
                _length = new FileInfo(path).Length;
                switch (_length)
                {
                    case 0:
                        return;
                    case < JournalFraming.FileHeaderSize:
                        throw JournalFraming.CreateTruncatedHeaderException();
                    default:
                        _stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
                        if (!StreamEx.TryReadExact(_stream, header))
                            throw JournalFraming.CreateTruncatedHeaderException();

                        JournalFraming.EnsureSegmentHeaderSupported(header);
                        _valid = true;
                        _offset = JournalFraming.FileHeaderSize;
                        return;
                }
            }

            JournalRecord IJournalRecordEnumerator.Current => _current ?? throw new InvalidOperationException("Enumerator is not positioned on a valid record.");

            void IDisposable.Dispose()
            {
                if (_disposed)
                    return;

                ReturnRentedFrameBuffer();
                _stream?.Dispose();
                _disposed = true;
            }

            bool IJournalRecordEnumerator.MoveNext()
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                if (!_valid || _stream is null)
                    return false;

                _cancellationToken.ThrowIfCancellationRequested();
                if (_offset >= _length)
                    return false;

                return MoveNextFrame();
            }

            private bool MoveNextFrame()
            {
                ReturnRentedFrameBuffer();

                var read = JournalFrameReader.ReadNext(_stream!, _offset, out var buffer, out var payloadLength);
                if (read.Status is JournalFrameReadStatus.EndOfFile)
                    return false;

                if (read.Status is not JournalFrameReadStatus.Success)
                {
                    if (buffer is not null)
                        ArrayPool<byte>.Shared.Return(buffer);

                    if (ShouldThrowOnReadFailure(read.Status))
                        throw new InvalidDataException("journal segment corruption.");

                    return Stop();
                }

                _rentedFrameBuffer = buffer ?? throw new InvalidDataException("journal segment missing payload buffer.");
                _current = BinaryJournalCodec.Decode(buffer, payloadLength);
                _offset = read.NextFrameOffset;
                return true;
            }

            private void ReturnRentedFrameBuffer()
            {
                if (_rentedFrameBuffer is null)
                    return;

                ArrayPool<byte>.Shared.Return(_rentedFrameBuffer);
                _rentedFrameBuffer = null;
            }

            private bool ShouldThrowOnReadFailure(JournalFrameReadStatus status) =>
                !_tolerateTruncatedTail || status is JournalFrameReadStatus.ChecksumMismatch or JournalFrameReadStatus.OversizedFrame;

            private bool Stop()
            {
                _valid = false;
                return false;
            }
        }

        /// <summary>Enumerates journal records across segment files.</summary>
        private sealed class JournalReplayEnumerator : IJournalRecordEnumerator
        {
            private readonly CancellationToken _cancellationToken;
            private readonly JournalSegment[] _segments;
            private IJournalRecordEnumerator? _segmentEnumerator;
            private int _segmentIndex;

            internal JournalReplayEnumerator(JournalSegment[] segments, CancellationToken cancellationToken)
            {
                _segments = segments;
                _cancellationToken = cancellationToken;
                _segmentIndex = -1;
                _segmentEnumerator = null;
            }

            JournalRecord IJournalRecordEnumerator.Current => _segmentEnumerator!.Current;

            void IDisposable.Dispose() => DisposeSegmentEnumerator();

            bool IJournalRecordEnumerator.MoveNext()
            {
                if (TryMoveCurrentSegment())
                    return true;

                while (OpenNextSegment())
                    if (TryMoveCurrentSegment())
                        return true;

                return false;
            }

            private static BinaryJournalSegmentEnumerator CreateSegmentEnumerator(string path, bool tolerateTruncatedTail, CancellationToken cancellationToken) =>
                new(path, tolerateTruncatedTail, cancellationToken);

            private static InvalidDataException CreateSegmentReadFailure(string path, bool tolerateTruncatedTail, Exception ex) => new(
                $"failed reading journal segment '{path}' (tolerateTruncatedTail={tolerateTruncatedTail}): {ex.Message}",
                ex);

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
                    _segmentEnumerator = CreateSegmentEnumerator(segment.Path, tolerateTruncatedTail, _cancellationToken);
                    return true;
                }
                catch (Exception ex) when (ex is InvalidDataException or IOException)
                {
                    throw CreateSegmentReadFailure(segment.Path, tolerateTruncatedTail, ex);
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
}
