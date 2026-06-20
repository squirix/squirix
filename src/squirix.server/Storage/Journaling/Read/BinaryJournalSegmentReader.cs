using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.JsonFramed;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Read;

/// <summary>Reads <see cref="JournalRecord" /> from a segment, auto-detecting JsonFramed JSON vs Pipelined binary frame bodies.</summary>
internal sealed class BinaryJournalSegmentReader : IJournalSegmentReader
{
    private readonly CancellationToken _cancellationToken;

    public BinaryJournalSegmentReader(string path, bool tolerateTruncatedTail, CancellationToken cancellationToken)
    {
        Path = path;
        TolerateTruncatedTail = tolerateTruncatedTail;
        _cancellationToken = cancellationToken;
    }

    public string Path { get; }

    public bool TolerateTruncatedTail { get; }

    public IEnumerator<JournalRecord> GetEnumerator() => new Enumerator(Path, TolerateTruncatedTail, _cancellationToken);

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private sealed class Enumerator : IEnumerator<JournalRecord>
    {
        private readonly CancellationToken _cancellationToken;
        private readonly IJournalFrameCodec _codec;
        private readonly long _length;
        private readonly FileStream? _stream;
        private readonly bool _tolerateTruncatedTail;
        private JournalRecord? _current;
        private bool _disposed;
        private long _offset;
        private bool _valid;

        public Enumerator(string path, bool tolerateTruncatedTail, CancellationToken cancellationToken)
        {
            _tolerateTruncatedTail = tolerateTruncatedTail;
            _cancellationToken = cancellationToken;
            _length = new FileInfo(path).Length;
            switch (_length)
            {
                case 0:
                    _codec = JournalFrameCodecFactory.JsonFramed;
                    return;
                case < JournalFraming.FileHeaderSize:
                    throw JournalFraming.CreateTruncatedHeaderException(_length);
                default:
                    _stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                    Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
                    if (!StreamEx.TryReadExact(_stream, header))
                        throw JournalFraming.CreateTruncatedHeaderException(_length);

                    _codec = JournalFrameCodecFactory.DetectFromSegmentStart(header, _stream, _length);
                    _valid = true;
                    _offset = JournalFraming.FileHeaderSize;
                    return;
            }
        }

        public JournalRecord Current => _current ?? throw new InvalidOperationException("Enumerator is not positioned on a valid record.");

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            if (_disposed)
                return;

            _stream?.Dispose();
            _disposed = true;
        }

        public bool MoveNext()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (!_valid || _stream is null)
                return false;

            _cancellationToken.ThrowIfCancellationRequested();
            if (_offset >= _length)
                return false;

            return MoveNextFrame();
        }

        public void Reset() => throw new NotSupportedException();

        private bool MoveNextFrame()
        {
            var read = JournalFrameReader.ReadNext(_stream!, _offset, out var buffer, out var payloadLength);
            if (read.Status is JournalFrameReadStatus.EndOfFile)
                return false;

            if (read.Status is not JournalFrameReadStatus.Success)
            {
                if (buffer is not null)
                    ArrayPool<byte>.Shared.Return(buffer);

                if (ShouldThrowOnReadFailure(read.Status))
                    throw new InvalidDataException($"journal segment corruption at offset {_offset.ToString(CultureInfo.InvariantCulture)}: {read.Status}.");

                return Stop();
            }

            try
            {
                if (buffer is null)
                    throw new InvalidDataException($"journal segment missing payload buffer at offset {_offset.ToString(CultureInfo.InvariantCulture)}.");

                _current = _codec.Decode(buffer.AsSpan(0, payloadLength));
                _offset = read.NextFrameOffset;
                return true;
            }
            finally
            {
                if (buffer is not null)
                    ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        private bool ShouldThrowOnReadFailure(JournalFrameReadStatus status) =>
            !_tolerateTruncatedTail || status is JournalFrameReadStatus.ChecksumMismatch or JournalFrameReadStatus.OversizedFrame;

        private bool Stop()
        {
            _valid = false;
            return false;
        }
    }
}
