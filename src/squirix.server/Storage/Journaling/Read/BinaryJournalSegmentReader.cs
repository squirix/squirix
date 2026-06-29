using System;
using System.Buffers;
using System.Globalization;
using System.IO;
using System.Threading;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Framing;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Read;

/// <summary>Reads <see cref="JournalRecord" /> from a binary journal segment.</summary>
internal static class BinaryJournalSegmentReader
{
    public sealed class Enumerator : IDisposable
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

        public Enumerator(string path, bool tolerateTruncatedTail, CancellationToken cancellationToken)
        {
            _tolerateTruncatedTail = tolerateTruncatedTail;
            _cancellationToken = cancellationToken;
            _length = new FileInfo(path).Length;
            switch (_length)
            {
                case 0:
                    return;
                case < JournalFraming.FileHeaderSize:
                    throw JournalFraming.CreateTruncatedHeaderException(_length);
                default:
                    _stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                    Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
                    if (!StreamEx.TryReadExact(_stream, header))
                        throw JournalFraming.CreateTruncatedHeaderException(_length);

                    JournalFraming.EnsureSegmentHeaderSupported(header);
                    _valid = true;
                    _offset = JournalFraming.FileHeaderSize;
                    return;
            }
        }

        public JournalRecord Current => _current ?? throw new InvalidOperationException("Enumerator is not positioned on a valid record.");

        public void Dispose()
        {
            if (_disposed)
                return;

            ReturnRentedFrameBuffer();
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
                    throw new InvalidDataException($"journal segment corruption at offset {_offset.ToString(CultureInfo.InvariantCulture)}: {read.Status}.");

                return Stop();
            }

            _rentedFrameBuffer = buffer ?? throw new InvalidDataException($"journal segment missing payload buffer at offset {_offset.ToString(CultureInfo.InvariantCulture)}.");
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
}
