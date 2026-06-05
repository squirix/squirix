using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using Squirix.Server.Storage.Journaling.Json;
using Squirix.Server.Storage.JournalProto;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

internal sealed class MappedJournalSegmentReader : IEnumerable<JournalEnvelope>
{
    private readonly CancellationToken _cancellationToken;
    private readonly string _path;
    private readonly bool _tolerateTruncatedTail;

    public MappedJournalSegmentReader(string path, bool tolerateTruncatedTail, CancellationToken cancellationToken)
    {
        _path = path;
        _tolerateTruncatedTail = tolerateTruncatedTail;
        _cancellationToken = cancellationToken;
    }

    public IEnumerator<JournalEnvelope> GetEnumerator() => new Enumerator(_path, _tolerateTruncatedTail, _cancellationToken);

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private sealed class Enumerator : IEnumerator<JournalEnvelope>
    {
        private readonly CancellationToken _cancellationToken;
        private readonly long _length;
        private readonly FileStream? _stream;
        private readonly bool _tolerateTruncatedTail;
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
                    return;
                case < JournalFraming.FileHeaderSize:
                    throw JournalFraming.CreateTruncatedHeaderException(_length);
                default:
                    _stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                    Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
                    if (!StreamEx.TryReadExact(_stream, header))
                        throw JournalFraming.CreateTruncatedHeaderException(_length);

                    JournalFraming.ThrowIfSegmentHeaderInvalid(_length, header);
                    _valid = true;
                    _offset = JournalFraming.FileHeaderSize;
                    return;
            }
        }

        public JournalEnvelope Current { get; private set; } = new();

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

            var read = JournalFrameReader.ReadNext(_stream, _offset, out var rentedBuffer, out var payloadLength);
            if (read.Status == JournalFrameReadStatus.EndOfFile)
                return false;

            if (read.Status != JournalFrameReadStatus.Success)
            {
                if (rentedBuffer is not null)
                    ArrayPool<byte>.Shared.Return(rentedBuffer);

                return read.Status is JournalFrameReadStatus.ChecksumMismatch or JournalFrameReadStatus.OversizedFrame || !_tolerateTruncatedTail
                    ? throw new InvalidDataException($"journal segment corruption at offset {_offset}: {read.Status}.")
                    : Stop();
            }

            try
            {
                ArgumentNullException.ThrowIfNull(rentedBuffer);
                Current = RecordCodec.Deserialize(rentedBuffer.AsSpan(0, payloadLength));
                _offset = read.NextFrameOffset;
                return true;
            }
            catch (JsonException ex)
            {
                throw new InvalidDataException($"journal segment JSON corruption at offset {_offset}.", ex);
            }
            finally
            {
                if (rentedBuffer is not null)
                    ArrayPool<byte>.Shared.Return(rentedBuffer);
            }
        }

        public void Reset() => throw new NotSupportedException();

        private bool Stop()
        {
            _valid = false;
            return false;
        }
    }
}
