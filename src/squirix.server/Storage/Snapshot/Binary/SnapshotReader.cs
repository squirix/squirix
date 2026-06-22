using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Entries.Binary;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot.Binary;

internal sealed class SnapshotReader : ISnapshotReader
{
    private const int InitialRecordScratchSize = 4096;

    public Task<SnapshotLoadResult<T>> LoadStrictAsync<T>(string path, bool skipExpired = true, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var entries = new List<(CacheKey Key, CacheEntry<T> Entry)>(1024);
        var idempotencyRecords = new List<PersistedIdempotencyRecord>();
        foreach (var record in new SnapshotRecordEnumerable(path, true, cancellationToken))
        {
            switch (record)
            {
                case EntryRecord entry:
                    if (skipExpired && IsExpired(entry.Entry))
                        continue;

                    if (!CacheEntryCodec.TryMapEntry<T>(entry.Entry, out var mapped) || mapped is null)
                        throw new InvalidDataException("Binary snapshot entry payload could not be read.");

                    entries.Add((entry.Key, mapped));
                    break;

                case IdempotencyRecord idempotency:
                    idempotencyRecords.Add(idempotency.Record);
                    break;
            }
        }

        return Task.FromResult(new SnapshotLoadResult<T>(entries, idempotencyRecords));
    }

    private static bool IsExpired(CacheEntry<object?> entry) => entry.ExpiresUtc is { } expiresUtc && expiresUtc.ToUniversalTime() <= DateTime.UtcNow;

    private sealed record EntryRecord(CacheKey Key, CacheEntry<object?> Entry);

    private sealed record IdempotencyRecord(PersistedIdempotencyRecord Record);

    private sealed class SnapshotRecordEnumerable : IEnumerable<object>
    {
        private readonly CancellationToken _cancellationToken;
        private readonly string _path;
        private readonly bool _strict;

        public SnapshotRecordEnumerable(string path, bool strict, CancellationToken cancellationToken)
        {
            _path = path;
            _strict = strict;
            _cancellationToken = cancellationToken;
        }

        public IEnumerator<object> GetEnumerator() => new Enumerator(_path, _strict, _cancellationToken);

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        private sealed class Enumerator : IEnumerator<object>
        {
            private readonly CancellationToken _cancellationToken;
            private readonly long _footerOffset;
            private readonly bool _strict;
            private readonly FileStream _stream;
            private object? _current;
            private bool _disposed;
            private bool _footerValidated;
            private uint _crc;
            private byte[] _scratch = new byte[InitialRecordScratchSize];

            public Enumerator(string path, bool strict, CancellationToken cancellationToken)
            {
                _strict = strict;
                _cancellationToken = cancellationToken;
                _stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
                if (_stream.Length < Codec.FileHeaderSize + Codec.FileFooterSize)
                    throw new InvalidDataException("Binary snapshot file is truncated.");

                Span<byte> header = stackalloc byte[Codec.FileHeaderSize];
                if (!StreamEx.TryReadExact(_stream, header))
                    throw new EndOfStreamException("Binary snapshot file header is truncated.");

                Codec.ValidateFileHeader(header);
                _crc = Crc32C.Append(Crc32C.InitialValue, [Codec.Version]);
                _footerOffset = _stream.Length - Codec.FileFooterSize;
            }

            public object Current => _current ?? throw new InvalidOperationException("Enumerator is not positioned on a valid record.");

            object IEnumerator.Current => Current;

            public bool MoveNext()
            {
                while (true)
                {
                    ObjectDisposedException.ThrowIf(_disposed, this);
                    if (_footerValidated)
                        return false;

                    _cancellationToken.ThrowIfCancellationRequested();
                    if (_stream.Position >= _footerOffset)
                        return ValidateFooterAndStop();

                    if (!TryReadNextRecord(out var record))
                        return false;

                    if (record is null)
                        continue;

                    _current = record;
                    return true;
                }
            }

            public void Reset() => throw new NotSupportedException();

            public void Dispose()
            {
                if (_disposed)
                    return;

                _stream.Dispose();
                _disposed = true;
            }

            private bool ValidateFooterAndStop()
            {
                if (_stream.Position != _footerOffset)
                {
                    if (_strict)
                        throw new InvalidDataException("Binary snapshot file footer is misaligned.");

                    _footerValidated = true;
                    return false;
                }

                Span<byte> footer = stackalloc byte[Codec.FileFooterSize];
                if (!StreamEx.TryReadExact(_stream, footer))
                    throw new EndOfStreamException("Binary snapshot file footer is truncated.");

                Codec.ValidateFileFooter(footer, Crc32C.Finalize(_crc));
                _footerValidated = true;
                return false;
            }

            private bool TryReadNextRecord(out object? record)
            {
                record = null;
                if (_stream.Position >= _footerOffset)
                    return false;

                Span<byte> recordHeader = stackalloc byte[Codec.RecordHeaderSize];
                if (!StreamEx.TryReadExact(_stream, recordHeader))
                {
                    if (_strict)
                        throw new EndOfStreamException("Binary snapshot record header is truncated.");

                    return false;
                }

                var bodyLength = BinaryPrimitives.ReadUInt32LittleEndian(recordHeader[1..]);
                var recordLength = Codec.ComputeRecordLength(int.CreateChecked(bodyLength));
                if (_scratch.Length < recordLength)
                    _scratch = new byte[recordLength];

                recordHeader.CopyTo(_scratch);
                if (!StreamEx.TryReadExact(_stream, _scratch.AsSpan(Codec.RecordHeaderSize, recordLength - Codec.RecordHeaderSize)))
                {
                    if (_strict)
                        throw new EndOfStreamException("Binary snapshot record body is truncated.");

                    return false;
                }

                var recordBytes = _scratch.AsSpan(0, recordLength);
                if (!Codec.TryReadRecord(recordBytes, out var kind, out var body, out _))
                {
                    if (_strict)
                        throw new InvalidDataException("Binary snapshot record is truncated.");

                    return false;
                }

                _crc = Crc32C.Append(_crc, recordBytes);
                record = MapRecord(kind, body);
                return true;
            }

            private object? MapRecord(Codec.RecordKind kind, ReadOnlySpan<byte> body)
            {
                switch (kind)
                {
                    case Codec.RecordKind.Entry:
                        if (Codec.TryReadEntryBody(body, out var key, out var entry) && entry is not null)
                            return new EntryRecord(key, entry);
                        if (_strict)
                            throw new InvalidDataException("Binary snapshot entry body could not be read.");

                        return null;

                    case Codec.RecordKind.Idempotency:
                        return TryReadIdempotency(body, out var idempotencyRecord) && idempotencyRecord is not null ? new IdempotencyRecord(idempotencyRecord) : null;

                    default:
                        if (_strict)
                            throw new InvalidDataException($"Unsupported binary snapshot record kind: {kind}.");

                        return null;
                }
            }

            private bool TryReadIdempotency(ReadOnlySpan<byte> body, out PersistedIdempotencyRecord? record)
            {
                try
                {
                    record = IdempotencyCodec.Read(body);
                    return true;
                }
                catch (InvalidDataException) when (!_strict)
                {
                    record = null;
                    return false;
                }
            }
        }
    }
}
