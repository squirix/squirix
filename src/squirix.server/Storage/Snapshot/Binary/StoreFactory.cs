using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage.Entries.Binary;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot.Binary;

internal static class StoreFactory
{
    internal static ISnapshotReader CreateReader(PersistenceOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new SnapshotReader();
    }

    internal static ISnapshotWriter CreateWriter(PersistenceOptions options, IStorageFileOperations? fileOperations = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        var fileOps = fileOperations ?? new FileOperations();
        return new SnapshotWriter(options.DataDir, fileOps);
    }

    private sealed class SnapshotReader : ISnapshotReader
    {
        private const int InitialRecordScratchSize = 4096;

        public Task<LoadResult<T>> LoadStrictAsync<T>(string path, bool skipExpired = true, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var entries = new List<(CacheKey Key, NodeCacheEntry<T> Entry)>(1024);
            var idempotencyRecords = new List<PersistedIdempotencyRecord>(16);
            using (var enumerator = new SnapshotRecordEnumerator(path, true, cancellationToken))
            {
                while (enumerator.MoveNext())
                {
                    switch (enumerator.Current)
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
            }

            return Task.FromResult(new LoadResult<T>(entries, idempotencyRecords));
        }

        private static bool IsExpired(NodeCacheEntry<object?> entry) => entry.ExpiresUtc is { } expiresUtc && expiresUtc.ToUniversalTime() <= DateTime.UtcNow;

        private sealed record EntryRecord(CacheKey Key, NodeCacheEntry<object?> Entry);

        private sealed record IdempotencyRecord(PersistedIdempotencyRecord Record);

        private sealed class SnapshotRecordEnumerator : IEnumerator<object>
        {
            private readonly CancellationToken _cancellationToken;
            private readonly long _footerOffset;
            private readonly FileStream _stream;
            private readonly bool _strict;
            private uint _crc;
            private object? _current;
            private bool _disposed;
            private bool _footerValidated;
            private byte[] _scratch = new byte[InitialRecordScratchSize];

            internal SnapshotRecordEnumerator(string path, bool strict, CancellationToken cancellationToken)
            {
                _strict = strict;
                _cancellationToken = cancellationToken;
                _stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
                if (_stream.Length < SnapshotCodec.FileHeaderSize + SnapshotCodec.FileFooterSize)
                    throw new InvalidDataException("Binary snapshot file is truncated.");

                Span<byte> header = stackalloc byte[SnapshotCodec.FileHeaderSize];
                if (!StreamEx.TryReadExact(_stream, header))
                    throw new EndOfStreamException("Binary snapshot file header is truncated.");

                SnapshotCodec.ValidateFileHeader(header);
                _crc = Crc32C.Append(Crc32C.InitialValue, SnapshotCodec.Version);
                _footerOffset = _stream.Length - SnapshotCodec.FileFooterSize;
            }

            public object Current => _current ?? throw new InvalidOperationException("Enumerator is not positioned on a valid record.");

            public void Dispose()
            {
                if (_disposed)
                    return;

                _stream.Dispose();
                _disposed = true;
            }

            public bool MoveNext()
            {
                while (true)
                {
                    ObjectDisposedException.ThrowIf(_disposed, this);
                    if (_footerValidated)
                        return false;

                    _cancellationToken.ThrowIfCancellationRequested();
                    if (_stream.Position >= _footerOffset)
                    {
                        ValidateFooter();
                        return false;
                    }

                    if (!TryReadNextRecord(out var record))
                        return false;

                    if (record is null)
                        continue;

                    _current = record;
                    return true;
                }
            }

            public void Reset() => throw new NotSupportedException();

            private object? MapRecord(RecordKind kind, ReadOnlySpan<byte> body)
            {
                switch (kind)
                {
                    case RecordKind.Entry:
                        if (SnapshotCodec.TryReadEntryBody(body, out var key, out var entry) && entry is not null)
                            return new EntryRecord(key, entry);
                        if (_strict)
                            throw new InvalidDataException("Binary snapshot entry body could not be read.");

                        return null;

                    case RecordKind.Idempotency:
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

            private bool TryReadNextRecord(out object? record)
            {
                record = null;
                if (_stream.Position >= _footerOffset)
                    return false;

                if (!TryReadRecordBytes(out var recordBytes))
                    return false;

                if (!SnapshotCodec.TryReadRecord(recordBytes, out var kind, out var body))
                {
                    if (_strict)
                        throw new InvalidDataException("Binary snapshot record is truncated.");

                    return false;
                }

                _crc = Crc32C.Append(_crc, recordBytes);
                record = MapRecord(kind, body);
                return true;
            }

            private bool TryReadRecordBytes(out ReadOnlySpan<byte> recordBytes)
            {
                recordBytes = default;
                Span<byte> recordHeader = stackalloc byte[SnapshotCodec.RecordHeaderSize];
                if (!StreamEx.TryReadExact(_stream, recordHeader))
                {
                    if (_strict)
                        throw new EndOfStreamException("Binary snapshot record header is truncated.");

                    return false;
                }

                var bodyLength = BinaryPrimitives.ReadUInt32LittleEndian(recordHeader[1..]);
                var recordLength = SnapshotCodec.ComputeRecordLength(int.CreateChecked(bodyLength));
                if (_scratch.Length < recordLength)
                    _scratch = new byte[recordLength];

                recordHeader.CopyTo(_scratch);
                if (!StreamEx.TryReadExact(_stream, _scratch.AsSpan(SnapshotCodec.RecordHeaderSize, recordLength - SnapshotCodec.RecordHeaderSize)))
                {
                    if (_strict)
                        throw new EndOfStreamException("Binary snapshot record body is truncated.");

                    return false;
                }

                recordBytes = _scratch.AsSpan(0, recordLength);
                return true;
            }

            private void ValidateFooter()
            {
                if (_stream.Position != _footerOffset)
                {
                    if (_strict)
                        throw new InvalidDataException("Binary snapshot file footer is misaligned.");

                    _footerValidated = true;
                    return;
                }

                Span<byte> footer = stackalloc byte[SnapshotCodec.FileFooterSize];
                if (!StreamEx.TryReadExact(_stream, footer))
                    throw new EndOfStreamException("Binary snapshot file footer is truncated.");

                SnapshotCodec.ValidateFileFooter(footer, Crc32C.Finalize(_crc));
                _footerValidated = true;
            }
        }
    }
}
