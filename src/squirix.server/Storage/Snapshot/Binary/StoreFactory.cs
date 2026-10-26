using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot.Binary;

internal static class StoreFactory
{
    internal static ISnapshotReader CreateReader() => new SnapshotReader();

    internal static ISnapshotWriter CreateWriter(PersistenceOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new SnapshotWriter(options.DataDir, new FileOperations());
    }

    [Immutable]
    private sealed class SnapshotReader : ISnapshotReader
    {
        private const int InitialRecordScratchSize = 4096;

        public ValueTask<LoadResult<T>> LoadStrictAsync<T>(string path, DateTime? expiredAsOf, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var entries = new List<(CacheKey Key, NodeCacheEntry<T> Entry)>(1024);
            var lengths = new List<int?>(1024);
            var idempotencyRecords = new List<PersistedIdempotencyRecord>(16);
            using (var enumerator = new SnapshotRecordEnumerator(path, cancellationToken))
            {
                while (enumerator.MoveNext())
                    AppendRecord(enumerator.Current, expiredAsOf, entries, lengths, idempotencyRecords);
            }

            return new ValueTask<LoadResult<T>>(new LoadResult<T>(entries, idempotencyRecords, lengths));
        }

        private static void AppendRecord<T>(
            object record,
            DateTime? expiredAsOf,
            List<(CacheKey Key, NodeCacheEntry<T> Entry)> entries,
            List<int?> lengths,
            List<PersistedIdempotencyRecord> records)
        {
            switch (record)
            {
                case EntryRecord entry:
                    if (expiredAsOf is { } utcNow && IsExpired(entry.Entry, utcNow))
                        return;

                    if (!CacheEntryCodec.TryMapEntry<T>(entry.Entry, out var mapped) || mapped == null)
                        throw new InvalidDataException("Binary snapshot entry payload could not be read.");

                    entries.Add((entry.Key, mapped));
                    lengths.Add(entry.EncodedLength);
                    return;

                case IdempotencyRecord idempotency:
                    records.Add(idempotency.Record);
                    return;
                default:
                    throw new InvalidOperationException("Encountered unexpected record.");
            }
        }

        private static bool IsExpired(NodeCacheEntry<object?> entry, DateTime utcNow) => entry.ExpiresUtc is { } expiresUtc && expiresUtc.ToUniversalTime() <= utcNow;

        [Immutable]
        private sealed record EntryRecord(CacheKey Key, NodeCacheEntry<object?> Entry, int? EncodedLength);

        [Immutable]
        private sealed record IdempotencyRecord(PersistedIdempotencyRecord Record);

        private sealed class SnapshotRecordEnumerator : IEnumerator<object>
        {
            private readonly CancellationToken _cancellationToken;
            private readonly long _footerOffset;
            private readonly SafeFileHandle _handle;
            private uint _crc;
            private int _disposed;
            private bool _footerValidated;
            private long _offset;
            private byte[] _scratch = new byte[InitialRecordScratchSize];

            internal SnapshotRecordEnumerator(string path, CancellationToken cancellationToken)
            {
                _cancellationToken = cancellationToken;
                _handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, FileOptions.SequentialScan);
                try
                {
                    if (RandomAccess.GetLength(_handle) < SnapshotCodec.FileHeaderSize + SnapshotCodec.FileFooterSize)
                        throw new InvalidDataException("Binary snapshot file is truncated.");

                    Span<byte> header = stackalloc byte[SnapshotCodec.FileHeaderSize];
                    if (!HandleEx.TryReadExact(_handle, header, ref _offset))
                        throw new EndOfStreamException("Binary snapshot file header is truncated.");

                    SnapshotCodec.ValidateFileHeader(header);
                    _crc = Crc32C.Append(Crc32C.InitialValue, SnapshotCodec.Version);
                    _footerOffset = RandomAccess.GetLength(_handle) - SnapshotCodec.FileFooterSize;
                }
                catch
                {
                    // A throwing constructor hands the caller nothing to dispose, so the handle is released here;
                    // otherwise it stays open until finalization and blocks deleting the rejected file.
                    _handle.Dispose();
                    throw;
                }
            }

            public object Current
            {
                get => ThrowHelper.Required(field, "Enumerator is not positioned on a valid record.");
                private set;
            }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) != 0)
                    return;

                _handle.Dispose();
            }

            public bool MoveNext()
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
                if (_footerValidated)
                    return false;

                _cancellationToken.ThrowIfCancellationRequested();
                if (_offset >= _footerOffset)
                {
                    ValidateFooter();
                    return false;
                }

                Current = ReadNextRecord();
                return true;
            }

            public void Reset() => throw new NotSupportedException();

            private static object MapRecord(RecordKind kind, ReadOnlySpan<byte> body)
            {
                switch (kind)
                {
                    case RecordKind.Entry:
                        if (SnapshotCodec.TryReadEntryBody(body, out var key, out var entry, out var encodedLength) && entry != null)
                            return new EntryRecord(key, entry, encodedLength);

                        throw new InvalidDataException("Binary snapshot entry body could not be read.");

                    case RecordKind.Idempotency:
                        return new IdempotencyRecord(IdempotencyCodec.Read(body));

                    default:
                        throw new InvalidDataException($"Unsupported binary snapshot record kind: {kind}.");
                }
            }

            private object ReadNextRecord()
            {
                var recordBytes = ReadRecordBytes();
                if (!SnapshotCodec.TryReadRecord(recordBytes, out var kind, out var body))
                    throw new InvalidDataException("Binary snapshot record is truncated.");

                _crc = Crc32C.Append(_crc, recordBytes);
                return MapRecord(kind, body);
            }

            private ReadOnlySpan<byte> ReadRecordBytes()
            {
                Span<byte> recordHeader = stackalloc byte[SnapshotCodec.RecordHeaderSize];
                if (!HandleEx.TryReadExact(_handle, recordHeader, ref _offset))
                    throw new EndOfStreamException("Binary snapshot record header is truncated.");

                var recordStart = _offset - SnapshotCodec.RecordHeaderSize;
                var bodyLength = BinaryPrimitives.ReadUInt32LittleEndian(recordHeader[1..]);
                if (bodyLength > SnapshotCodec.MaxRecordBodyLength)
                    throw new InvalidDataException("Binary snapshot record body exceeds the maximum allowed length.");

                var recordLength = SnapshotCodec.ComputeRecordLength(int.CreateChecked(bodyLength));
                if (recordLength > _footerOffset - recordStart)
                    throw new InvalidDataException("Binary snapshot record extends past the file footer.");

                if (_scratch.Length < recordLength)
                    _scratch = new byte[recordLength];

                recordHeader.CopyTo(_scratch);
                return HandleEx.TryReadExact(_handle, _scratch.AsSpan(SnapshotCodec.RecordHeaderSize, recordLength - SnapshotCodec.RecordHeaderSize), ref _offset)
                    ? _scratch.AsSpan(0, recordLength)
                    : throw new EndOfStreamException("Binary snapshot record body is truncated.");
            }

            private void ValidateFooter()
            {
                if (_offset != _footerOffset)
                    throw new InvalidDataException("Binary snapshot file footer is misaligned.");

                Span<byte> footer = stackalloc byte[SnapshotCodec.FileFooterSize];
                if (!HandleEx.TryReadExact(_handle, footer, ref _offset))
                    throw new EndOfStreamException("Binary snapshot file footer is truncated.");

                SnapshotCodec.ValidateFileFooter(footer, Crc32C.Finalize(_crc));
                _footerValidated = true;
            }
        }
    }
}
