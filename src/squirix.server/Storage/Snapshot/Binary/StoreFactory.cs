using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot.Binary;

internal static class StoreFactory
{
    internal static ISnapshotReader CreateReader() => new SnapshotReader(SnapshotReader.DefaultWindowSize);

    internal static ISnapshotReader CreateReader(int windowSize) => new SnapshotReader(windowSize);

    internal static ISnapshotWriter CreateWriter(PersistenceOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new SnapshotWriter(options.DataDir, new FileOperations());
    }

    [Immutable]
    private sealed class SnapshotReader : ISnapshotReader
    {
        internal const int DefaultWindowSize = 512 * 1024;

        private readonly int _windowSize;

        internal SnapshotReader(int windowSize)
        {
            _windowSize = windowSize;
        }

        public ValueTask<LoadResult<T>> LoadStrictAsync<T>(string path, DateTime? expiredAsOf, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new ValueTask<LoadResult<T>>(Load<T>(path, expiredAsOf, cancellationToken));
        }

        private static bool IsExpired(NodeCacheEntry<object?> entry, DateTime utcNow) => entry.ExpiresUtc is { } expiresUtc && expiresUtc.ToUniversalTime() <= utcNow;

        private static uint ReadRecord<T>(
            SnapshotFileWindow window,
            long footerOffset,
            uint crc,
            DateTime? expiredAsOf,
            List<(CacheKey Key, NodeCacheEntry<T> Entry)> entries,
            List<int?> lengths,
            List<PersistedIdempotencyRecord> idempotencyRecords)
        {
            if (!window.TryRead(SnapshotCodec.RecordHeaderSize, out var recordHeader))
                throw new EndOfStreamException("Binary snapshot record header is truncated.");

            var bodyLength = BinaryPrimitives.ReadUInt32LittleEndian(recordHeader[1..]);
            if (bodyLength > SnapshotCodec.MaxRecordBodyLength)
                throw new InvalidDataException("Binary snapshot record body exceeds the maximum allowed length.");

            var recordLength = SnapshotCodec.ComputeRecordLength(int.CreateChecked(bodyLength));
            if (recordLength > footerOffset - window.Position)
                throw new InvalidDataException("Binary snapshot record extends past the file footer.");

            if (!window.TryRead(recordLength, out var recordBytes))
                throw new EndOfStreamException("Binary snapshot record body is truncated.");

            if (!SnapshotCodec.TryReadRecord(recordBytes, out var kind, out var body))
                throw new InvalidDataException("Binary snapshot record is truncated.");

            crc = Crc32C.Append(crc, recordBytes);
            switch (kind)
            {
                case RecordKind.Entry:
                    if (!SnapshotCodec.TryReadEntryBody(body, out var key, out var entry, out var encodedLength) || entry == null)
                        throw new InvalidDataException("Binary snapshot entry body could not be read.");

                    if (expiredAsOf is { } utcNow && IsExpired(entry, utcNow))
                        break;

                    if (!CacheEntryCodec.TryMapEntry<T>(entry, out var mapped) || mapped == null)
                        throw new InvalidDataException("Binary snapshot entry payload could not be read.");

                    entries.Add((key, mapped));
                    lengths.Add(encodedLength);
                    break;

                case RecordKind.Idempotency:
                    idempotencyRecords.Add(IdempotencyCodec.Read(body));
                    break;

                default:
                    throw new InvalidDataException($"Unsupported binary snapshot record kind: {kind}.");
            }

            window.Advance(recordLength);
            return crc;
        }

        private static void ValidateFooter(SnapshotFileWindow window, long footerOffset, uint crc)
        {
            if (window.Position != footerOffset)
                throw new InvalidDataException("Binary snapshot file footer is misaligned.");

            if (!window.TryRead(SnapshotCodec.FileFooterSize, out var footer))
                throw new EndOfStreamException("Binary snapshot file footer is truncated.");

            SnapshotCodec.ValidateFileFooter(footer, Crc32C.Finalize(crc));
        }

        private LoadResult<T> Load<T>(string path, DateTime? expiredAsOf, CancellationToken cancellationToken)
        {
            var entries = new List<(CacheKey Key, NodeCacheEntry<T> Entry)>(1024);
            var lengths = new List<int?>(1024);
            var idempotencyRecords = new List<PersistedIdempotencyRecord>(16);
            using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, FileOptions.SequentialScan);
            var fileLength = RandomAccess.GetLength(handle);
            if (fileLength < SnapshotCodec.FileHeaderSize + SnapshotCodec.FileFooterSize)
                throw new InvalidDataException("Binary snapshot file is truncated.");

            var footerOffset = fileLength - SnapshotCodec.FileFooterSize;
            using var window = new SnapshotFileWindow(handle, _windowSize, 0);
            if (!window.TryRead(SnapshotCodec.FileHeaderSize, out var header))
                throw new EndOfStreamException("Binary snapshot file header is truncated.");

            SnapshotCodec.ValidateFileHeader(header);
            window.Advance(SnapshotCodec.FileHeaderSize);
            var crc = Crc32C.Append(Crc32C.InitialValue, SnapshotCodec.Version);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (window.Position >= footerOffset)
                    break;

                crc = ReadRecord(window, footerOffset, crc, expiredAsOf, entries, lengths, idempotencyRecords);
            }

            ValidateFooter(window, footerOffset, crc);
            return new LoadResult<T>(entries, idempotencyRecords, lengths);
        }
    }
}
