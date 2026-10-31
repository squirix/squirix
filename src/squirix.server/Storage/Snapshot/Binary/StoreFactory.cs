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

        private static uint ReadRecord<T>(SnapshotFileWindow window, long footerOffset, uint crc, LoadedRecords<T> records)
        {
            if (!window.TryRead(ReadRecordLength(window, footerOffset), out var recordBytes))
                throw new EndOfStreamException("Binary snapshot record body is truncated.");

            if (!SnapshotCodec.TryReadRecord(recordBytes, out var kind, out var body))
                throw new InvalidDataException("Binary snapshot record is truncated.");

            crc = Crc32C.Append(crc, recordBytes);
            switch (kind)
            {
                case RecordKind.Entry:
                    records.AddEntry(body);
                    break;

                case RecordKind.Idempotency:
                    records.IdempotencyRecords.Add(IdempotencyCodec.Read(body));
                    break;

                default:
                    throw new InvalidDataException($"Unsupported binary snapshot record kind: {kind}.");
            }

            window.Advance(recordBytes.Length);
            return crc;
        }

        private static int ReadRecordLength(SnapshotFileWindow window, long footerOffset)
        {
            if (!window.TryRead(SnapshotCodec.RecordHeaderSize, out var recordHeader))
                throw new EndOfStreamException("Binary snapshot record header is truncated.");

            var bodyLength = BinaryPrimitives.ReadUInt32LittleEndian(recordHeader[1..]);
            if (bodyLength > SnapshotCodec.MaxRecordBodyLength)
                throw new InvalidDataException("Binary snapshot record body exceeds the maximum allowed length.");

            var recordLength = SnapshotCodec.ComputeRecordLength(int.CreateChecked(bodyLength));
            return recordLength <= footerOffset - window.Position
                ? recordLength
                : throw new InvalidDataException("Binary snapshot record extends past the file footer.");
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
            var records = new LoadedRecords<T>(expiredAsOf);
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

                crc = ReadRecord(window, footerOffset, crc, records);
            }

            ValidateFooter(window, footerOffset, crc);
            return new LoadResult<T>(records.Entries, records.IdempotencyRecords, records.Lengths);
        }

        /// <summary>The records a load has read so far.</summary>
        /// <typeparam name="T">The entry value type.</typeparam>
        [Mutable]
        private sealed class LoadedRecords<T>
        {
            private readonly DateTime? _expiredAsOf;

            internal LoadedRecords(DateTime? expiredAsOf)
            {
                _expiredAsOf = expiredAsOf;
            }

            internal List<(CacheKey Key, NodeCacheEntry<T> Entry)> Entries { get; } = [with(1024)];

            internal List<PersistedIdempotencyRecord> IdempotencyRecords { get; } = [with(16)];

            internal List<int?> Lengths { get; } = [with(1024)];

            /// <summary>Adds an entry record, skipping it when it expired by the load cutoff.</summary>
            /// <param name="body">The entry record body.</param>
            /// <exception cref="InvalidDataException">The body is not a readable entry.</exception>
            internal void AddEntry(ReadOnlySpan<byte> body)
            {
                if (!SnapshotCodec.TryReadEntryBody(body, out var key, out var entry, out var encodedLength) || entry == null)
                    throw new InvalidDataException("Binary snapshot entry body could not be read.");

                if (_expiredAsOf is { } utcNow && IsExpired(entry, utcNow))
                    return;

                if (!CacheEntryCodec.TryMapEntry<T>(entry, out var mapped) || mapped == null)
                    throw new InvalidDataException("Binary snapshot entry payload could not be read.");

                Entries.Add((key, mapped));
                Lengths.Add(encodedLength);
            }

            private static bool IsExpired(NodeCacheEntry<object?> entry, DateTime utcNow) => entry.ExpiresUtc is { } expiresUtc && expiresUtc.ToUniversalTime() <= utcNow;
        }
    }
}
