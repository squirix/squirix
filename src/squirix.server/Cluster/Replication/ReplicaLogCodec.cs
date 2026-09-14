using System;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Canonical binary encoding of <see cref="ReplicaLogRecord" /> for durable log payloads.</summary>
/// <remarks>
/// The encoding is an explicit field order with 32-bit little-endian length prefixes; decoding rejects
/// truncated buffers, trailing bytes, and unknown versions. The payload checksums ride opaquely inside
/// the encoding (journal frames add their own CRC); owner, follower log, and follower applier observe
/// identical bytes without sharing anything but this codec.
/// </remarks>
[Immutable]
internal static class ReplicaLogCodec
{
    private const ushort Version = 1;

    /// <summary>Decodes canonical bytes back to a record.</summary>
    /// <param name="bytes">The canonical payload bytes.</param>
    /// <returns>The decoded record, or <see langword="null" /> when the payload is invalid.</returns>
    internal static ReplicaLogRecord? Decode(ReadOnlyMemory<byte> bytes)
    {
        var span = bytes.Span;
        if (span.Length < 2 || BinaryPrimitives.ReadUInt16LittleEndian(span) != Version)
            return null;

        var decoder = new Decoder(bytes[2..]);
        try
        {
            var isComplete = decoder.TryReadRecord(out var record) && decoder.AtEnd;
            return isComplete ? record : null;
        }
        catch (DecoderFallbackException)
        {
            // Malformed UTF-8 inside a length-prefixed string field marks the canonical payload as corrupt
            // and is refused exactly like any other structurally invalid payload.
            return null;
        }
    }

    /// <summary>Encodes a record to its canonical bytes.</summary>
    /// <param name="record">The record to encode.</param>
    /// <returns>The canonical payload bytes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when a field exceeds the 32-bit length prefix.</exception>
    [SuppressMessage(
        "Usage",
        "MA0045:Use async disposable",
        Justification = "BinaryWriter and MemoryStream are in-memory and are intentionally encoded synchronously before durable asynchronous I/O.")]
    internal static byte[] Encode(in ReplicaLogRecord record)
    {
        // Project the exact payload length before serializing so a pathological record is rejected
        // before the buffer can grow.
        var length = 2L + 8 + 8;
        length += 4L + Encoding.UTF8.GetByteCount(record.OperationId) + 4L + Encoding.UTF8.GetByteCount(record.OperationScope);
        length += 4L + record.OperationFingerprint.Length;
        length += 4L + Encoding.UTF8.GetByteCount(record.RecordKind) + 4L + Encoding.UTF8.GetByteCount(record.CacheName);
        length += 4L + record.KeyPayload.Length;
        length += 4L + Encoding.UTF8.GetByteCount(record.MutationKind);
        length += 4L + record.MutationPayload.Length;
        length += 4L + record.OutcomePayload.Length;
        length += 8 + 8 + 8 + 4;
        if (length > int.MaxValue)
            throw new InvalidOperationException("Replica log record exceeds the maximum encoded length.");

        using var stream = new MemoryStream(int.CreateChecked(length));
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
        {
            writer.Write(Version);
            writer.Write(record.LogIndex);
            writer.Write(record.Term);
            WriteString(writer, record.OperationId);
            WriteString(writer, record.OperationScope);
            WriteBytes(writer, record.OperationFingerprint.Span);
            WriteString(writer, record.RecordKind);
            WriteString(writer, record.CacheName);
            WriteBytes(writer, record.KeyPayload.Span);
            WriteString(writer, record.MutationKind);
            WriteBytes(writer, record.MutationPayload.Span);
            WriteBytes(writer, record.OutcomePayload.Span);
            writer.Write(record.ExpiresUtcTicks);
            writer.Write(record.CreatedUtcTicks);
            writer.Write(record.ResolvedUtcTicks);
            writer.Write(record.PayloadChecksum);
        }

        // The byte count was projected exactly before writing, so the underlying allocated buffer holds
        // precisely the encoded payload; return it directly instead of copying via ToArray().
        return stream.GetBuffer();
    }

    private static void WriteBytes(BinaryWriter writer, ReadOnlySpan<byte> value)
    {
        writer.Write(value.Length);
        writer.Write(value);
    }

    private static void WriteString(BinaryWriter writer, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        writer.Write(bytes.Length);
        writer.Write(bytes);
    }

    /// <summary>Cursor reader for canonical record fields with bounds checks.</summary>
    private sealed class Decoder
    {
        /// <summary>UTF-8 decoder that throws on malformed sequences so corrupt canonical payloads are rejected.</summary>
        private static readonly UTF8Encoding StrictUtf8 = new(false, true);

        private readonly ReadOnlyMemory<byte> _buffer;
        private int _offset;

        internal Decoder(ReadOnlyMemory<byte> buffer)
        {
            _buffer = buffer;
        }

        internal bool AtEnd => _offset == _buffer.Length;

        /// <summary>Reads the head, middle, and tail sections in field order into a record.</summary>
        /// <param name="record">The assembled record when all sections decode; otherwise <see langword="null" />.</param>
        /// <returns><see langword="true" /> when every section decoded; otherwise <see langword="false" />.</returns>
        internal bool TryReadRecord(out ReplicaLogRecord? record)
        {
            record = null;

            if (!TryReadHead(out var head))
                return false;
            if (!TryReadMiddle(out var middle))
                return false;
            if (!TryReadTail(out var tail))
                return false;

            record = CreateRecord(head, middle, tail);
            return true;
        }

        private static ReplicaLogRecord CreateRecord(HeadSection head, MiddleSection middle, TailSection tail)
        {
            return new ReplicaLogRecord(
                head.LogIndex,
                head.Term,
                head.OperationId,
                head.OperationScope,
                head.Fingerprint,
                head.RecordKind,
                head.CacheName,
                middle.Key,
                middle.MutationKind,
                middle.Mutation,
                middle.Outcome,
                tail.ExpiresUtcTicks,
                tail.CreatedUtcTicks,
                tail.ResolvedUtcTicks,
                tail.PayloadChecksum);
        }

        private bool TryReadHead([NotNullWhen(true)] out HeadSection? head)
        {
            head = null;
            if (!TryReadUInt64(out var logIndex))
                return false;
            if (!TryReadUInt64(out var term))
                return false;
            if (!TryReadString(out var operationId))
                return false;
            if (!TryReadString(out var operationScope))
                return false;
            if (!TryReadBytes(out var fingerprint))
                return false;
            if (!TryReadString(out var recordKind))
                return false;
            if (!TryReadString(out var cacheName))
                return false;

            head = new HeadSection(logIndex, term, operationId, operationScope, fingerprint, recordKind, cacheName);
            return true;
        }

        private bool TryReadMiddle([NotNullWhen(true)] out MiddleSection? middle)
        {
            middle = null;
            if (!TryReadBytes(out var key))
                return false;
            if (!TryReadString(out var mutationKind))
                return false;
            if (!TryReadBytes(out var mutation))
                return false;
            if (!TryReadBytes(out var outcome))
                return false;

            middle = new MiddleSection(key, mutationKind, mutation, outcome);
            return true;
        }

        private bool TryReadTail([NotNullWhen(true)] out TailSection? tail)
        {
            tail = null;
            if (!TryReadInt64(out var expires))
                return false;
            if (!TryReadInt64(out var created))
                return false;
            if (!TryReadInt64(out var resolved))
                return false;
            if (!TryReadUInt32(out var checksum))
                return false;

            tail = new TailSection(expires, created, resolved, checksum);
            return true;
        }

        private bool TryReadUInt64(out ulong value)
        {
            value = 0;
            if (!TryTake(8, out var bytes))
                return false;

            value = BinaryPrimitives.ReadUInt64LittleEndian(bytes);
            return true;
        }

        private bool TryReadInt64(out long value)
        {
            value = 0;
            if (!TryTake(8, out var bytes))
                return false;

            value = BinaryPrimitives.ReadInt64LittleEndian(bytes);
            return true;
        }

        private bool TryReadUInt32(out uint value)
        {
            value = 0;
            if (!TryTake(4, out var bytes))
                return false;

            value = BinaryPrimitives.ReadUInt32LittleEndian(bytes);
            return true;
        }

        private bool TryReadString(out string value)
        {
            value = string.Empty;
            if (!TryTakeLengthPrefixed(out var bytes))
                return false;

            value = StrictUtf8.GetString(bytes);
            return true;
        }

        private bool TryReadBytes(out byte[] value)
        {
            value = [];
            if (!TryTakeLengthPrefixed(out var bytes))
                return false;

            value = OwnedBufferKit.CopyToOwned(bytes);
            return true;
        }

        private bool TryTake(int size, out ReadOnlySpan<byte> slice)
        {
            slice = default;
            if (size < 0 || size > _buffer.Length - _offset)
                return false;

            slice = _buffer.Span.Slice(_offset, size);
            _offset += size;
            return true;
        }

        private bool TryTakeLengthPrefixed(out ReadOnlySpan<byte> slice)
        {
            slice = default;
            if (!TryTake(4, out var lengthBytes))
                return false;

            var length = BinaryPrimitives.ReadInt32LittleEndian(lengthBytes);
            return length >= 0 && TryTake(length, out slice);
        }

        [Immutable]
        private sealed record HeadSection(ulong LogIndex, ulong Term, string OperationId, string OperationScope, byte[] Fingerprint, string RecordKind, string CacheName);

        [Immutable]
        private sealed record MiddleSection(byte[] Key, string MutationKind, byte[] Mutation, byte[] Outcome);

        [Immutable]
        private sealed record TailSection(long ExpiresUtcTicks, long CreatedUtcTicks, long ResolvedUtcTicks, uint PayloadChecksum);

        /// <summary>Exact-size owned byte buffer helper for decoder output.</summary>
        /// <remarks>
        /// The decoded record outlives the source buffer, so fields need owned copies that the span
        /// cannot lend. The escape is exact-size and caller-retained.
        /// </remarks>
        private static class OwnedBufferKit
        {
#pragma warning disable ZA0302 // ZA0302: exact-size owned buffer escape; the decoder output retains ownership.
            internal static byte[] CopyToOwned(ReadOnlySpan<byte> source)
            {
                var owned = new byte[source.Length];
                source.CopyTo(owned);
                return owned;
            }
#pragma warning restore ZA0302
        }
    }
}
