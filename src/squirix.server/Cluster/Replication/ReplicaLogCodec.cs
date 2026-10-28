using System;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Unicode;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Canonical binary encoding of <see cref="ReplicaLogRecord" /> for durable log payloads.</summary>
/// <remarks>
/// The encoding is an explicit field order with 32-bit little-endian length prefixes; decoding rejects
/// truncated buffers, trailing bytes, and unknown versions. Journal frames add their own CRC; owner, follower log, and follower
/// applier observe identical bytes without sharing anything but this codec. The wire carries these bytes unchanged, so a follower checks them with
/// <see cref="IsWellFormed" /> before it stores them.
/// </remarks>
[Immutable]
internal static class ReplicaLogCodec
{
    /// <summary>The canonical encoding version.</summary>
    /// <remarks>
    /// Version 5 carries the outcome and the effect the leader decided: the resulting entry of an upserting mutation and its pinned
    /// absolute deadline, or the passed deadline of an entry the leader expired, and no payload checksum. Records of any other version are refused, so every node
    /// of a replica group must run the same encoding version.
    /// </remarks>
    private const ushort Version = 5;

    private const int VersionSize = sizeof(ushort);

    private const int LengthPrefixSize = sizeof(int);

    private const int LengthPrefixedFieldCount = 9;

    /// <summary>Bit per length-prefixed field, in field order, set for the UTF-8 string fields: operation id, scope, record kind, cache name, mutation kind.</summary>
    private const int StringFieldMask = 0b0101_1011;

    private const int TailSize = 3 * sizeof(long);

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

    /// <summary>Checks that canonical bytes would decode to a record at the given position, without decoding them.</summary>
    /// <param name="payload">The canonical payload bytes.</param>
    /// <param name="logIndex">The log index the embedded index must equal.</param>
    /// <param name="term">The term the embedded term must equal.</param>
    /// <returns>
    /// <see langword="true" /> when <see cref="Decode" /> accepts the payload and its embedded index and term equal the arguments;
    /// otherwise <see langword="false" />. Nothing is allocated.
    /// </returns>
    internal static bool IsWellFormed(ReadOnlySpan<byte> payload, ulong logIndex, ulong term)
    {
        if (payload.Length < VersionSize || BinaryPrimitives.ReadUInt16LittleEndian(payload) != Version)
            return false;

        var rest = payload[VersionSize..];
        if (rest.Length < 2 * sizeof(ulong))
            return false;

        if (BinaryPrimitives.ReadUInt64LittleEndian(rest) != logIndex || BinaryPrimitives.ReadUInt64LittleEndian(rest[sizeof(ulong)..]) != term)
            return false;

        rest = rest[(2 * sizeof(ulong))..];

        for (var field = 0; field < LengthPrefixedFieldCount; field++)
        {
            if (!TrySkipField(ref rest, ((StringFieldMask >> field) & 1) != 0))
                return false;
        }

        return rest.Length == TailSize;
    }

    /// <summary>Encodes a record to its canonical bytes.</summary>
    /// <param name="record">The record to encode.</param>
    /// <returns>The canonical payload bytes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when a field exceeds the 32-bit length prefix.</exception>
    internal static byte[] Encode(in ReplicaLogRecord record)
    {
        // Measure each string once and project the exact payload length before allocating, so a
        // pathological record is rejected before any buffer exists.
        var operationIdLength = Encoding.UTF8.GetByteCount(record.OperationId);
        var operationScopeLength = Encoding.UTF8.GetByteCount(record.OperationScope);
        var recordKindLength = Encoding.UTF8.GetByteCount(record.RecordKind);
        var cacheNameLength = Encoding.UTF8.GetByteCount(record.CacheName);
        var mutationKindLength = Encoding.UTF8.GetByteCount(record.MutationKind);

        var length = VersionSize + (2L * sizeof(ulong)) + (LengthPrefixedFieldCount * LengthPrefixSize) + (3L * sizeof(long))
            + operationIdLength + operationScopeLength + recordKindLength + cacheNameLength + mutationKindLength
            + record.OperationFingerprint.Length + record.KeyPayload.Length + record.MutationPayload.Length + record.OutcomePayload.Length;
        if (length > int.MaxValue)
            throw new InvalidOperationException("Replica log record exceeds the maximum encoded length.");

#pragma warning disable ZA0302 // ZA0302: exact-size result buffer escapes to the caller, which owns it.
        var buffer = new byte[int.CreateChecked(length)];
#pragma warning restore ZA0302
        var span = buffer.AsSpan();

        BinaryPrimitives.WriteUInt16LittleEndian(span, Version);
        span = span[VersionSize..];
        BinaryPrimitives.WriteUInt64LittleEndian(span, record.LogIndex);
        BinaryPrimitives.WriteUInt64LittleEndian(span[sizeof(ulong)..], record.Term);
        span = span[(2 * sizeof(ulong))..];
        span = WriteString(span, record.OperationId, operationIdLength);
        span = WriteString(span, record.OperationScope, operationScopeLength);
        span = WriteBytes(span, record.OperationFingerprint.Span);
        span = WriteString(span, record.RecordKind, recordKindLength);
        span = WriteString(span, record.CacheName, cacheNameLength);
        span = WriteBytes(span, record.KeyPayload.Span);
        span = WriteString(span, record.MutationKind, mutationKindLength);
        span = WriteBytes(span, record.MutationPayload.Span);
        span = WriteBytes(span, record.OutcomePayload.Span);
        WriteTimestamps(span, in record);
        return buffer;
    }

    private static bool TrySkipField(ref ReadOnlySpan<byte> rest, bool isString)
    {
        if (rest.Length < LengthPrefixSize)
            return false;

        var length = BinaryPrimitives.ReadInt32LittleEndian(rest);
        var body = rest[LengthPrefixSize..];
        if (length < 0 || length > body.Length)
            return false;

        if (isString && !Utf8.IsValid(body[..length]))
            return false;

        rest = body[length..];
        return true;
    }

    private static Span<byte> WriteBytes(Span<byte> destination, ReadOnlySpan<byte> value)
    {
        BinaryPrimitives.WriteInt32LittleEndian(destination, value.Length);
        value.CopyTo(destination[LengthPrefixSize..]);
        return destination[(LengthPrefixSize + value.Length)..];
    }

    private static Span<byte> WriteString(Span<byte> destination, string value, int byteCount)
    {
        BinaryPrimitives.WriteInt32LittleEndian(destination, byteCount);
        _ = Encoding.UTF8.GetBytes(value, destination.Slice(LengthPrefixSize, byteCount));
        return destination[(LengthPrefixSize + byteCount)..];
    }

    private static void WriteTimestamps(Span<byte> destination, in ReplicaLogRecord record)
    {
        BinaryPrimitives.WriteInt64LittleEndian(destination, record.ExpiresUtcTicks);
        BinaryPrimitives.WriteInt64LittleEndian(destination[sizeof(long)..], record.DecidedUtcTicks);
        BinaryPrimitives.WriteInt64LittleEndian(destination[(2 * sizeof(long))..], record.ResolvedUtcTicks);
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
                tail.DecidedUtcTicks,
                tail.ResolvedUtcTicks);
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

            tail = new TailSection(expires, created, resolved);
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
        private sealed record TailSection(long ExpiresUtcTicks, long DecidedUtcTicks, long ResolvedUtcTicks);

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
