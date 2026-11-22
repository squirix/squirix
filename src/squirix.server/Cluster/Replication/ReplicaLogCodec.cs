using System;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
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
    /// Version 6 carries compact boolean outcomes for removal and distributed renewal, without response values. It carries the outcome and the effect the leader decided: the resulting entry of an upserting mutation and its pinned
    /// absolute deadline, or the passed deadline of an entry the leader expired, and no payload checksum. Writers retain this encoding for records without an eviction.
    /// </remarks>
    private const ushort Version = 6;

    /// <summary>Only insertion records carrying a leader-selected victim append a UTF-8 victim key after the timestamps.</summary>
    private const ushort EvictionVersion = 7;

    private const int VersionSize = sizeof(ushort);

    private const int LengthPrefixSize = sizeof(int);

    private const int LengthPrefixedFieldCount = 9;

    /// <summary>Bit per length-prefixed field, in field order, set for the UTF-8 string fields: operation id, scope, record kind, cache name, mutation kind.</summary>
    private const int StringFieldMask = 0b0101_1011;

    private const int TailSize = 3 * sizeof(long);

    /// <summary>Decodes a record for immediate use while the caller retains its unchanged canonical buffer.</summary>
    /// <param name="bytes">Canonical bytes whose lifetime covers consumption of the record.</param>
    /// <returns>The decoded record, or <see langword="null" /> for an invalid payload.</returns>
    /// <remarks>Binary fields borrow the source; callers must not retain them in recovery or idempotency state.</remarks>
    internal static ReplicaLogRecord? DecodeBorrowed(ReadOnlyMemory<byte> bytes)
    {
        var span = bytes.Span;
        if (span.Length < VersionSize || BinaryPrimitives.ReadUInt16LittleEndian(span) is not (Version or EvictionVersion))
            return null;

        var rest = span[VersionSize..];
        try
        {
            if (!Decoder.TryReadRecord(ref rest, bytes, out var record))
                return null;

            if (BinaryPrimitives.ReadUInt16LittleEndian(span) == Version)
                return rest.IsEmpty ? record : null;

            var field = rest;
            if (!TrySkipField(ref rest, true) || !rest.IsEmpty || field.Length == LengthPrefixSize)
                return null;

            return record!.Value with { EvictedKey = Encoding.UTF8.GetString(field[LengthPrefixSize..]) };
        }
        catch (DecoderFallbackException)
        {
            return null;
        }
    }

    /// <summary>Checks that canonical bytes would decode to a record at the given position, without decoding them.</summary>
    /// <param name="payload">The canonical payload bytes.</param>
    /// <param name="logIndex">The log index the embedded index must equal.</param>
    /// <param name="term">The term the embedded term must equal.</param>
    /// <returns>
    /// <see langword="true" /> when <see cref="DecodeBorrowed" /> accepts the payload and its embedded index and term equal the arguments;
    /// otherwise <see langword="false" />. Nothing is allocated.
    /// </returns>
    internal static bool IsWellFormed(ReadOnlySpan<byte> payload, ulong logIndex, ulong term)
    {
        if (payload.Length < VersionSize)
            return false;

        var version = BinaryPrimitives.ReadUInt16LittleEndian(payload);
        if (version is not (Version or EvictionVersion))
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

        return version == Version ? rest.Length == TailSize : HasValidEvictionTail(rest);
    }

    /// <summary>Encodes a record to its canonical bytes.</summary>
    /// <param name="record">The record to encode.</param>
    /// <param name="key">The textual key to encode directly instead of <see cref="ReplicaLogRecord.KeyPayload" />, or null to use the record bytes.</param>
    /// <returns>The canonical payload bytes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when a field exceeds the 32-bit length prefix.</exception>
    internal static byte[] Encode(in ReplicaLogRecord record, string? key = null)
    {
        var buffer = AllocateRecord(in record, key, record.MutationPayload.Length, out var mutation);
        record.MutationPayload.CopyTo(mutation);
        return buffer;
    }

    /// <summary>Writes the record envelope and reserves the mutation field for the caller to fill before publication.</summary>
    /// <param name="record">The record supplying the envelope fields.</param>
    /// <param name="key">The textual key, or null to use the key bytes in the record.</param>
    /// <param name="mutationLength">The number of bytes reserved for the mutation.</param>
    /// <param name="mutation">The entire mutation slot in the returned buffer.</param>
    /// <returns>The canonical buffer with the mutation slot still uninitialized.</returns>
    /// <exception cref="InvalidOperationException">The total encoded length exceeds the supported maximum.</exception>
    internal static byte[] AllocateRecord(in ReplicaLogRecord record, string? key, int mutationLength, out Memory<byte> mutation)
    {
        // Measure each string once and project the exact payload length before allocating, so a
        // pathological record is rejected before any buffer exists.
        var operationIdLength = Encoding.UTF8.GetByteCount(record.OperationId);
        var operationScopeLength = Encoding.UTF8.GetByteCount(record.OperationScope);
        var recordKindLength = Encoding.UTF8.GetByteCount(record.RecordKind);
        var cacheNameLength = Encoding.UTF8.GetByteCount(record.CacheName);
        var mutationKindLength = Encoding.UTF8.GetByteCount(record.MutationKind);
        var keyLength = key == null ? record.KeyPayload.Length : Encoding.UTF8.GetByteCount(key);

        var length = VersionSize + (2L * sizeof(ulong)) + (LengthPrefixedFieldCount * LengthPrefixSize) + (3L * sizeof(long))
            + operationIdLength + operationScopeLength + recordKindLength + cacheNameLength + mutationKindLength
            + record.OperationFingerprint.Length + keyLength + mutationLength + record.OutcomePayload.Length;
        var victimBytes = record.EvictedKey == null ? 0 : Encoding.UTF8.GetByteCount(record.EvictedKey);
        if (record.EvictedKey != null)
            length += long.CreateChecked(LengthPrefixSize) + victimBytes;
        if (length > int.MaxValue)
            throw new InvalidOperationException("Replica log record exceeds the maximum encoded length.");

        var buffer = GC.AllocateUninitializedArray<byte>(int.CreateChecked(length));
        var span = buffer.AsSpan();

        span = WriteRecordHeader(span, in record);
        span = WriteString(span, record.OperationId, operationIdLength);
        span = WriteString(span, record.OperationScope, operationScopeLength);
        span = WriteBytes(span, record.OperationFingerprint.Span);
        span = WriteString(span, record.RecordKind, recordKindLength);
        span = WriteString(span, record.CacheName, cacheNameLength);
        span = key == null ? WriteBytes(span, record.KeyPayload.Span) : WriteString(span, key, keyLength);
        span = WriteString(span, record.MutationKind, mutationKindLength);
        BinaryPrimitives.WriteInt32LittleEndian(span, mutationLength);
        mutation = buffer.AsMemory(buffer.Length - span.Length + LengthPrefixSize, mutationLength);
        span = span[(LengthPrefixSize + mutationLength)..];
        WriteRecordTail(span, in record, victimBytes);
        return buffer;
    }

    private static bool HasValidEvictionTail(ReadOnlySpan<byte> rest)
    {
        if (rest.Length <= TailSize + LengthPrefixSize)
            return false;

        rest = rest[TailSize..];
        return TrySkipField(ref rest, true) && rest.IsEmpty;
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

    private static Span<byte> WriteRecordHeader(Span<byte> span, scoped in ReplicaLogRecord record)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(span, record.EvictedKey == null ? Version : EvictionVersion);
        span = span[VersionSize..];
        BinaryPrimitives.WriteUInt64LittleEndian(span, record.LogIndex);
        BinaryPrimitives.WriteUInt64LittleEndian(span[sizeof(ulong)..], record.Term);
        return span[(2 * sizeof(ulong))..];
    }

    private static void WriteRecordTail(Span<byte> span, in ReplicaLogRecord record, int victimBytes)
    {
        span = WriteBytes(span, record.OutcomePayload.Span);
        WriteTimestamps(span, in record);
        if (record.EvictedKey != null)
            _ = WriteString(span[TailSize..], record.EvictedKey, victimBytes);
    }

    private static void WriteTimestamps(Span<byte> destination, in ReplicaLogRecord record)
    {
        BinaryPrimitives.WriteInt64LittleEndian(destination, record.ExpiresUtcTicks);
        BinaryPrimitives.WriteInt64LittleEndian(destination[sizeof(long)..], record.DecidedUtcTicks);
        BinaryPrimitives.WriteInt64LittleEndian(destination[(2 * sizeof(long))..], record.ResolvedUtcTicks);
    }

    /// <summary>Reads canonical fields from a caller-owned span cursor with bounds checks.</summary>
    private static class Decoder
    {
        /// <summary>UTF-8 decoder that throws on malformed sequences so corrupt canonical payloads are rejected.</summary>
        private static readonly UTF8Encoding StrictUtf8 = new(false, true);

        /// <summary>Reads the head, middle, and tail sections in field order into a record.</summary>
        /// <param name="rest">The remaining canonical fields, advanced as fields are read.</param>
        /// <param name="borrowedSource">The source buffer containing the binary fields.</param>
        /// <param name="record">The assembled record when all sections decode; otherwise <see langword="null" />.</param>
        /// <returns><see langword="true" /> when every section decoded; otherwise <see langword="false" />.</returns>
        internal static bool TryReadRecord(ref ReadOnlySpan<byte> rest, ReadOnlyMemory<byte> borrowedSource, out ReplicaLogRecord? record)
        {
            record = null;

            if (!TryReadHead(ref rest, borrowedSource, out var head))
                return false;
            if (!TryReadMiddle(ref rest, borrowedSource, out var middle))
                return false;
            if (!TryReadTail(ref rest, out var tail))
                return false;

            record = CreateRecord(in head, in middle, in tail);
            return true;
        }

        private static ReplicaLogRecord CreateRecord(in HeadSection head, in MiddleSection middle, in TailSection tail)
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

        private static bool TryReadHead(ref ReadOnlySpan<byte> rest, ReadOnlyMemory<byte> borrowedSource, out HeadSection head)
        {
            head = default;
            if (!TryReadUInt64(ref rest, out var logIndex))
                return false;
            if (!TryReadUInt64(ref rest, out var term))
                return false;
            if (!TryReadString(ref rest, out var operationId))
                return false;
            if (!TryReadString(ref rest, out var operationScope))
                return false;
            if (!TryReadBytes(ref rest, borrowedSource, out var fingerprint))
                return false;
            if (!TryReadKind(ref rest, out var recordKind))
                return false;
            if (!TryReadString(ref rest, out var cacheName))
                return false;

            head = new HeadSection(logIndex, term, operationId, operationScope, fingerprint, recordKind, cacheName);
            return true;
        }

        private static bool TryReadMiddle(ref ReadOnlySpan<byte> rest, ReadOnlyMemory<byte> borrowedSource, out MiddleSection middle)
        {
            middle = default;
            if (!TryReadBytes(ref rest, borrowedSource, out var key))
                return false;
            if (!TryReadKind(ref rest, out var mutationKind))
                return false;
            if (!TryReadBytes(ref rest, borrowedSource, out var mutation))
                return false;
            if (!TryReadBytes(ref rest, borrowedSource, out var outcome))
                return false;

            middle = new MiddleSection(key, mutationKind, mutation, outcome);
            return true;
        }

        private static bool TryReadTail(ref ReadOnlySpan<byte> rest, out TailSection tail)
        {
            tail = default;
            if (!TryReadInt64(ref rest, out var expires))
                return false;
            if (!TryReadInt64(ref rest, out var created))
                return false;
            if (!TryReadInt64(ref rest, out var resolved))
                return false;

            tail = new TailSection(expires, created, resolved);
            return true;
        }

        private static bool TryReadUInt64(ref ReadOnlySpan<byte> rest, out ulong value)
        {
            value = 0;
            if (!TryTake(ref rest, 8, out var bytes))
                return false;

            value = BinaryPrimitives.ReadUInt64LittleEndian(bytes);
            return true;
        }

        private static bool TryReadInt64(ref ReadOnlySpan<byte> rest, out long value)
        {
            value = 0;
            if (!TryTake(ref rest, 8, out var bytes))
                return false;

            value = BinaryPrimitives.ReadInt64LittleEndian(bytes);
            return true;
        }

        private static bool TryReadString(ref ReadOnlySpan<byte> rest, out string value)
        {
            value = string.Empty;
            if (!TryTakeLengthPrefixed(ref rest, out var bytes))
                return false;

            value = StrictUtf8.GetString(bytes);
            return true;
        }

        private static bool TryReadKind(ref ReadOnlySpan<byte> rest, out string value)
        {
            value = string.Empty;
            if (!TryTakeLengthPrefixed(ref rest, out var bytes))
                return false;

            value = bytes.Length switch
            {
                3 when bytes.SequenceEqual("set"u8) => "set",
                5 when bytes.SequenceEqual("touch"u8) => "touch",
                6 when bytes.SequenceEqual("remove"u8) => "remove",
                6 when bytes.SequenceEqual("tryadd"u8) => "tryadd",
                6 when bytes.SequenceEqual("update"u8) => "update",
                6 when bytes.SequenceEqual("expire"u8) => "expire",
                10 when bytes.SequenceEqual("LeaderTerm"u8) => "LeaderTerm",
                10 when bytes.SequenceEqual("Expiration"u8) => "Expiration",
                11 when bytes.SequenceEqual("leader-noop"u8) => "leader-noop",
                12 when bytes.SequenceEqual("UserMutation"u8) => "UserMutation",
                16 when bytes.SequenceEqual("removeexpiration"u8) => "removeexpiration",
                _ => StrictUtf8.GetString(bytes),
            };
            return true;
        }

        private static bool TryReadBytes(ref ReadOnlySpan<byte> rest, ReadOnlyMemory<byte> borrowedSource, out ReadOnlyMemory<byte> value)
        {
            value = default;
            if (!TryTakeLengthPrefixed(ref rest, out var bytes))
                return false;

            value = borrowedSource.Slice(borrowedSource.Length - rest.Length - bytes.Length, bytes.Length);
            return true;
        }

        private static bool TryTake(ref ReadOnlySpan<byte> rest, int size, out ReadOnlySpan<byte> slice)
        {
            slice = default;
            if (size < 0 || size > rest.Length)
                return false;

            slice = rest[..size];
            rest = rest[size..];
            return true;
        }

        private static bool TryTakeLengthPrefixed(ref ReadOnlySpan<byte> rest, out ReadOnlySpan<byte> slice)
        {
            slice = default;
            if (!TryTake(ref rest, 4, out var lengthBytes))
                return false;

            var length = BinaryPrimitives.ReadInt32LittleEndian(lengthBytes);
            return length >= 0 && TryTake(ref rest, length, out slice);
        }

        [Immutable]
        private readonly record struct HeadSection(ulong LogIndex, ulong Term, string OperationId, string OperationScope, ReadOnlyMemory<byte> Fingerprint, string RecordKind, string CacheName);

        [Immutable]
        private readonly record struct MiddleSection(ReadOnlyMemory<byte> Key, string MutationKind, ReadOnlyMemory<byte> Mutation, ReadOnlyMemory<byte> Outcome);

        [Immutable]
        [StructLayout(LayoutKind.Auto)]
        private readonly record struct TailSection(long ExpiresUtcTicks, long DecidedUtcTicks, long ResolvedUtcTicks);
    }
}
