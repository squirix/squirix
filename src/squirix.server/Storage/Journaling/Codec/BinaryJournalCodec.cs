using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Storage.Journaling.Codec;

/// <summary>Binary frame body codec for a pipelined journal (SJRN v1 file header).</summary>
internal static class BinaryJournalCodec
{
    /// <summary>
    /// SJRN v1 frame body layout: seq u64 | unixMs i64 | opcode u8 | nsLen u16 | keyLen u16 | payloadLen i32,
    /// followed by the namespace bytes, the key bytes, and the operation payload bytes.
    /// </summary>
    internal const int FixedPrefixSize = PayloadLengthOffset + PayloadLengthPrefixSize;

    private const int FingerprintLengthPrefixSize = sizeof(ushort);
    private const int KeyLengthOffset = NamespaceLengthOffset + NamespaceLengthPrefixSize;
    private const int KeyLengthPrefixSize = sizeof(ushort);

    /// <summary>Sentinel for a missing touch-expiration timestamp.</summary>
    private const long MissingExpirationUnixMs = 0L;

    private const int NamespaceLengthOffset = OpcodeOffset + OpcodeSize;
    private const int NamespaceLengthPrefixSize = sizeof(ushort);
    private const int OpcodeOffset = UnixMsOffset + UnixMsSize;
    private const int OpcodeSize = sizeof(byte);

    /// <summary>Idempotency payload length-prefix sizes.</summary>
    private const int OperationIdLengthPrefixSize = sizeof(ushort);

    private const int PayloadLengthOffset = KeyLengthOffset + KeyLengthPrefixSize;
    private const int PayloadLengthPrefixSize = sizeof(int);
    private const int ResponseLengthPrefixSize = sizeof(int);

    /// <summary>Fixed-prefix field offsets.</summary>
    private const int SequenceOffset = 0;

    /// <summary>Fixed-prefix field sizes.</summary>
    private const int SequenceSize = sizeof(ulong);

    /// <summary>A touch-expiration payload is a single Unix-milliseconds timestamp.</summary>
    private const int TimestampSize = sizeof(long);

    private const int UnixMsOffset = SequenceOffset + SequenceSize;

    private const int UnixMsSize = sizeof(long);

    private const string UnknownJournalOpcodeMessage = "Unknown journal opcode.";

    internal static int ComputeFrameBodyLength(JournalRecord record) => EncodeContext.From(record).BodyLength;

    internal static JournalRecord Decode(byte[] frameBuffer, int frameLength)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(frameLength, frameBuffer.Length);

        var frameBody = frameBuffer.AsSpan(SequenceOffset, frameLength);
        if (frameBody.Length < FixedPrefixSize)
            throw new InvalidDataException("binary journal frame body is truncated.");

        var header = ReadFrameHeader(frameBody);
        var cacheKey = ReadCacheKey(frameBody, header, out var offset);
        ThrowIfPayloadTruncated(frameBody, offset, header.PayloadLength);
        return DispatchDecode(frameBuffer, frameBody, header, cacheKey, offset);
    }

    internal static int Encode(JournalRecord record, Span<byte> destination, in EncodeContext context)
    {
        WriteFixedPrefix(destination, record, context.KeyNamespaceLength, context.KeyLength, context.PayloadUtf8Length);

        var offset = FixedPrefixSize;
        offset += Encoding.UTF8.GetBytes(record.Key.Namespace, destination[offset..]);
        offset += Encoding.UTF8.GetBytes(record.Key.Key, destination[offset..]);
        return WriteOperationPayload(record, destination, offset);
    }

    internal static EncodeContext PrepareEncode(JournalRecord record) => EncodeContext.From(record);

    private static NotSupportedException CreateOperationNotEncodableException() => new("Journal operation cannot be encoded.");

    private static JournalRecord DecodeIdempotencyOutcome(ulong seq, long unixMs, CacheKey cacheKey, byte[] frameBuffer, ReadOnlySpan<byte> frameBody, int offset, int payloadLen)
    {
        var payload = frameBody.Slice(offset, payloadLen);
        var pOff = 0;
        if (payload.Length < OperationIdLengthPrefixSize)
            throw new InvalidDataException("idempotency outcome operation id length is missing.");
        var opIdLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[pOff..]);
        pOff += OperationIdLengthPrefixSize;
        if (payload.Length < pOff + opIdLen + FingerprintLengthPrefixSize)
            throw new InvalidDataException("idempotency outcome operation id or fingerprint length is truncated.");
        var operationId = Encoding.UTF8.GetString(payload.Slice(pOff, opIdLen));
        pOff += opIdLen;
        var fpLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[pOff..]);
        pOff += FingerprintLengthPrefixSize;
        if (payload.Length < pOff + fpLen + ResponseLengthPrefixSize)
            throw new InvalidDataException("idempotency outcome fingerprint or response length is truncated.");
        var fingerprint = Encoding.UTF8.GetString(payload.Slice(pOff, fpLen));
        pOff += fpLen;
        var respLen = BinaryPrimitives.ReadInt32LittleEndian(payload[pOff..]);
        pOff += ResponseLengthPrefixSize;
        if (payload.Length < pOff + respLen)
            throw new InvalidDataException("idempotency outcome response bytes are truncated.");
        var responseBytes = frameBuffer.AsMemory(offset + pOff, respLen);

        return new JournalRecord
        {
            Sequence = seq,
            UnixMs = unixMs,
            Operation = JournalOperationKind.IdempotencyOutcome,
            Key = cacheKey,
            IdempotencyOperationId = operationId,
            IdempotencyFingerprint = fingerprint,
            IdempotencyResponseBytes = responseBytes,
        };
    }

    private static JournalRecord DecodeIdempotencyStarted(ulong seq, long unixMs, CacheKey cacheKey, ReadOnlySpan<byte> frameBody, int offset, int payloadLen)
    {
        var payload = frameBody.Slice(offset, payloadLen);
        var pOff = 0;
        if (payload.Length < OperationIdLengthPrefixSize)
            throw new InvalidDataException("idempotency started operation id length is missing.");
        var opIdLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[pOff..]);
        pOff += OperationIdLengthPrefixSize;
        if (payload.Length < pOff + opIdLen + FingerprintLengthPrefixSize)
            throw new InvalidDataException("idempotency started operation id or fingerprint length is truncated.");
        var operationId = Encoding.UTF8.GetString(payload.Slice(pOff, opIdLen));
        pOff += opIdLen;
        var fpLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[pOff..]);
        pOff += FingerprintLengthPrefixSize;
        if (payload.Length < pOff + fpLen)
            throw new InvalidDataException("idempotency started fingerprint is truncated.");
        var fingerprint = Encoding.UTF8.GetString(payload.Slice(pOff, fpLen));

        return new JournalRecord
        {
            Sequence = seq,
            UnixMs = unixMs,
            Operation = JournalOperationKind.IdempotencyStarted,
            Key = cacheKey,
            IdempotencyOperationId = operationId,
            IdempotencyFingerprint = fingerprint,
        };
    }

    /// <summary>Decodes a mutation frame carrying a write-ahead operation-id prefix.</summary>
    /// <param name="opcode">The mutation opcode with an operation-id prefix.</param>
    /// <param name="seq">The monotonic journal sequence number.</param>
    /// <param name="unixMs">The operation timestamp in Unix milliseconds.</param>
    /// <param name="cacheKey">The cache key for the operation.</param>
    /// <param name="frameBuffer">The frame bytes containing the payload.</param>
    /// <param name="offset">The payload offset within the frame.</param>
    /// <param name="payloadLen">The payload length in bytes.</param>
    /// <returns>The decoded journal record stamped with the mutation operation id.</returns>
    /// <exception cref="InvalidDataException">When the operation-id prefix is truncated.</exception>
    private static JournalRecord DecodeMutationPrefixed(JournalOpcode opcode, ulong seq, long unixMs, CacheKey cacheKey, byte[] frameBuffer, int offset, int payloadLen)
    {
        var mutationOperationId = MutationOperationIdCodec.DecodeMutationOperationId(frameBuffer.AsSpan(offset, payloadLen));
        return opcode switch
        {
            JournalOpcode.PutWithMutationOperationId => DecodePut(seq, unixMs, cacheKey, frameBuffer, offset, payloadLen, mutationOperationId),
            JournalOpcode.RemoveWithMutationOperationId => DecodeMutationWithoutPayload(seq, unixMs, cacheKey, JournalOperationKind.Remove, payloadLen, mutationOperationId),
            JournalOpcode.RemoveExpirationWithMutationOperationId => DecodeMutationWithoutPayload(
                seq,
                unixMs,
                cacheKey,
                JournalOperationKind.RemoveExpiration,
                payloadLen,
                mutationOperationId),
            JournalOpcode.TouchExpirationWithMutationOperationId => DecodeTouchExpiration(seq, unixMs, cacheKey, frameBuffer, offset, payloadLen, mutationOperationId),
            JournalOpcode.Put => throw new InvalidDataException(UnknownJournalOpcodeMessage),
            JournalOpcode.Remove => throw new InvalidDataException(UnknownJournalOpcodeMessage),
            JournalOpcode.RemoveExpiration => throw new InvalidDataException(UnknownJournalOpcodeMessage),
            JournalOpcode.TouchExpiration => throw new InvalidDataException(UnknownJournalOpcodeMessage),
            JournalOpcode.IdempotencyOutcome => throw new InvalidDataException(UnknownJournalOpcodeMessage),
            JournalOpcode.IdempotencyStarted => throw new InvalidDataException(UnknownJournalOpcodeMessage),
            _ => throw new InvalidDataException(UnknownJournalOpcodeMessage),
        };
    }

    private static JournalRecord DecodeMutationWithoutPayload(
        ulong seq,
        long unixMs,
        CacheKey cacheKey,
        JournalOperationKind operation,
        int payloadLen,
        string? mutationOperationId)
    {
        var prefixLength = MutationOperationIdCodec.EncodeMutationOperationIdPrefixLength(mutationOperationId);
        return payloadLen < prefixLength ? throw new InvalidDataException("mutation frame payload is truncated.") : new JournalRecord
        {
            Sequence = seq,
            UnixMs = unixMs,
            Operation = operation,
            Key = cacheKey,
            MutationOperationId = mutationOperationId,
        };
    }

    private static JournalRecord DecodePut(ulong seq, long unixMs, CacheKey cacheKey, byte[] frameBuffer, int offset, int payloadLen, string? mutationOperationId)
    {
        var prefixLength = MutationOperationIdCodec.EncodeMutationOperationIdPrefixLength(mutationOperationId);
        var entryStart = offset + prefixLength;
        var entryLength = payloadLen - prefixLength;
        return (entryLength < 0, frameBuffer.Length < entryStart + entryLength) switch
        {
            (true, _) => throw new InvalidDataException("binary journal put frame has invalid payload length."),
            (false, true) => throw new InvalidDataException("binary journal put frame is truncated."),
            (false, false) => new JournalRecord
            {
                Sequence = seq,
                UnixMs = unixMs,
                Operation = JournalOperationKind.Put,
                Key = cacheKey,
                MutationOperationId = mutationOperationId,
                PutEntryBytes = entryLength > 0 ? frameBuffer.AsMemory(entryStart, entryLength) : ReadOnlyMemory<byte>.Empty,
            },
        };
    }

    private static JournalRecord DecodeTouchExpiration(ulong seq, long unixMs, CacheKey cacheKey, byte[] frameBuffer, int offset, int payloadLen, string? mutationOperationId)
    {
        var prefixLength = MutationOperationIdCodec.EncodeMutationOperationIdPrefixLength(mutationOperationId);
        var expiresOffset = offset + prefixLength;
        var expiresLength = payloadLen - prefixLength;
        return expiresLength != TimestampSize ? throw new InvalidDataException("touch expiration frame payload is truncated.") : new JournalRecord
        {
            Sequence = seq,
            UnixMs = unixMs,
            Operation = JournalOperationKind.TouchExpiration,
            Key = cacheKey,
            MutationOperationId = mutationOperationId,
            TouchExpirationUtc = DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(frameBuffer.AsSpan(expiresOffset, TimestampSize))).UtcDateTime,
        };
    }

    private static JournalRecord DispatchDecode(byte[] frameBuffer, ReadOnlySpan<byte> frameBody, FrameHeader header, CacheKey cacheKey, int offset)
    {
        var payloadLen = header.PayloadLength;
        return header.Opcode switch
        {
            JournalOpcode.Put => DecodePut(header.Sequence, header.UnixMs, cacheKey, frameBuffer, offset, payloadLen, null),
            JournalOpcode.PutWithMutationOperationId or JournalOpcode.RemoveWithMutationOperationId or JournalOpcode.RemoveExpirationWithMutationOperationId
                or JournalOpcode.TouchExpirationWithMutationOperationId => DecodeMutationPrefixed(
                    header.Opcode,
                    header.Sequence,
                    header.UnixMs,
                    cacheKey,
                    frameBuffer,
                    offset,
                    payloadLen),
            JournalOpcode.Remove => DecodeMutationWithoutPayload(header.Sequence, header.UnixMs, cacheKey, JournalOperationKind.Remove, payloadLen, null),
            JournalOpcode.RemoveExpiration => DecodeMutationWithoutPayload(header.Sequence, header.UnixMs, cacheKey, JournalOperationKind.RemoveExpiration, payloadLen, null),
            JournalOpcode.TouchExpiration => DecodeTouchExpiration(header.Sequence, header.UnixMs, cacheKey, frameBuffer, offset, payloadLen, null),
            JournalOpcode.IdempotencyOutcome => DecodeIdempotencyOutcome(header.Sequence, header.UnixMs, cacheKey, frameBuffer, frameBody, offset, payloadLen),
            JournalOpcode.IdempotencyStarted => DecodeIdempotencyStarted(header.Sequence, header.UnixMs, cacheKey, frameBody, offset, payloadLen),
            _ => throw new InvalidDataException(UnknownJournalOpcodeMessage),
        };
    }

    private static int EncodeIdempotencyOutcome(JournalRecord record, Span<byte> destination, int offset)
    {
        var opId = record.IdempotencyOperationId!;
        var fingerprint = record.IdempotencyFingerprint!;
        var opIdLen = Encoding.UTF8.GetByteCount(opId);
        var fpLen = Encoding.UTF8.GetByteCount(fingerprint);
        var respBytes = record.IdempotencyResponseBytes.Span;

        if (opIdLen > ushort.MaxValue || fpLen > ushort.MaxValue)
            throw new InvalidDataException("Idempotency outcome operation id or fingerprint exceeds maximum encoded length.");

        BinaryPrimitives.WriteUInt16LittleEndian(destination[offset..], ushort.CreateTruncating(opIdLen));
        offset += OperationIdLengthPrefixSize;
        offset += Encoding.UTF8.GetBytes(opId, destination[offset..]);
        BinaryPrimitives.WriteUInt16LittleEndian(destination[offset..], ushort.CreateTruncating(fpLen));
        offset += FingerprintLengthPrefixSize;
        offset += Encoding.UTF8.GetBytes(fingerprint, destination[offset..]);
        BinaryPrimitives.WriteInt32LittleEndian(destination[offset..], respBytes.Length);
        offset += ResponseLengthPrefixSize;
        respBytes.CopyTo(destination[offset..]);
        return offset + respBytes.Length;
    }

    /// <summary>Encodes an idempotency outcome or started payload.</summary>
    /// <param name="record">The record to encode.</param>
    /// <param name="destination">The destination span.</param>
    /// <param name="offset">The payload offset within the destination.</param>
    /// <returns>The offset after the encoded payload.</returns>
    private static int EncodeIdempotencyPayload(JournalRecord record, Span<byte> destination, int offset) => record.Operation == JournalOperationKind.IdempotencyOutcome
        ? EncodeIdempotencyOutcome(record, destination, offset) : EncodeIdempotencyStarted(record, destination, offset);

    private static int EncodeIdempotencyStarted(JournalRecord record, Span<byte> destination, int offset)
    {
        var opId = record.IdempotencyOperationId!;
        var fingerprint = record.IdempotencyFingerprint ?? string.Empty;
        var opIdLen = Encoding.UTF8.GetByteCount(opId);
        var fpLen = Encoding.UTF8.GetByteCount(fingerprint);

        if (opIdLen > ushort.MaxValue || fpLen > ushort.MaxValue)
            throw new InvalidDataException("Idempotency started operation id or fingerprint exceeds maximum encoded length.");

        BinaryPrimitives.WriteUInt16LittleEndian(destination[offset..], ushort.CreateTruncating(opIdLen));
        offset += OperationIdLengthPrefixSize;
        offset += Encoding.UTF8.GetBytes(opId, destination[offset..]);
        BinaryPrimitives.WriteUInt16LittleEndian(destination[offset..], ushort.CreateTruncating(fpLen));
        offset += FingerprintLengthPrefixSize;
        offset += Encoding.UTF8.GetBytes(fingerprint, destination[offset..]);
        return offset;
    }

    private static CacheKey ReadCacheKey(ReadOnlySpan<byte> frameBody, FrameHeader header, out int offset)
    {
        offset = FixedPrefixSize;
        var ns = Encoding.UTF8.GetString(frameBody.Slice(offset, header.NamespaceLength));
        offset += header.NamespaceLength;
        var key = Encoding.UTF8.GetString(frameBody.Slice(offset, header.KeyLength));
        offset += header.KeyLength;
        return new CacheKey(ns, key);
    }

    private static FrameHeader ReadFrameHeader(ReadOnlySpan<byte> frameBody)
    {
        var seq = BinaryPrimitives.ReadUInt64LittleEndian(frameBody);
        var unixMs = BinaryPrimitives.ReadInt64LittleEndian(frameBody[UnixMsOffset..]);
        var opcode = JournalOpcodeWire.FromByte(frameBody[OpcodeOffset]);
        var nsLen = BinaryPrimitives.ReadUInt16LittleEndian(frameBody[NamespaceLengthOffset..]);
        var keyLen = BinaryPrimitives.ReadUInt16LittleEndian(frameBody[KeyLengthOffset..]);
        var payloadLen = BinaryPrimitives.ReadInt32LittleEndian(frameBody[PayloadLengthOffset..]);
        return new FrameHeader(seq, unixMs, opcode, nsLen, keyLen, payloadLen);
    }

    private static void ThrowIfPayloadTruncated(ReadOnlySpan<byte> frameBody, int offset, int payloadLen)
    {
        // Helpers slice the full frame buffer: reject a payload length that overruns the bounded
        // frame body first, so truncated frames cannot silently consume padded trailing bytes.
        if (payloadLen < 0 || payloadLen > frameBody.Length - offset)
            throw new InvalidDataException("binary journal frame payload is truncated.");
    }

    private static JournalOpcode ToOpcode(JournalRecord record)
    {
        var operation = record.Operation;
        var hasOperationId = record.MutationOperationId != null;
        return operation switch
        {
            JournalOperationKind.Put => hasOperationId ? JournalOpcode.PutWithMutationOperationId : JournalOpcode.Put,
            JournalOperationKind.Remove => hasOperationId ? JournalOpcode.RemoveWithMutationOperationId : JournalOpcode.Remove,
            JournalOperationKind.RemoveExpiration => hasOperationId ? JournalOpcode.RemoveExpirationWithMutationOperationId : JournalOpcode.RemoveExpiration,
            JournalOperationKind.TouchExpiration => hasOperationId ? JournalOpcode.TouchExpirationWithMutationOperationId : JournalOpcode.TouchExpiration,
            JournalOperationKind.IdempotencyOutcome => JournalOpcode.IdempotencyOutcome,
            JournalOperationKind.IdempotencyStarted => JournalOpcode.IdempotencyStarted,
            JournalOperationKind.AwaitDurabilityCommit => throw CreateOperationNotEncodableException(),
            JournalOperationKind.WaitForStartup => throw CreateOperationNotEncodableException(),
            JournalOperationKind.MaintenanceExclusive => throw CreateOperationNotEncodableException(),
            JournalOperationKind.SnapshotCut => throw CreateOperationNotEncodableException(),
            JournalOperationKind.UnderSnapshotBarrier => throw CreateOperationNotEncodableException(),
            _ => throw CreateOperationNotEncodableException(),
        };
    }

    private static void WriteFixedPrefix(Span<byte> destination, JournalRecord record, int nsLen, int keyLen, int payloadLen)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(destination, record.Sequence);
        BinaryPrimitives.WriteInt64LittleEndian(destination[UnixMsOffset..], record.UnixMs);
        destination[OpcodeOffset] = JournalOpcodeWire.ToWireValue(ToOpcode(record));
        BinaryPrimitives.WriteUInt16LittleEndian(destination[NamespaceLengthOffset..], Convert.ToUInt16(nsLen));
        BinaryPrimitives.WriteUInt16LittleEndian(destination[KeyLengthOffset..], Convert.ToUInt16(keyLen));
        BinaryPrimitives.WriteInt32LittleEndian(destination[PayloadLengthOffset..], payloadLen);
    }

    private static int WriteOperationPayload(JournalRecord record, Span<byte> destination, int offset) => record.Operation switch
    {
        JournalOperationKind.IdempotencyOutcome or JournalOperationKind.IdempotencyStarted => EncodeIdempotencyPayload(record, destination, offset),
        JournalOperationKind.Put => WritePutPayload(record, destination, offset),
        JournalOperationKind.Remove or JournalOperationKind.RemoveExpiration => MutationOperationIdCodec.EncodeMutationOperationIdPrefix(
            record.MutationOperationId,
            destination,
            offset),
        JournalOperationKind.TouchExpiration => WriteTouchExpirationPayload(record, destination, offset),
        JournalOperationKind.AwaitDurabilityCommit or JournalOperationKind.WaitForStartup or JournalOperationKind.MaintenanceExclusive
            or JournalOperationKind.SnapshotCut or JournalOperationKind.UnderSnapshotBarrier => throw CreateOperationNotEncodableException(),
        _ => throw CreateOperationNotEncodableException(),
    };

    /// <summary>Encodes a put mutation payload including the optional write-ahead operation-id prefix.</summary>
    /// <param name="record">The record to encode.</param>
    /// <param name="destination">The destination span.</param>
    /// <param name="offset">The payload offset within the destination.</param>
    /// <returns>The offset after the encoded payload.</returns>
    private static int WritePutPayload(JournalRecord record, Span<byte> destination, int offset)
    {
        offset = MutationOperationIdCodec.EncodeMutationOperationIdPrefix(record.MutationOperationId, destination, offset);
        record.PutEntryBytes.Span.CopyTo(destination[offset..]);
        return offset + record.PutEntryBytes.Length;
    }

    /// <summary>Encodes a touch-expiration mutation payload including the optional write-ahead operation-id prefix.</summary>
    /// <param name="record">The record to encode.</param>
    /// <param name="destination">The destination span.</param>
    /// <param name="offset">The payload offset within the destination.</param>
    /// <returns>The offset after the encoded payload.</returns>
    private static int WriteTouchExpirationPayload(JournalRecord record, Span<byte> destination, int offset)
    {
        offset = MutationOperationIdCodec.EncodeMutationOperationIdPrefix(record.MutationOperationId, destination, offset);
        var expiresMs = record.TouchExpirationUtc is { } utc ? new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc)).ToUnixTimeMilliseconds() : MissingExpirationUnixMs;
        BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], expiresMs);
        return offset + TimestampSize;
    }

    [Immutable]
    private sealed record FrameHeader(ulong Sequence, long UnixMs, JournalOpcode Opcode, int NamespaceLength, int KeyLength, int PayloadLength);

    private static class JournalOpcodeWire
    {
        private const byte IdempotencyOutcomeWire = 5;
        private const byte IdempotencyStartedWire = 10;
        private const byte PutWire = 1;
        private const byte PutWithMutationOpIdWire = 6;
        private const byte RemoveExpirationWire = 3;
        private const byte RemoveExpirationWithMutationOpIdWire = 8;
        private const byte RemoveWire = 2;
        private const byte RemoveWithMutationOpIdWire = 7;
        private const byte TouchExpirationWire = 4;
        private const byte TouchExpirationWithMutationOpIdWire = 9;

        internal static JournalOpcode FromByte(byte value) => value switch
        {
            PutWire => JournalOpcode.Put,
            RemoveWire => JournalOpcode.Remove,
            RemoveExpirationWire => JournalOpcode.RemoveExpiration,
            TouchExpirationWire => JournalOpcode.TouchExpiration,
            IdempotencyOutcomeWire => JournalOpcode.IdempotencyOutcome,
            PutWithMutationOpIdWire => JournalOpcode.PutWithMutationOperationId,
            RemoveWithMutationOpIdWire => JournalOpcode.RemoveWithMutationOperationId,
            RemoveExpirationWithMutationOpIdWire => JournalOpcode.RemoveExpirationWithMutationOperationId,
            TouchExpirationWithMutationOpIdWire => JournalOpcode.TouchExpirationWithMutationOperationId,
            IdempotencyStartedWire => JournalOpcode.IdempotencyStarted,
            _ => throw new InvalidDataException(UnknownJournalOpcodeMessage),
        };

        internal static byte ToWireValue(JournalOpcode opcode) => opcode switch
        {
            JournalOpcode.Put => PutWire,
            JournalOpcode.Remove => RemoveWire,
            JournalOpcode.RemoveExpiration => RemoveExpirationWire,
            JournalOpcode.TouchExpiration => TouchExpirationWire,
            JournalOpcode.IdempotencyOutcome => IdempotencyOutcomeWire,
            JournalOpcode.PutWithMutationOperationId => PutWithMutationOpIdWire,
            JournalOpcode.RemoveWithMutationOperationId => RemoveWithMutationOpIdWire,
            JournalOpcode.RemoveExpirationWithMutationOperationId => RemoveExpirationWithMutationOpIdWire,
            JournalOpcode.TouchExpirationWithMutationOperationId => TouchExpirationWithMutationOpIdWire,
            JournalOpcode.IdempotencyStarted => IdempotencyStartedWire,
            _ => throw new InvalidDataException(UnknownJournalOpcodeMessage),
        };
    }
}
