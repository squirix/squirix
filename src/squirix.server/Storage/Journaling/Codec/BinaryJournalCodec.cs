using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Storage.Journaling.Codec;

/// <summary>Binary frame body codec for Pipelined journal (SJRN v1 file header).</summary>
internal static class BinaryJournalCodec
{
    internal const int FixedPrefixSize = 8 + 8 + 1 + 2 + 2 + 4;

    private const string UnknownJournalOpcodeMessage = "Unknown journal opcode.";

    internal static int ComputeFrameBodyLength(JournalRecord record) => EncodeContext.From(record).BodyLength;

    internal static JournalRecord Decode(byte[] frameBuffer, int frameLength)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(frameLength, frameBuffer.Length);

        var frameBody = frameBuffer.AsSpan(0, frameLength);
        if (frameBody.Length < FixedPrefixSize)
            throw new InvalidDataException("binary journal frame body is truncated.");

        var seq = BinaryPrimitives.ReadUInt64LittleEndian(frameBody);
        var unixMs = BinaryPrimitives.ReadInt64LittleEndian(frameBody[8..]);
        var opcode = JournalOpcodeWire.FromByte(frameBody[16]);
        var nsLen = BinaryPrimitives.ReadUInt16LittleEndian(frameBody[17..]);
        var keyLen = BinaryPrimitives.ReadUInt16LittleEndian(frameBody[19..]);
        var payloadLen = BinaryPrimitives.ReadInt32LittleEndian(frameBody[21..]);
        var offset = FixedPrefixSize;
        var ns = Encoding.UTF8.GetString(frameBody.Slice(offset, nsLen));
        offset += nsLen;
        var key = Encoding.UTF8.GetString(frameBody.Slice(offset, keyLen));
        offset += keyLen;
        var cacheKey = new CacheKey(ns, key);

        // Helpers slice the full frame buffer: reject a payload length that overruns the bounded
        // frame body first, so truncated frames cannot silently consume padded trailing bytes.
        if (payloadLen < 0 || payloadLen > frameBody.Length - offset)
            throw new InvalidDataException("binary journal frame payload is truncated.");

        return opcode switch
        {
            JournalOpcode.Put => DecodePut(seq, unixMs, cacheKey, frameBuffer, offset, payloadLen, null),
            JournalOpcode.PutWithMutationOperationId or JournalOpcode.RemoveWithMutationOperationId or JournalOpcode.RemoveExpirationWithMutationOperationId
                or JournalOpcode.TouchExpirationWithMutationOperationId => DecodeMutationPrefixed(opcode, seq, unixMs, cacheKey, frameBuffer, offset, payloadLen),
            JournalOpcode.Remove => DecodeMutationWithoutPayload(seq, unixMs, cacheKey, JournalOperationKind.Remove, payloadLen, null),
            JournalOpcode.RemoveExpiration => DecodeMutationWithoutPayload(seq, unixMs, cacheKey, JournalOperationKind.RemoveExpiration, payloadLen, null),
            JournalOpcode.TouchExpiration => DecodeTouchExpiration(seq, unixMs, cacheKey, frameBuffer, offset, payloadLen, null),
            JournalOpcode.IdempotencyOutcome => DecodeIdempotencyOutcome(seq, unixMs, cacheKey, frameBuffer, frameBody, offset, payloadLen),
            JournalOpcode.IdempotencyStarted => DecodeIdempotencyStarted(seq, unixMs, cacheKey, frameBody, offset, payloadLen),
            _ => throw new InvalidDataException(UnknownJournalOpcodeMessage),
        };
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
        if (payload.Length < 2)
            throw new InvalidDataException("idempotency outcome operation id length is missing.");
        var opIdLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[pOff..]);
        pOff += 2;
        if (payload.Length < pOff + opIdLen + 2)
            throw new InvalidDataException("idempotency outcome operation id or fingerprint length is truncated.");
        var operationId = Encoding.UTF8.GetString(payload.Slice(pOff, opIdLen));
        pOff += opIdLen;
        var fpLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[pOff..]);
        pOff += 2;
        if (payload.Length < pOff + fpLen + 4)
            throw new InvalidDataException("idempotency outcome fingerprint or response length is truncated.");
        var fingerprint = Encoding.UTF8.GetString(payload.Slice(pOff, fpLen));
        pOff += fpLen;
        var respLen = BinaryPrimitives.ReadInt32LittleEndian(payload[pOff..]);
        pOff += 4;
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
        if (payload.Length < 2)
            throw new InvalidDataException("idempotency started operation id length is missing.");
        var opIdLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[pOff..]);
        pOff += 2;
        if (payload.Length < pOff + opIdLen + 2)
            throw new InvalidDataException("idempotency started operation id or fingerprint length is truncated.");
        var operationId = Encoding.UTF8.GetString(payload.Slice(pOff, opIdLen));
        pOff += opIdLen;
        var fpLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[pOff..]);
        pOff += 2;
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
        if (payloadLen < prefixLength)
            throw new InvalidDataException("mutation frame payload is truncated.");

        return new JournalRecord
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
        if (entryLength < 0)
            throw new InvalidDataException("binary journal put frame has invalid payload length.");

        if (frameBuffer.Length < entryStart + entryLength)
            throw new InvalidDataException("binary journal put frame is truncated.");

        return new JournalRecord
        {
            Sequence = seq,
            UnixMs = unixMs,
            Operation = JournalOperationKind.Put,
            Key = cacheKey,
            MutationOperationId = mutationOperationId,
            PutEntryBytes = entryLength > 0 ? frameBuffer.AsMemory(entryStart, entryLength) : ReadOnlyMemory<byte>.Empty,
        };
    }

    private static JournalRecord DecodeTouchExpiration(ulong seq, long unixMs, CacheKey cacheKey, byte[] frameBuffer, int offset, int payloadLen, string? mutationOperationId)
    {
        var prefixLength = MutationOperationIdCodec.EncodeMutationOperationIdPrefixLength(mutationOperationId);
        var expiresOffset = offset + prefixLength;
        var expiresLength = payloadLen - prefixLength;
        if (expiresLength != 8)
            throw new InvalidDataException("touch expiration frame payload is truncated.");

        return new JournalRecord
        {
            Sequence = seq,
            UnixMs = unixMs,
            Operation = JournalOperationKind.TouchExpiration,
            Key = cacheKey,
            MutationOperationId = mutationOperationId,
            TouchExpirationUtc = DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(frameBuffer.AsSpan(expiresOffset, 8))).UtcDateTime,
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
        offset += 2;
        offset += Encoding.UTF8.GetBytes(opId, destination[offset..]);
        BinaryPrimitives.WriteUInt16LittleEndian(destination[offset..], ushort.CreateTruncating(fpLen));
        offset += 2;
        offset += Encoding.UTF8.GetBytes(fingerprint, destination[offset..]);
        BinaryPrimitives.WriteInt32LittleEndian(destination[offset..], respBytes.Length);
        offset += 4;
        respBytes.CopyTo(destination[offset..]);
        return offset + respBytes.Length;
    }

    /// <summary>Encodes an idempotency outcome or started payload.</summary>
    /// <param name="record">The record to encode.</param>
    /// <param name="destination">The destination span.</param>
    /// <param name="offset">The payload offset within the destination.</param>
    /// <returns>The offset after the encoded payload.</returns>
    private static int EncodeIdempotencyPayload(JournalRecord record, Span<byte> destination, int offset)
    {
        if (record.Operation == JournalOperationKind.IdempotencyOutcome)
            return EncodeIdempotencyOutcome(record, destination, offset);

        return EncodeIdempotencyStarted(record, destination, offset);
    }

    private static int EncodeIdempotencyStarted(JournalRecord record, Span<byte> destination, int offset)
    {
        var opId = record.IdempotencyOperationId!;
        var fingerprint = record.IdempotencyFingerprint ?? string.Empty;
        var opIdLen = Encoding.UTF8.GetByteCount(opId);
        var fpLen = Encoding.UTF8.GetByteCount(fingerprint);

        if (opIdLen > ushort.MaxValue || fpLen > ushort.MaxValue)
            throw new InvalidDataException("Idempotency started operation id or fingerprint exceeds maximum encoded length.");

        BinaryPrimitives.WriteUInt16LittleEndian(destination[offset..], ushort.CreateTruncating(opIdLen));
        offset += 2;
        offset += Encoding.UTF8.GetBytes(opId, destination[offset..]);
        BinaryPrimitives.WriteUInt16LittleEndian(destination[offset..], ushort.CreateTruncating(fpLen));
        offset += 2;
        offset += Encoding.UTF8.GetBytes(fingerprint, destination[offset..]);
        return offset;
    }

    /// <summary>Determines whether <paramref name="operation" /> is an idempotency outcome or started record.</summary>
    /// <param name="operation">The journal operation kind.</param>
    /// <returns><see langword="true" /> for idempotency records; otherwise <see langword="false" />.</returns>
    private static bool IsIdempotencyOperation(JournalOperationKind operation) =>
        operation == JournalOperationKind.IdempotencyOutcome || operation == JournalOperationKind.IdempotencyStarted;

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
        BinaryPrimitives.WriteInt64LittleEndian(destination[8..], record.UnixMs);
        destination[16] = JournalOpcodeWire.ToWireValue(ToOpcode(record));
        BinaryPrimitives.WriteUInt16LittleEndian(destination[17..], Convert.ToUInt16(nsLen));
        BinaryPrimitives.WriteUInt16LittleEndian(destination[19..], Convert.ToUInt16(keyLen));
        BinaryPrimitives.WriteInt32LittleEndian(destination[21..], payloadLen);
    }

    private static int WriteOperationPayload(JournalRecord record, Span<byte> destination, int offset) => IsIdempotencyOperation(record.Operation)
        ? EncodeIdempotencyPayload(record, destination, offset) : record.Operation switch
        {
            JournalOperationKind.Put => WritePutPayload(record, destination, offset),
            JournalOperationKind.Remove or JournalOperationKind.RemoveExpiration => MutationOperationIdCodec.EncodeMutationOperationIdPrefix(record.MutationOperationId, destination, offset),
            JournalOperationKind.TouchExpiration => WriteTouchExpirationPayload(record, destination, offset),
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
        var expiresMs = record.TouchExpirationUtc is { } utc ? new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc)).ToUnixTimeMilliseconds() : 0L;
        BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], expiresMs);
        return offset + 8;
    }

    private static class JournalOpcodeWire
    {
        internal static JournalOpcode FromByte(byte value) => value switch
        {
            1 => JournalOpcode.Put,
            2 => JournalOpcode.Remove,
            3 => JournalOpcode.RemoveExpiration,
            4 => JournalOpcode.TouchExpiration,
            5 => JournalOpcode.IdempotencyOutcome,
            6 => JournalOpcode.PutWithMutationOperationId,
            7 => JournalOpcode.RemoveWithMutationOperationId,
            8 => JournalOpcode.RemoveExpirationWithMutationOperationId,
            9 => JournalOpcode.TouchExpirationWithMutationOperationId,
            10 => JournalOpcode.IdempotencyStarted,
            _ => throw new InvalidDataException(UnknownJournalOpcodeMessage),
        };

        internal static byte ToWireValue(JournalOpcode opcode) => opcode switch
        {
            JournalOpcode.Put => 1,
            JournalOpcode.Remove => 2,
            JournalOpcode.RemoveExpiration => 3,
            JournalOpcode.TouchExpiration => 4,
            JournalOpcode.IdempotencyOutcome => 5,
            JournalOpcode.PutWithMutationOperationId => 6,
            JournalOpcode.RemoveWithMutationOperationId => 7,
            JournalOpcode.RemoveExpirationWithMutationOperationId => 8,
            JournalOpcode.TouchExpirationWithMutationOperationId => 9,
            JournalOpcode.IdempotencyStarted => 10,
            _ => throw new InvalidDataException(UnknownJournalOpcodeMessage),
        };
    }
}
