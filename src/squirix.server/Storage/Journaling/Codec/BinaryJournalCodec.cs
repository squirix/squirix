using System;
using System.Buffers.Binary;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Observability;

namespace Squirix.Server.Storage.Journaling.Codec;

/// <summary>Binary frame body codec for Pipelined journal (SJRN v1 file header).</summary>
internal static class BinaryJournalCodec
{
    private const int FixedPrefixSize = 8 + 8 + 1 + 2 + 2 + 4;

    public static int ComputeFrameBodyLength(JournalRecord record) => EncodeContext.From(record).BodyLength;

    public static EncodeContext PrepareEncode(JournalRecord record) => EncodeContext.From(record);

    public static int Encode(JournalRecord record, Span<byte> destination, in EncodeContext context)
    {
        var ns = record.Key.Namespace;
        var key = record.Key.Key;
        var keyUtf8 = context.KeyUtf8;
        var nsLen = keyUtf8.NamespaceLength;
        var keyLen = keyUtf8.KeyLength;
        var payloadLen = context.PayloadUtf8Length;

        BinaryPrimitives.WriteUInt64LittleEndian(destination, record.Sequence);
        BinaryPrimitives.WriteInt64LittleEndian(destination[8..], record.UnixMs);
        destination[16] = JournalOpcodeWire.ToWireValue(ToOpcode(record.Operation));
        BinaryPrimitives.WriteUInt16LittleEndian(destination[17..], Convert.ToUInt16(nsLen));
        BinaryPrimitives.WriteUInt16LittleEndian(destination[19..], Convert.ToUInt16(keyLen));
        BinaryPrimitives.WriteInt32LittleEndian(destination[21..], payloadLen);

        var offset = FixedPrefixSize;
        offset += Encoding.UTF8.GetBytes(ns, destination[offset..]);
        offset += Encoding.UTF8.GetBytes(key, destination[offset..]);

        switch (record.Operation)
        {
            case JournalOperationKind.Put:
                record.PutEntryBytes.Span.CopyTo(destination[offset..]);
                return offset + record.PutEntryBytes.Length;

            case JournalOperationKind.Remove:
            case JournalOperationKind.RemoveExpiration:
                return offset;

            case JournalOperationKind.TouchExpiration:
                var expiresMs = record.TouchExpirationUtc is { } utc ? new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc)).ToUnixTimeMilliseconds() : 0L;
                BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], expiresMs);
                return offset + 8;

            case JournalOperationKind.IdempotencyOutcome:
                return EncodeIdempotencyOutcome(record, destination, offset);

            case JournalOperationKind.AwaitDurabilityCommit:
            case JournalOperationKind.WaitForStartup:
            case JournalOperationKind.MaintenanceExclusive:
            case JournalOperationKind.SnapshotCut:
            case JournalOperationKind.UnderSnapshotBarrier:
            default:
                throw new NotSupportedException($"journal operation {record.Operation} cannot be encoded.");
        }
    }

    public static int Encode(JournalRecord record, Span<byte> destination)
    {
        var context = EncodeContext.From(record);
        return Encode(record, destination, in context);
    }

    public static JournalRecord Decode(byte[] frameBuffer, int frameLength)
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

        return opcode switch
        {
            JournalOpcode.Put => DecodePut(seq, unixMs, cacheKey, frameBuffer, offset, payloadLen),
            JournalOpcode.Remove => new JournalRecord
            {
                Sequence = seq,
                UnixMs = unixMs,
                Operation = JournalOperationKind.Remove,
                Key = cacheKey,
            },
            JournalOpcode.RemoveExpiration => new JournalRecord
            {
                Sequence = seq,
                UnixMs = unixMs,
                Operation = JournalOperationKind.RemoveExpiration,
                Key = cacheKey,
            },
            JournalOpcode.TouchExpiration => new JournalRecord
            {
                Sequence = seq,
                UnixMs = unixMs,
                Operation = JournalOperationKind.TouchExpiration,
                Key = cacheKey,
                TouchExpirationUtc = DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(frameBody.Slice(offset, 8))).UtcDateTime,
            },
            JournalOpcode.IdempotencyOutcome => DecodeIdempotencyOutcome(seq, unixMs, cacheKey, frameBuffer, frameBody, offset, payloadLen),
            _ => throw new InvalidDataException($"unknown journal opcode {Enum.GetName(opcode)}."),
        };
    }

    private static JournalRecord DecodePut(ulong seq, long unixMs, CacheKey cacheKey, byte[] frameBuffer, int offset, int payloadLen)
    {
        if (payloadLen < 0)
            throw new InvalidDataException("binary journal put frame has invalid payload length.");

        if (frameBuffer.Length < offset + payloadLen)
            throw new InvalidDataException("binary journal put frame is truncated.");

        return new JournalRecord
        {
            Sequence = seq,
            UnixMs = unixMs,
            Operation = JournalOperationKind.Put,
            Key = cacheKey,
            PutEntryBytes = payloadLen > 0 ? frameBuffer.AsMemory(offset, payloadLen) : ReadOnlyMemory<byte>.Empty,
        };
    }

    private static int EncodeIdempotencyOutcome(JournalRecord record, Span<byte> destination, int offset)
    {
        var opId = record.IdempotencyOperationId!;
        var fingerprint = record.IdempotencyFingerprint!;
        var opIdLen = Encoding.UTF8.GetByteCount(opId);
        var fpLen = Encoding.UTF8.GetByteCount(fingerprint);
        var respBytes = record.IdempotencyResponseBytes.Span;

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

    private static JournalOpcode ToOpcode(JournalOperationKind operation)
    {
        return operation switch
        {
            JournalOperationKind.Put => JournalOpcode.Put,
            JournalOperationKind.Remove => JournalOpcode.Remove,
            JournalOperationKind.RemoveExpiration => JournalOpcode.RemoveExpiration,
            JournalOperationKind.TouchExpiration => JournalOpcode.TouchExpiration,
            JournalOperationKind.IdempotencyOutcome => JournalOpcode.IdempotencyOutcome,
            JournalOperationKind.AwaitDurabilityCommit => throw new NotSupportedException($"journal operation {operation} cannot be encoded."),
            JournalOperationKind.WaitForStartup => throw new NotSupportedException($"journal operation {operation} cannot be encoded."),
            JournalOperationKind.MaintenanceExclusive => throw new NotSupportedException($"journal operation {operation} cannot be encoded."),
            JournalOperationKind.SnapshotCut => throw new NotSupportedException($"journal operation {operation} cannot be encoded."),
            JournalOperationKind.UnderSnapshotBarrier => throw new NotSupportedException($"journal operation {operation} cannot be encoded."),
            _ => throw new NotSupportedException($"journal operation {operation} cannot be encoded."),
        };
    }

    [StructLayout(LayoutKind.Auto)]
    internal readonly struct EncodeContext
    {
        private EncodeContext(Utf8KeyLengths keyUtf8, int payloadUtf8Length)
        {
            KeyUtf8 = keyUtf8;
            PayloadUtf8Length = payloadUtf8Length;
        }

        public Utf8KeyLengths KeyUtf8 { get; }

        public int PayloadUtf8Length { get; }

        public int BodyLength => FixedPrefixSize + KeyUtf8.TotalLength + PayloadUtf8Length;

        public static EncodeContext From(JournalRecord record)
        {
            var keyUtf8 = Utf8KeyLengths.From(record.Key);
            var payloadUtf8Length = GetOperationPayloadLength(record);
            return new EncodeContext(keyUtf8, payloadUtf8Length);
        }

        private static int GetOperationPayloadLength(JournalRecord record)
        {
            switch (record.Operation)
            {
                case JournalOperationKind.Put:
                    return record.PutEntryBytes.Length;

                case JournalOperationKind.TouchExpiration:
                    return 8;

                case JournalOperationKind.Remove:
                case JournalOperationKind.RemoveExpiration:
                    return 0;

                case JournalOperationKind.IdempotencyOutcome:
                    return 2 + Encoding.UTF8.GetByteCount(record.IdempotencyOperationId ?? string.Empty)
                           + 2 + Encoding.UTF8.GetByteCount(record.IdempotencyFingerprint ?? string.Empty)
                           + 4 + record.IdempotencyResponseBytes.Length;

                case JournalOperationKind.AwaitDurabilityCommit:
                case JournalOperationKind.WaitForStartup:
                case JournalOperationKind.MaintenanceExclusive:
                case JournalOperationKind.SnapshotCut:
                case JournalOperationKind.UnderSnapshotBarrier:
                default:
                    throw new NotSupportedException($"the length of operation {record.Operation} cannot be determined.");
            }
        }
    }

    [StructLayout(LayoutKind.Auto)]
    internal readonly struct Utf8KeyLengths
    {
        private Utf8KeyLengths(int namespaceLength, int keyLength)
        {
            NamespaceLength = namespaceLength;
            KeyLength = keyLength;
        }

        public int KeyLength { get; }

        public int NamespaceLength { get; }

        public int TotalLength => NamespaceLength + KeyLength;

        public static Utf8KeyLengths From(CacheKey key) => new(Encoding.UTF8.GetByteCount(key.Namespace), Encoding.UTF8.GetByteCount(key.Key));
    }
}
