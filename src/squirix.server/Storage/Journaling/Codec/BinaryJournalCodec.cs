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
    private const int FixedPrefixSize = 8 + 8 + 1 + 2 + 2 + 2 + 4;

    public static int ComputeFrameBodyLength(JournalRecord record) => ComputeFrameBodyLength(record, Utf8KeyLengths.From(record.Key));

    public static int Encode(JournalRecord record, Span<byte> destination, Utf8KeyLengths keyUtf8)
    {
        var ns = record.Key.Namespace;
        var key = record.Key.Key;
        var nsLen = keyUtf8.NamespaceLength;
        var keyLen = keyUtf8.KeyLength;
        GetPayloadLengths(record, out var opIdLen, out var payloadLen);

        BinaryPrimitives.WriteUInt64LittleEndian(destination, record.Sequence);
        BinaryPrimitives.WriteInt64LittleEndian(destination[8..], record.UnixMs);
        destination[16] = JournalOpcodeWire.ToWireValue(ToOpcode(record.Operation));
        BinaryPrimitives.WriteUInt16LittleEndian(destination[17..], Convert.ToUInt16(nsLen));
        BinaryPrimitives.WriteUInt16LittleEndian(destination[19..], Convert.ToUInt16(keyLen));
        BinaryPrimitives.WriteUInt16LittleEndian(destination[21..], Convert.ToUInt16(opIdLen));
        BinaryPrimitives.WriteInt32LittleEndian(destination[23..], payloadLen);

        var offset = FixedPrefixSize;
        offset += Encoding.UTF8.GetBytes(ns, destination[offset..]);
        offset += Encoding.UTF8.GetBytes(key, destination[offset..]);

        switch (record.Operation)
        {
            case JournalOperationKind.Put:
                return EncodePut(record, destination, offset, opIdLen);

            case JournalOperationKind.Remove:
            case JournalOperationKind.RemoveExpiration:
                return offset;

            case JournalOperationKind.TouchExpiration:
            {
                var expiresMs = record.TouchExpirationUtc is { } utc ? new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc)).ToUnixTimeMilliseconds() : 0L;
                BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], expiresMs);
                return offset + 8;
            }

            case JournalOperationKind.AwaitDurabilityCommit:
            case JournalOperationKind.WaitForStartup:
            case JournalOperationKind.MaintenanceExclusive:
            case JournalOperationKind.SnapshotCut:
            case JournalOperationKind.UnderSnapshotBarrier:
            default:
                throw new NotSupportedException($"journal operation {record.Operation} cannot be encoded.");
        }
    }

    public static (int BodyLength, Utf8KeyLengths KeyUtf8) PrepareEncode(JournalRecord record)
    {
        var keyUtf8 = Utf8KeyLengths.From(record.Key);
        return (ComputeFrameBodyLength(record, keyUtf8), keyUtf8);
    }

    public static int Encode(JournalRecord record, Span<byte> destination) =>
        Encode(record, destination, Utf8KeyLengths.From(record.Key));

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
        var opIdLen = BinaryPrimitives.ReadUInt16LittleEndian(frameBody[21..]);
        var payloadLen = BinaryPrimitives.ReadInt32LittleEndian(frameBody[23..]);
        var offset = FixedPrefixSize;
        var ns = Encoding.UTF8.GetString(frameBody.Slice(offset, nsLen));
        offset += nsLen;
        var key = Encoding.UTF8.GetString(frameBody.Slice(offset, keyLen));
        offset += keyLen;
        var cacheKey = new CacheKey(ns, key);

        return opcode switch
        {
            JournalOpcode.Put => new JournalRecord
            {
                Sequence = seq,
                UnixMs = unixMs,
                Operation = JournalOperationKind.Put,
                Key = cacheKey,
                PutEntryBytes = payloadLen > 0 ? frameBuffer.AsMemory(offset, payloadLen) : ReadOnlyMemory<byte>.Empty,
                PutOperationId = opIdLen > 0 ? Encoding.UTF8.GetString(frameBody.Slice(offset + payloadLen, opIdLen)) : string.Empty,
            },
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
            _ => throw new InvalidDataException($"unknown journal opcode {Enum.GetName(opcode)}."),
        };
    }

    private static int ComputeFrameBodyLength(JournalRecord record, Utf8KeyLengths keyUtf8)
    {
        var extra = record.Operation switch
        {
            JournalOperationKind.Put => record.PutEntryBytes.Length + Encoding.UTF8.GetByteCount(record.PutOperationId ?? string.Empty),
            JournalOperationKind.TouchExpiration => 8,
            _ => 0,
        };
        return FixedPrefixSize + keyUtf8.TotalLength + extra;
    }

    private static int EncodePut(JournalRecord record, Span<byte> destination, int offset, int opIdLen)
    {
        var payload = record.PutEntryBytes.Span;
        payload.CopyTo(destination[offset..]);
        offset += payload.Length;
        if (opIdLen > 0)
            offset += Encoding.UTF8.GetBytes(record.PutOperationId!, destination[offset..]);
        return offset;
    }

    private static void GetPayloadLengths(JournalRecord record, out int opIdLen, out int payloadLen)
    {
        opIdLen = 0;
        payloadLen = 0;
        switch (record.Operation)
        {
            case JournalOperationKind.Put:
                payloadLen = record.PutEntryBytes.Length;
                opIdLen = Encoding.UTF8.GetByteCount(record.PutOperationId ?? string.Empty);
                break;

            case JournalOperationKind.TouchExpiration:
                payloadLen = 8;
                break;

            case JournalOperationKind.Remove:
            case JournalOperationKind.RemoveExpiration:
            case JournalOperationKind.AwaitDurabilityCommit:
            case JournalOperationKind.WaitForStartup:
            case JournalOperationKind.MaintenanceExclusive:
            case JournalOperationKind.SnapshotCut:
            case JournalOperationKind.UnderSnapshotBarrier:
            default:
                throw new NotSupportedException($"the length of operation {record.Operation} cannot be determined.");
        }
    }

    private static JournalOpcode ToOpcode(JournalOperationKind operation)
    {
        return operation switch
        {
            JournalOperationKind.Put => JournalOpcode.Put,
            JournalOperationKind.Remove => JournalOpcode.Remove,
            JournalOperationKind.RemoveExpiration => JournalOpcode.RemoveExpiration,
            JournalOperationKind.TouchExpiration => JournalOpcode.TouchExpiration,
            JournalOperationKind.AwaitDurabilityCommit => throw new NotSupportedException($"journal operation {operation} cannot be encoded."),
            JournalOperationKind.WaitForStartup => throw new NotSupportedException($"journal operation {operation} cannot be encoded."),
            JournalOperationKind.MaintenanceExclusive => throw new NotSupportedException($"journal operation {operation} cannot be encoded."),
            JournalOperationKind.SnapshotCut => throw new NotSupportedException($"journal operation {operation} cannot be encoded."),
            JournalOperationKind.UnderSnapshotBarrier => throw new NotSupportedException($"journal operation {operation} cannot be encoded."),
            _ => throw new NotSupportedException($"journal operation {operation} cannot be encoded."),
        };
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
