using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.JsonFramed;

namespace Squirix.Server.Storage.Journaling.Pipelined.Codec;

/// <summary>Binary frame body codec for Pipelined journal (SJRN v1 file header).</summary>
internal sealed class BinaryJournalCodec : IJournalFrameCodec
{
    private const int FixedPrefixSize = 8 + 8 + 1 + 2 + 2 + 2 + 4;

    public static BinaryJournalCodec Instance { get; } = new();

    public byte FileVersion => JournalBinaryFraming.FileVersion;

    public static int ComputeFrameBodyLength(JournalRecord record)
    {
        var nsLen = Encoding.UTF8.GetByteCount(record.Key.Namespace);
        var keyLen = Encoding.UTF8.GetByteCount(record.Key.Key);
        var extra = record.Operation switch
        {
            JournalOperationKind.Put => (record.PutDiscriminatedEntryJson?.Length ?? 0) + Encoding.UTF8.GetByteCount(record.PutOperationId ?? string.Empty),
            JournalOperationKind.TouchExpiration => 8,
            _ => 0,
        };
        return FixedPrefixSize + nsLen + keyLen + extra;
    }

    public JournalRecord Decode(ReadOnlySpan<byte> frameBody)
    {
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
                PutDiscriminatedEntryJson = frameBody.Slice(offset, payloadLen).ToArray(),
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

    public int Encode(JournalRecord record, Span<byte> destination)
    {
        var ns = record.Key.Namespace;
        var key = record.Key.Key;
        var nsLen = Encoding.UTF8.GetByteCount(ns);
        var keyLen = Encoding.UTF8.GetByteCount(key);
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

    private static int EncodePut(JournalRecord record, Span<byte> destination, int offset, int opIdLen)
    {
        var payload = record.PutDiscriminatedEntryJson ?? [];
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
                payloadLen = record.PutDiscriminatedEntryJson?.Length ?? 0;
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
                break;
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
}
