using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using Squirix.Server.Node.Services;

namespace Squirix.Server.Storage.Snapshot.Binary;

internal static class IdempotencyCodec
{
    public static int ComputeEncodedLength(PersistedIdempotencyRecord record)
    {
        var operationIdBytes = Encoding.UTF8.GetByteCount(record.OperationId);
        var fingerprintBytes = Encoding.UTF8.GetByteCount(record.Fingerprint);
        var outcomeBytes = Encoding.UTF8.GetByteCount(record.Outcome.Kind);
        if (operationIdBytes > ushort.MaxValue || fingerprintBytes > ushort.MaxValue || outcomeBytes > ushort.MaxValue)
            throw new InvalidDataException("Snapshot idempotency field exceeds maximum encoded length.");

        return 2 + operationIdBytes + 2 + fingerprintBytes + 8 + 2 + outcomeBytes;
    }

    public static void Write(PersistedIdempotencyRecord record, Span<byte> destination)
    {
        var required = ComputeEncodedLength(record);
        if (destination.Length < required)
            throw new ArgumentException("Destination span is too small for the encoded idempotency record.", nameof(destination));

        var offset = 0;
        offset += WriteUtf8Prefixed(record.OperationId, destination[offset..]);
        offset += WriteUtf8Prefixed(record.Fingerprint, destination[offset..]);
        BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], new DateTimeOffset(record.CreatedUtc.ToUniversalTime()).ToUnixTimeMilliseconds());
        offset += 8;
        _ = WriteUtf8Prefixed(record.Outcome.Kind, destination[offset..]);
    }

    public static PersistedIdempotencyRecord Read(ReadOnlySpan<byte> source)
    {
        if (!TryReadUtf8Prefixed(source, out var operationId, out var operationIdBytes))
            throw new InvalidDataException("Snapshot idempotency operation id is missing.");

        if (!TryReadUtf8Prefixed(source[operationIdBytes..], out var fingerprint, out var fingerprintBytes))
            throw new InvalidDataException("Snapshot idempotency fingerprint is missing.");

        var offset = operationIdBytes + fingerprintBytes;
        if (source.Length < offset + 8)
            throw new InvalidDataException("Snapshot idempotency created timestamp is missing.");

        var createdUtc = DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(source[offset..])).UtcDateTime;
        offset += 8;
        if (!TryReadUtf8Prefixed(source[offset..], out var outcomeKind, out _))
            throw new InvalidDataException("Snapshot idempotency outcome kind is missing.");

        var record = new PersistedIdempotencyRecord
        {
            OperationId = operationId,
            Fingerprint = fingerprint,
            CreatedUtc = createdUtc,
            Outcome = new PersistedIdempotencyOutcome { Kind = outcomeKind },
        };
        Validate(record);
        return record;
    }

    private static void Validate(PersistedIdempotencyRecord record)
    {
        if (string.IsNullOrWhiteSpace(record.OperationId))
            throw new InvalidDataException("Snapshot idempotency operation id is missing.");

        if (string.IsNullOrWhiteSpace(record.Fingerprint))
            throw new InvalidDataException("Snapshot idempotency fingerprint is missing.");

        if (!string.Equals(record.Outcome.Kind, "insert", StringComparison.Ordinal))
            throw new InvalidDataException($"Unsupported snapshot idempotency outcome kind: {record.Outcome.Kind}");
    }

    private static int WriteUtf8Prefixed(string text, Span<byte> destination)
    {
        var byteCount = Encoding.UTF8.GetByteCount(text);
        if (byteCount > ushort.MaxValue)
            throw new InvalidDataException("Snapshot string exceeds maximum encoded length.");

        BinaryPrimitives.WriteUInt16LittleEndian(destination, ushort.CreateTruncating(byteCount));
        _ = Encoding.UTF8.GetBytes(text, destination[2..]);
        return 2 + byteCount;
    }

    private static bool TryReadUtf8Prefixed(ReadOnlySpan<byte> source, out string text, out int bytesRead)
    {
        text = string.Empty;
        bytesRead = 0;
        if (source.Length < 2)
            return false;

        var length = BinaryPrimitives.ReadUInt16LittleEndian(source);
        bytesRead = 2 + length;
        if (source.Length < bytesRead)
            return false;

        text = Encoding.UTF8.GetString(source.Slice(2, length));
        return true;
    }
}
