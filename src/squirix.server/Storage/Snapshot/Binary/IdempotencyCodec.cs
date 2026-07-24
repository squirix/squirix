using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;

namespace Squirix.Server.Storage.Snapshot.Binary;

internal static class IdempotencyCodec
{
    internal static PersistedIdempotencyRecord Read(ReadOnlySpan<byte> source)
    {
        if (!TryReadUtf8Prefixed(source, out var operationId, out var operationIdBytes))
            throw new InvalidDataException("Snapshot idempotency operation id is missing.");

        if (!TryReadUtf8Prefixed(source[operationIdBytes..], out var fingerprint, out var fingerprintBytes))
            throw new InvalidDataException("Snapshot idempotency fingerprint is missing.");

        var offset = operationIdBytes + fingerprintBytes;
        var createdUtc = ReadCreatedUtc(source, ref offset);
        var responseBytes = ReadResponseBytes(source, ref offset);
        var record = new PersistedIdempotencyRecord(operationId, fingerprint, responseBytes, createdUtc);
        Validate(record);
        return record;
    }

    internal static void Write(PersistedIdempotencyRecord record, Span<byte> destination)
    {
        var offset = 0;
        offset += WriteUtf8Prefixed(record.OperationId, destination[offset..]);
        offset += WriteUtf8Prefixed(record.Fingerprint, destination[offset..]);
        BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], new DateTimeOffset(record.CreatedUtc.ToUniversalTime()).ToUnixTimeMilliseconds());
        offset += 8;
        BinaryPrimitives.WriteInt32LittleEndian(destination[offset..], record.ResponseBytes.Length);
        offset += 4;
        record.ResponseBytes.AsSpan().CopyTo(destination[offset..]);
    }

    internal static int ComputeEncodedLength(PersistedIdempotencyRecord record)
    {
        var operationIdBytes = Encoding.UTF8.GetByteCount(record.OperationId);
        var fingerprintBytes = Encoding.UTF8.GetByteCount(record.Fingerprint);
        var responseBytes = record.ResponseBytes.Length;
        if (operationIdBytes > ushort.MaxValue || fingerprintBytes > ushort.MaxValue)
            throw new InvalidDataException("Snapshot idempotency field exceeds maximum encoded length.");

        try
        {
            return checked(2 + operationIdBytes + 2 + fingerprintBytes + 8 + 4 + responseBytes);
        }
        catch (OverflowException)
        {
            throw new InvalidDataException("Snapshot idempotency field exceeds maximum encoded length.");
        }
    }

    private static DateTime ReadCreatedUtc(ReadOnlySpan<byte> source, ref int offset)
    {
        if (source.Length < offset + 8)
            throw new InvalidDataException("Snapshot idempotency created timestamp is missing.");

        var createdUtc = DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(source[offset..])).UtcDateTime;
        offset += 8;
        return createdUtc;
    }

    private static byte[] ReadResponseBytes(ReadOnlySpan<byte> source, ref int offset)
    {
        if (source.Length < offset + 4)
            throw new InvalidDataException("Snapshot idempotency response length is missing.");

        var responseLength = BinaryPrimitives.ReadInt32LittleEndian(source[offset..]);
        offset += 4;
        if (responseLength < 0 || source.Length < offset + responseLength)
            throw new InvalidDataException("Snapshot idempotency response bytes are truncated.");

        // ZA0302: exact-size owned buffer escape; the record must outlive the borrowed read span.
#pragma warning disable ZA0302
        var bytes = new byte[responseLength];
#pragma warning restore ZA0302
        source.Slice(offset, responseLength).CopyTo(bytes);
        return bytes;
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

    private static void Validate(PersistedIdempotencyRecord record)
    {
        if (string.IsNullOrWhiteSpace(record.OperationId))
            throw new InvalidDataException("Snapshot idempotency operation id is missing.");

        if (string.IsNullOrWhiteSpace(record.Fingerprint))
            throw new InvalidDataException("Snapshot idempotency fingerprint is missing.");

        if (record.ResponseBytes.Length is 0)
            throw new InvalidDataException("Snapshot idempotency response bytes are empty.");
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
}
