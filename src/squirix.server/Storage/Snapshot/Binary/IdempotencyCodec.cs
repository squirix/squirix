using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot.Binary;

internal static class IdempotencyCodec
{
    private const byte CompletedStateWire = 0;

    private const byte StartedStateWire = 1;

    private const byte FingerprintAbsentWire = 0;

    private const byte FingerprintPresentWire = 1;

    private const string FieldExceedsMaxLengthMessage = "Snapshot idempotency field exceeds maximum encoded length.";

    private const string FingerprintMissingMessage = "Snapshot idempotency fingerprint is missing.";

    internal static int ComputeEncodedLength(PersistedIdempotencyRecord record)
    {
        var operationIdBytes = Encoding.UTF8.GetByteCount(record.OperationId);
        if (operationIdBytes > ushort.MaxValue)
            throw new InvalidDataException(FieldExceedsMaxLengthMessage);

        if (record.State == IdempotencyRecordState.Started)
        {
            var hasFingerprint = record.Fingerprint != null ? 1 : 0;
            var startedFingerprintBytes = record.Fingerprint != null ? Encoding.UTF8.GetByteCount(record.Fingerprint) : 0;
            if (startedFingerprintBytes > ushort.MaxValue)
                throw new InvalidDataException(FieldExceedsMaxLengthMessage);

            try
            {
                return checked(1 + 2 + operationIdBytes + 1 + (hasFingerprint * (2 + startedFingerprintBytes)) + 8);
            }
            catch (OverflowException)
            {
                throw new InvalidDataException(FieldExceedsMaxLengthMessage);
            }
        }

        var fingerprintBytes = Encoding.UTF8.GetByteCount(record.Fingerprint ?? string.Empty);
        if (fingerprintBytes > ushort.MaxValue)
            throw new InvalidDataException(FieldExceedsMaxLengthMessage);

        var responseBytes = record.ResponseBytes.Length;
        try
        {
            return checked(1 + 2 + operationIdBytes + 2 + fingerprintBytes + 8 + 4 + responseBytes);
        }
        catch (OverflowException)
        {
            throw new InvalidDataException(FieldExceedsMaxLengthMessage);
        }
    }

    /// <summary>Decodes one idempotency record in the current snapshot layout, with a fallback for pre-upgrade snapshots.</summary>
    /// <param name="source">The record body bytes.</param>
    /// <returns>The decoded record.</returns>
    /// <exception cref="InvalidDataException">When the record is truncated or invalid.</exception>
    internal static PersistedIdempotencyRecord Read(ReadOnlySpan<byte> source)
    {
        try
        {
            return ReadVersioned(source);
        }
        catch (InvalidDataException)
        {
            // Pre-upgrade snapshots encode completed records without the leading state byte:
            // u16 opId + opId + u16 fingerprint + fingerprint + i64 created + i32 response length + response.
            // Without this fallback every pre-upgrade snapshot is rejected and recovery falls back to a
            // full journal replay (fatal when the journal range is unavailable).
            if (TryReadLegacy(source, out var legacy) && legacy != null)
                return legacy;

            throw;
        }
    }

    internal static void Write(PersistedIdempotencyRecord record, Span<byte> destination)
    {
        var offset = 0;
        destination[offset++] = record.State == IdempotencyRecordState.Started ? StartedStateWire : CompletedStateWire;
        offset += WriteUtf8Prefixed(record.OperationId, destination[offset..]);

        if (record.State == IdempotencyRecordState.Started)
        {
            destination[offset++] = record.Fingerprint != null ? FingerprintPresentWire : FingerprintAbsentWire;
            if (record.Fingerprint != null)
                offset += WriteUtf8Prefixed(record.Fingerprint, destination[offset..]);

            WriteCreatedUtc(record.CreatedUtc, destination, ref offset);
            return;
        }

        offset += WriteUtf8Prefixed(record.Fingerprint!, destination[offset..]);
        WriteCreatedUtc(record.CreatedUtc, destination, ref offset);
        BinaryPrimitives.WriteInt32LittleEndian(destination[offset..], record.ResponseBytes.Length);
        offset += 4;
        record.ResponseBytes.AsSpan().CopyTo(destination[offset..]);
    }

    /// <summary>Decodes one idempotency record in the versioned snapshot layout (leading state byte).</summary>
    /// <param name="source">The record body bytes.</param>
    /// <returns>The decoded record.</returns>
    /// <exception cref="InvalidDataException">When the record is truncated or invalid.</exception>
    private static PersistedIdempotencyRecord ReadVersioned(ReadOnlySpan<byte> source)
    {
        if (source.Length < 1)
            throw new InvalidDataException("Snapshot idempotency record state is missing.");

        var stateWire = source[0];
        if (stateWire is not (CompletedStateWire or StartedStateWire))
            throw new InvalidDataException("Snapshot idempotency record state is invalid.");

        var offset = 1;
        if (!TryReadUtf8Prefixed(source, ref offset, out var operationId))
            throw new InvalidDataException("Snapshot idempotency operation id is missing.");

        var record = stateWire == StartedStateWire
            ? ReadStartedRecord(source, ref offset, operationId)
            : ReadCompletedRecord(source, ref offset, operationId);

        EnsureFullyConsumed(source, offset);
        return record;
    }

    /// <summary>Decodes a write-ahead started record body after the operation id.</summary>
    /// <param name="source">The record body bytes.</param>
    /// <param name="offset">The offset after the operation id; advanced past the decoded fields.</param>
    /// <param name="operationId">The already-decoded operation id.</param>
    /// <returns>The decoded started record.</returns>
    /// <exception cref="InvalidDataException">When the record is truncated or invalid.</exception>
    private static PersistedIdempotencyRecord ReadStartedRecord(ReadOnlySpan<byte> source, ref int offset, string operationId)
    {
        if (source.Length < offset + 1)
            throw new InvalidDataException("Snapshot idempotency fingerprint presence flag is missing.");

        var hasFingerprint = source[offset++];
        if (hasFingerprint is not (0 or 1))
            throw new InvalidDataException("Snapshot idempotency fingerprint presence flag is invalid.");

        string? fingerprint = null;
        if (hasFingerprint == 1 && !TryReadUtf8Prefixed(source, ref offset, out fingerprint))
            throw new InvalidDataException(FingerprintMissingMessage);

        var createdUtc = ReadCreatedUtc(source, ref offset);
        var record = new PersistedIdempotencyRecord(operationId, fingerprint, createdUtc);
        Validate(record);
        return record;
    }

    /// <summary>Decodes a completed record body after the operation id.</summary>
    /// <param name="source">The record body bytes.</param>
    /// <param name="offset">The offset after the operation id; advanced past the decoded fields.</param>
    /// <param name="operationId">The already-decoded operation id.</param>
    /// <returns>The decoded completed record.</returns>
    /// <exception cref="InvalidDataException">When the record is truncated or invalid.</exception>
    private static PersistedIdempotencyRecord ReadCompletedRecord(ReadOnlySpan<byte> source, ref int offset, string operationId)
    {
        if (!TryReadUtf8Prefixed(source, ref offset, out var fingerprint))
            throw new InvalidDataException(FingerprintMissingMessage);

        var createdUtc = ReadCreatedUtc(source, ref offset);
        var responseBytes = ReadResponseBytes(source, ref offset);
        var record = new PersistedIdempotencyRecord(operationId, fingerprint, responseBytes, createdUtc);
        Validate(record);
        return record;
    }

    private static PersistedIdempotencyRecord ReadLegacy(ReadOnlySpan<byte> source)
    {
        var offset = 0;
        if (!TryReadUtf8Prefixed(source, ref offset, out var operationId))
            throw new InvalidDataException("Snapshot idempotency operation id is missing.");

        if (!TryReadUtf8Prefixed(source, ref offset, out var fingerprint))
            throw new InvalidDataException(FingerprintMissingMessage);

        var createdUtc = ReadCreatedUtc(source, ref offset);
        var responseBytes = ReadResponseBytes(source, ref offset);
        var record = new PersistedIdempotencyRecord(operationId, fingerprint, responseBytes, createdUtc);
        Validate(record);
        EnsureFullyConsumed(source, offset);
        return record;
    }

    private static bool TryReadLegacy(ReadOnlySpan<byte> source, out PersistedIdempotencyRecord? record)
    {
        try
        {
            record = ReadLegacy(source);
            return true;
        }
        catch (InvalidDataException)
        {
            record = null;
            return false;
        }
    }

    private static void EnsureFullyConsumed(ReadOnlySpan<byte> source, int offset)
    {
        if (offset != source.Length)
            throw new InvalidDataException("Snapshot idempotency record has trailing bytes.");
    }

    private static DateTime ReadCreatedUtc(ReadOnlySpan<byte> source, ref int offset)
    {
        if (source.Length < offset + 8)
            throw new InvalidDataException("Snapshot idempotency created timestamp is missing.");

        var createdUtc = DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(source[offset..])).UtcDateTime;
        offset += 8;
        return createdUtc;
    }

    private static void WriteCreatedUtc(DateTime createdUtc, Span<byte> destination, ref int offset)
    {
        BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], new DateTimeOffset(createdUtc.ToUniversalTime()).ToUnixTimeMilliseconds());
        offset += 8;
    }

    private static byte[] ReadResponseBytes(ReadOnlySpan<byte> source, ref int offset)
    {
        if (source.Length < offset + 4)
            throw new InvalidDataException("Snapshot idempotency response length is missing.");

        var responseLength = BinaryPrimitives.ReadInt32LittleEndian(source[offset..]);
        offset += 4;
        if (responseLength < 0 || source.Length < offset + responseLength)
            throw new InvalidDataException("Snapshot idempotency response bytes are truncated.");

        var responseBytes = BufferEx.CopyToOwned(source.Slice(offset, responseLength));
        offset += responseLength;
        return responseBytes;
    }

    private static bool TryReadUtf8Prefixed(ReadOnlySpan<byte> source, ref int offset, out string text)
    {
        text = string.Empty;
        if (source.Length < offset + 2)
            return false;

        var length = BinaryPrimitives.ReadUInt16LittleEndian(source[offset..]);
        offset += 2;
        if (source.Length < offset + length)
            return false;

        text = Encoding.UTF8.GetString(source.Slice(offset, length));
        offset += length;
        return true;
    }

    private static void Validate(PersistedIdempotencyRecord record)
    {
        if (string.IsNullOrWhiteSpace(record.OperationId))
            throw new InvalidDataException("Snapshot idempotency operation id is missing.");

        if (record.State == IdempotencyRecordState.Started)
            return;

        if (string.IsNullOrWhiteSpace(record.Fingerprint))
            throw new InvalidDataException(FingerprintMissingMessage);

        if (record.ResponseBytes.Length == 0)
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
