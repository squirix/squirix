using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;

namespace Squirix.Server.Storage.Journaling.Codec;

/// <summary>Mutation operation-id prefix encoding for write-ahead journal frames.</summary>
internal static class MutationOperationIdCodec
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    /// <summary>Decodes the prefix: the operation id, then the request fingerprint (empty when the stamp carries none).</summary>
    /// <param name="payload">The mutation payload starting at the prefix.</param>
    /// <param name="fingerprint">The decoded fingerprint, or <see langword="null" /> when the prefix carries none.</param>
    /// <returns>The decoded operation id.</returns>
    /// <exception cref="InvalidDataException">The prefix is truncated or not valid UTF-8.</exception>
    internal static string DecodeMutationOperationId(ReadOnlySpan<byte> payload, out string? fingerprint)
    {
        var operationId = DecodeField(payload, 0, "operation id", out var next);
        var fingerprintValue = DecodeField(payload, next, "fingerprint", out _);
        fingerprint = fingerprintValue.Length == 0 ? null : fingerprintValue;
        return operationId;
    }

    internal static int EncodeMutationOperationIdPrefix(string? operationId, string? fingerprint, Span<byte> destination, int offset)
    {
        if (operationId == null)
            return offset;

        offset = EncodeField(operationId, destination, offset);
        return EncodeField(fingerprint ?? string.Empty, destination, offset);
    }

    internal static int EncodeMutationOperationIdPrefixLength(string? operationId, string? fingerprint)
    {
        if (operationId == null)
            return 0;

        var count = Encoding.UTF8.GetByteCount(operationId);
        var fingerprintCount = fingerprint == null ? 0 : Encoding.UTF8.GetByteCount(fingerprint);
        return count > ushort.MaxValue || fingerprintCount > ushort.MaxValue
            ? throw new InvalidDataException("Mutation operation id or fingerprint exceeds maximum encoded length.")
            : 2 + count + 2 + fingerprintCount;
    }

    private static string DecodeField(ReadOnlySpan<byte> payload, int offset, string name, out int next)
    {
        if (payload.Length < offset + 2)
            throw new InvalidDataException($"mutation {name} length is missing.");

        var length = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
        if (payload.Length < offset + 2 + length)
            throw new InvalidDataException($"mutation {name} is truncated.");

        try
        {
            var value = StrictUtf8.GetString(payload.Slice(offset + 2, length));
            next = offset + 2 + length;
            return value;
        }
        catch (DecoderFallbackException ex)
        {
            throw new InvalidDataException($"mutation {name} is not valid UTF-8.", ex);
        }
    }

    private static int EncodeField(string value, Span<byte> destination, int offset)
    {
        var len = Encoding.UTF8.GetByteCount(value);
        BinaryPrimitives.WriteUInt16LittleEndian(destination[offset..], ushort.CreateTruncating(len));
        offset += 2;
        return offset + Encoding.UTF8.GetBytes(value, destination[offset..]);
    }
}
