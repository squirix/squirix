using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;

namespace Squirix.Server.Storage.Journaling.Codec;

/// <summary>Mutation operation-id prefix encoding for write-ahead journal frames.</summary>
internal static class MutationOperationIdCodec
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    internal static string DecodeMutationOperationId(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 2)
            throw new InvalidDataException("mutation operation id length is missing.");

        var length = BinaryPrimitives.ReadUInt16LittleEndian(payload);
        if (payload.Length < 2 + length)
            throw new InvalidDataException("mutation operation id is truncated.");

        try
        {
            return StrictUtf8.GetString(payload.Slice(2, length));
        }
        catch (DecoderFallbackException ex)
        {
            throw new InvalidDataException("mutation operation id is not valid UTF-8.", ex);
        }
    }

    internal static int EncodeMutationOperationIdPrefix(string? operationId, Span<byte> destination, int offset)
    {
        if (operationId == null)
            return offset;

        var len = Encoding.UTF8.GetByteCount(operationId);
        BinaryPrimitives.WriteUInt16LittleEndian(destination[offset..], ushort.CreateTruncating(len));
        offset += 2;
        offset += Encoding.UTF8.GetBytes(operationId, destination[offset..]);
        return offset;
    }

    internal static int EncodeMutationOperationIdPrefixLength(string? operationId)
    {
        if (operationId == null)
            return 0;

        var count = Encoding.UTF8.GetByteCount(operationId);
        return count > ushort.MaxValue ? throw new InvalidDataException("Mutation operation id exceeds maximum encoded length.") : 2 + count;
    }
}
