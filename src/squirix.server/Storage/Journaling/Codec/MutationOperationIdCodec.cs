using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;

namespace Squirix.Server.Storage.Journaling.Codec;

/// <summary>Mutation operation-id prefix encoding for write-ahead journal frames.</summary>
internal static class MutationOperationIdCodec
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    internal static int EncodeMutationOperationIdPrefixLength(string? mutationOperationId)
    {
        if (mutationOperationId == null)
            return 0;

        var opIdLen = Encoding.UTF8.GetByteCount(mutationOperationId);
        if (opIdLen > ushort.MaxValue)
            throw new InvalidDataException("Mutation operation id exceeds maximum encoded length.");

        return 2 + opIdLen;
    }

    internal static int EncodeMutationOperationIdPrefix(string? mutationOperationId, Span<byte> destination, int offset)
    {
        if (mutationOperationId == null)
            return offset;

        var opIdLen = Encoding.UTF8.GetByteCount(mutationOperationId);
        BinaryPrimitives.WriteUInt16LittleEndian(destination[offset..], ushort.CreateTruncating(opIdLen));
        offset += 2;
        offset += Encoding.UTF8.GetBytes(mutationOperationId, destination[offset..]);
        return offset;
    }

    internal static string DecodeMutationOperationId(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 2)
            throw new InvalidDataException("mutation operation id length is missing.");

        var opIdLen = BinaryPrimitives.ReadUInt16LittleEndian(payload);
        if (payload.Length < 2 + opIdLen)
            throw new InvalidDataException("mutation operation id is truncated.");

        try
        {
            return StrictUtf8.GetString(payload.Slice(2, opIdLen));
        }
        catch (DecoderFallbackException ex)
        {
            throw new InvalidDataException("mutation operation id is not valid UTF-8.", ex);
        }
    }
}
