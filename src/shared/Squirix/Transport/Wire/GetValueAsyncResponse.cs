using System.IO;
using System.Runtime.CompilerServices;
using Squirix.Transport.Binary;

namespace Squirix.Transport.Wire.Cache;

internal sealed class GetValueAsyncResponse : WireMessage
{
    private const int FoundFlag = 1;
    private const int ValueFlag = 2;

    internal static WireParser<GetValueAsyncResponse> Parser { get; } = new(Read);

    internal bool Found { get; set; }

    internal CacheValue? Value { get; set; }

    internal static bool HasBinaryPayload(byte flags) => (flags & ~FoundFlag) == PresentKind(CacheValue.KindOneofCase.BinaryValue);

    internal static bool HasRawPayload(byte flags) =>
        HasBinaryPayload(flags) || (flags & ~FoundFlag) == PresentKind(CacheValue.KindOneofCase.JsonUtf8);

    internal static GetValueAsyncResponse FromOwnedBinary(byte flags, byte[] payload) =>
        new() { Found = (flags & FoundFlag) != 0, Value = CacheValue.FromOwnedBinary(payload) };

    internal override void Encode(ref WireWriter writer)
    {
        var value = Value;
        if (!Found && value == null)
            return;

        var flags = Found ? FoundFlag : 0;
        if (value != null)
            flags |= PresentKind(value.KindCase);

        writer.Byte(byte.CreateTruncating(flags));
        value?.EncodePayload(ref writer, true);
    }

    private static int PresentKind(CacheValue.KindOneofCase kind) =>
        ValueFlag | (Unsafe.BitCast<CacheValue.KindOneofCase, byte>(kind) << 2);

    private static GetValueAsyncResponse Read(ref WireReader reader)
    {
        var message = new GetValueAsyncResponse();
        if (reader.Remaining == 0)
            return message;

        var flags = reader.Flags(31);
        message.Found = (flags & FoundFlag) != 0;
        var kind = Unsafe.BitCast<byte, CacheValue.KindOneofCase>(byte.CreateTruncating(flags >> 2));
        if ((flags & ValueFlag) != 0)
            message.Value = CacheValue.ReadPayload(ref reader, kind, true);
        else if (kind != CacheValue.KindOneofCase.None)
            throw new InvalidDataException("Cache value kind requires a value.");

        return message;
    }
}
