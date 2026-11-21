using System;
using System.IO;

namespace Squirix.Transport.Binary;

internal sealed class WireParser<T> : IWireParser
    where T : WireMessage
{
    private readonly WireDecoder<T> _decode;

    internal WireParser(WireDecoder<T> decode)
    {
        _decode = decode;
    }

    WireMessage IWireParser.ParseFrom(ReadOnlySpan<byte> bytes) => ParseFrom(bytes);

    internal T ParseFrom(ReadOnlySpan<byte> bytes)
    {
        var reader = new WireReader(bytes);
        return Decode(ref reader);
    }

    internal T ParseFrom(byte[] buffer, int offset, int length) => ParseFrom(buffer.AsSpan(offset, length));

    /// <summary>Decodes a message that borrows byte fields from the caller's frame.</summary>
    /// <param name="frame">Memory kept alive and unchanged throughout consumption of this message.</param>
    /// <returns>The decoded message.</returns>
    internal T ParseBorrowed(ReadOnlyMemory<byte> frame)
    {
        var reader = new WireReader(frame.Span, frame);
        return Decode(ref reader);
    }

    private T Decode(ref WireReader reader)
    {
        var result = _decode(ref reader);
        return reader.Remaining == 0 ? result : throw new InvalidDataException("Unexpected trailing bytes in a wire message.");
    }
}
