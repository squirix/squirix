using System;
using System.IO;

namespace Squirix.Transport.Wire.Codec;

internal readonly struct WireBodyParser<T>
{
    private readonly WireDecoder<T> _decode;

    internal WireBodyParser(WireDecoder<T> decode)
    {
        _decode = decode;
    }

    internal T ParseFrom(ReadOnlySpan<byte> bytes, WireNameCache? names = null)
    {
        var reader = new WireReader(bytes, names: names);
        return Decode(ref reader);
    }

    internal T ParseFrom(byte[] buffer, int offset, int length) => ParseFrom(buffer.AsSpan(offset, length));

    /// <summary>Decodes a message that borrows byte fields from the caller's frame.</summary>
    /// <param name="frame">Memory kept alive and unchanged throughout consumption of this message.</param>
    /// <returns>The decoded message.</returns>
    /// <param name="names">Optional per-connection cache name reuse.</param>
    internal T ParseBorrowed(ReadOnlyMemory<byte> frame, WireNameCache? names = null)
    {
        var reader = new WireReader(frame.Span, frame, names);
        return Decode(ref reader);
    }

    private T Decode(ref WireReader reader)
    {
        var result = _decode(ref reader);
        return reader.Remaining == 0 ? result : throw new InvalidDataException("Unexpected trailing bytes in a wire message.");
    }
}
