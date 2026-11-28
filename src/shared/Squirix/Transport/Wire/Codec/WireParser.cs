using System;

namespace Squirix.Transport.Wire.Codec;

internal sealed class WireParser<T> : IWireParser
    where T : WireMessage
{
    private readonly WireBodyParser<T> _body;

    internal WireParser(WireDecoder<T> decode)
    {
        _body = new WireBodyParser<T>(decode);
    }

    WireMessage IWireParser.ParseFrom(ReadOnlySpan<byte> bytes) => ParseFrom(bytes);

    internal T ParseFrom(ReadOnlySpan<byte> bytes, WireNameCache? names = null) => _body.ParseFrom(bytes, names);

    internal T ParseFrom(byte[] buffer, int offset, int length) => _body.ParseFrom(buffer.AsSpan(offset, length));

    internal T ParseBorrowed(ReadOnlyMemory<byte> frame, WireNameCache? names = null) => _body.ParseBorrowed(frame, names);
}
