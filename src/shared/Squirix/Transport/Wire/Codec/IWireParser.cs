using System;

namespace Squirix.Transport.Wire.Codec;

internal interface IWireParser
{
    WireMessage ParseFrom(ReadOnlySpan<byte> bytes);
}
