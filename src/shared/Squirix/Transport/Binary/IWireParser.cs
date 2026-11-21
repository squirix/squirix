using System;

namespace Squirix.Transport.Binary;

internal interface IWireParser
{
    WireMessage ParseFrom(ReadOnlySpan<byte> bytes);
}
