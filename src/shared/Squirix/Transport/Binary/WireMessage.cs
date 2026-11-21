using System;
using System.Buffers;

namespace Squirix.Transport.Binary;

internal abstract class WireMessage
{
    internal abstract void Encode(ref WireWriter writer);

    internal int CalculateSize()
    {
        WireWriter writer = default;
        Encode(ref writer);
        return writer.Written;
    }

    internal void WriteTo(Span<byte> destination)
    {
        var writer = new WireWriter(destination);
        Encode(ref writer);
    }

    internal void WriteTo(IBufferWriter<byte> destination)
    {
        var writer = new WireWriter(destination);
        Encode(ref writer);
    }

    internal byte[] ToByteArray()
    {
        var result = GC.AllocateUninitializedArray<byte>(CalculateSize());
        WriteTo(result);
        return result;
    }
}
