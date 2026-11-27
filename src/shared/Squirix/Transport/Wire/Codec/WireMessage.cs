using System;
using System.Buffers;

namespace Squirix.Transport.Wire.Codec;

internal abstract class WireMessage
{
    internal abstract void Encode(ref WireWriter writer);

    internal int CalculateSize(string? mutationOperationId = null) => CalculateSize(mutationOperationId, out _);

    internal int CalculateSize(string? mutationOperationId, out bool hasBorrowedBytes)
    {
        var writer = new WireWriter(mutationOperationId);
        Encode(ref writer);
        hasBorrowedBytes = writer.HasBorrowedBytes;
        return writer.Written;
    }

    internal void WriteTo(Span<byte> destination, string? mutationOperationId = null)
    {
        var writer = new WireWriter(destination, mutationOperationId);
        Encode(ref writer);
    }

    internal void WriteTo(IBufferWriter<byte> destination, string? mutationOperationId = null)
    {
        var writer = new WireWriter(destination, mutationOperationId);
        Encode(ref writer);
    }

    internal byte[] ToByteArray()
    {
        var result = GC.AllocateUninitializedArray<byte>(CalculateSize());
        WriteTo(result);
        return result;
    }
}
