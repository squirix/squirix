using System;
using System.Buffers;
using System.Runtime.InteropServices;

namespace Squirix.Transport.Wire.Codec;

/// <summary>Stores a borrowed byte range using the reference and scalar fields of a tagged wire value.</summary>
internal static class WireByteRange
{
    internal const long OwnedArray = -1;

    internal static ReadOnlyMemory<byte> View(object? owner, long range)
    {
        if (owner is byte[] bytes)
            return range == OwnedArray ? bytes : bytes.AsMemory(int.CreateTruncating(range >> 32), int.CreateTruncating(range));
        return owner is ReadOnlyMemory<byte> memory ? memory : default;
    }

    internal static void Store(ReadOnlyMemory<byte> memory, out object? owner, out long range)
    {
        if (!MemoryMarshal.TryGetMemoryManager<byte, MemoryManager<byte>>(memory, out _) && MemoryMarshal.TryGetArray(memory, out var slice))
        {
            owner = slice.Array;
            range = Pack(slice.Offset, slice.Count);
        }
        else
        {
            owner = memory;
            range = 0;
        }
    }

    private static long Pack(int offset, int length) => (long.CreateChecked(offset) << 32) | uint.CreateChecked(length);
}
