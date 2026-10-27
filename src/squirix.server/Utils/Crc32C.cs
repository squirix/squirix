using System;
using System.Buffers.Binary;
using System.Numerics;

namespace Squirix.Server.Utils;

/// <summary>
/// CRC32C (Castagnoli, reflected polynomial 0x82F63B78) computed with <see cref="BitOperations.Crc32C(uint, ulong)"/>,
/// which uses the SSE4.2 or Arm64 CRC32C instruction when available and a software fallback otherwise.
/// </summary>
internal static class Crc32C
{
    private const int WordSize = sizeof(ulong);
    private const int HalfWordSize = sizeof(uint);

    internal static uint InitialValue => 0xFFFF_FFFFu;

    internal static uint Append(uint crc, ReadOnlySpan<byte> data)
    {
        var offset = 0;
        while (data.Length - offset >= WordSize)
        {
            crc = BitOperations.Crc32C(crc, BinaryPrimitives.ReadUInt64LittleEndian(data[offset..]));
            offset += WordSize;
        }

        if (data.Length - offset >= HalfWordSize)
        {
            crc = BitOperations.Crc32C(crc, BinaryPrimitives.ReadUInt32LittleEndian(data[offset..]));
            offset += HalfWordSize;
        }

        for (; offset < data.Length; offset++)
            crc = BitOperations.Crc32C(crc, data[offset]);
        return crc;
    }

    internal static uint Append(uint crc, byte value) => BitOperations.Crc32C(crc, value);

    internal static uint Compute(ReadOnlySpan<byte> data) => Finalize(Append(InitialValue, data));

    internal static uint Finalize(uint crc) => ~crc;
}
