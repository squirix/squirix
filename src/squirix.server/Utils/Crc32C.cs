using System;

namespace Squirix.Server.Utils;

/// <summary>Software CRC32C (Castagnoli) with reflected polynomial 0x82F63B78.</summary>
internal static class Crc32C
{
    private const uint Poly = 0x82F63B78u;
    private static readonly uint[] Table = CreateTable();

    internal static uint InitialValue => 0xFFFF_FFFFu;

    internal static uint Append(uint crc, ReadOnlySpan<byte> data)
    {
        foreach (var b in data)
            crc = Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        return crc;
    }

    internal static uint Append(uint crc, byte value) => Table[(crc ^ value) & 0xFF] ^ (crc >> 8);

    internal static uint Compute(ReadOnlySpan<byte> data) => Finalize(Append(InitialValue, data));

    internal static uint Finalize(uint crc) => ~crc;

    private static uint[] CreateTable()
    {
        var t = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            var c = i;
            for (var k = 0; k < 8; k++)
                c = (c & 1) != 0 ? Poly ^ (c >> 1) : c >> 1;
            t[i] = c;
        }

        return t;
    }
}
