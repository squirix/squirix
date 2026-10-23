using System;
using System.Buffers.Binary;
using System.Numerics;

namespace Squirix.Server.Utils;

/// <summary>Non-cryptographic XXH64 (xxHash 64-bit) with seed 0, little-endian input reads.</summary>
internal static class XxHash64
{
    private const ulong Prime1 = 11400714785074694791UL;
    private const ulong Prime2 = 14029467366897019727UL;
    private const ulong Prime3 = 1609587929392839161UL;
    private const ulong Prime4 = 9650029242287828579UL;
    private const ulong Prime5 = 2870177450012600261UL;

    private const int StripeLength = 32;
    private const int LaneLength = 8;
    private const int HalfLaneLength = 4;

    private const int Round1Rotation = 31;
    private const int AvalancheShift1 = 33;
    private const int AvalancheShift2 = 29;
    private const int AvalancheShift3 = 32;
    private const int Lane1Rotation = 1;
    private const int Lane2Rotation = 7;
    private const int Lane3Rotation = 12;
    private const int Lane4Rotation = 18;
    private const int TailLaneRotation = 27;
    private const int TailHalfLaneRotation = 23;
    private const int TailByteRotation = 11;

    internal static ulong Hash(ReadOnlySpan<byte> data)
    {
        var remaining = data;
        var hash = remaining.Length >= StripeLength ? HashStripes(ref remaining) : Prime5;
        return HashTail(hash, data.Length, remaining);
    }

    private static ulong HashStripes(ref ReadOnlySpan<byte> remaining)
    {
        var v1 = unchecked(Prime1 + Prime2);
        var v2 = Prime2;
        ulong v3 = 0;
        var v4 = unchecked(0UL - Prime1);

        do
        {
            v1 = Round(v1, BinaryPrimitives.ReadUInt64LittleEndian(remaining));
            v2 = Round(v2, BinaryPrimitives.ReadUInt64LittleEndian(remaining[LaneLength..]));
            v3 = Round(v3, BinaryPrimitives.ReadUInt64LittleEndian(remaining[(2 * LaneLength)..]));
            v4 = Round(v4, BinaryPrimitives.ReadUInt64LittleEndian(remaining[(3 * LaneLength)..]));
            remaining = remaining[StripeLength..];
        }
        while (remaining.Length >= StripeLength);

        var hash = unchecked(BitOperations.RotateLeft(v1, Lane1Rotation) + BitOperations.RotateLeft(v2, Lane2Rotation)
                             + BitOperations.RotateLeft(v3, Lane3Rotation) + BitOperations.RotateLeft(v4, Lane4Rotation));
        hash = MergeRound(hash, v1);
        hash = MergeRound(hash, v2);
        hash = MergeRound(hash, v3);
        return MergeRound(hash, v4);
    }

    private static ulong HashTail(ulong hash, int totalLength, ReadOnlySpan<byte> remaining)
    {
        unchecked
        {
            hash += Convert.ToUInt64(totalLength);

            while (remaining.Length >= LaneLength)
            {
                hash ^= Round(0, BinaryPrimitives.ReadUInt64LittleEndian(remaining));
                hash = (BitOperations.RotateLeft(hash, TailLaneRotation) * Prime1) + Prime4;
                remaining = remaining[LaneLength..];
            }

            if (remaining.Length >= HalfLaneLength)
            {
                hash ^= BinaryPrimitives.ReadUInt32LittleEndian(remaining) * Prime1;
                hash = (BitOperations.RotateLeft(hash, TailHalfLaneRotation) * Prime2) + Prime3;
                remaining = remaining[HalfLaneLength..];
            }

            foreach (var b in remaining)
            {
                hash ^= b * Prime5;
                hash = BitOperations.RotateLeft(hash, TailByteRotation) * Prime1;
            }

            hash ^= hash >> AvalancheShift1;
            hash *= Prime2;
            hash ^= hash >> AvalancheShift2;
            hash *= Prime3;
            hash ^= hash >> AvalancheShift3;
            return hash;
        }
    }

    private static ulong MergeRound(ulong hash, ulong value)
    {
        unchecked
        {
            return ((hash ^ Round(0, value)) * Prime1) + Prime4;
        }
    }

    private static ulong Round(ulong accumulator, ulong input)
    {
        unchecked
        {
            return BitOperations.RotateLeft(accumulator + (input * Prime2), Round1Rotation) * Prime1;
        }
    }
}
