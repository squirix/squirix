using System;
using System.Buffers;
using System.Security.Cryptography;
using System.Text;

namespace Squirix.Server.Cluster;

/// <summary>
/// SHA-256 with 64-bit truncation (first 8 bytes as little-endian).
/// Very even distribution for CH rings; slower than FNV, but OK for ring build/lookups.
/// </summary>
internal sealed class Sha256Hasher : IHash
{
    public ulong HashCacheRouteKey(string cacheName, string key)
    {
        ArgumentNullException.ThrowIfNull(cacheName);
        ArgumentNullException.ThrowIfNull(key);

        var byteCount = checked(CountDigits(cacheName.Length) + 1 + Encoding.UTF8.GetByteCount(cacheName) + 1 + Encoding.UTF8.GetByteCount(key));
        var rented = ArrayPool<byte>.Shared.Rent(byteCount);

        try
        {
            var buffer = rented.AsSpan(0, byteCount);
            var written = WriteNonNegativeIntUtf8(cacheName.Length, buffer);
            buffer[written++] = (byte)':';
            written += Encoding.UTF8.GetBytes(cacheName.AsSpan(), buffer[written..]);
            buffer[written++] = 0x1F;
            written += Encoding.UTF8.GetBytes(key.AsSpan(), buffer[written..]);
            return Hash(buffer[..written]);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    public ulong HashString(string text)
    {
        if (text.Length == 0)
            return Hash([]);

        var byteCount = Encoding.UTF8.GetByteCount(text);
        var rented = ArrayPool<byte>.Shared.Rent(byteCount);

        try
        {
            var bytesWritten = Encoding.UTF8.GetBytes(text, rented);
            return Hash(rented.AsSpan(0, bytesWritten));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static int CountDigits(int value)
    {
        var digits = 1;
        while (value >= 10)
        {
            value /= 10;
            digits++;
        }

        return digits;
    }

    private static ulong Hash(ReadOnlySpan<byte> data)
    {
        Span<byte> digest = stackalloc byte[32];
        _ = SHA256.HashData(data, digest);
        return BitConverter.ToUInt64(digest);
    }

    private static int WriteNonNegativeIntUtf8(int value, Span<byte> destination)
    {
        var digits = CountDigits(value);
        for (var i = digits - 1; i >= 0; i--)
        {
            destination[i] = (byte)('0' + (value % 10));
            value /= 10;
        }

        return digits;
    }
}
