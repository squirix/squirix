using System;
using System.Buffers;
using System.Text;

namespace Squirix.Server.Utils;

/// <summary>Stackalloc / ArrayPool scratch helpers for exact-size owned byte buffers.</summary>
internal static class BufferEx
{
    private const int StackallocThreshold = 256;

    internal static byte[] Utf8ToOwned(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        var byteCount = Encoding.UTF8.GetByteCount(text);
        if (byteCount <= StackallocThreshold)
        {
            Span<byte> scratch = stackalloc byte[byteCount];
            _ = Encoding.UTF8.GetBytes(text, scratch);
            return CopyToOwned(scratch);
        }

        var rented = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            var span = rented.AsSpan(0, byteCount);
            _ = Encoding.UTF8.GetBytes(text, span);
            return CopyToOwned(span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static byte[] CopyToOwned(ReadOnlySpan<byte> source)
    {
        // ZA0302: exact-size owned buffer escape; scratch already came from stackalloc or ArrayPool.
#pragma warning disable ZA0302
        var owned = new byte[source.Length];
#pragma warning restore ZA0302
        source.CopyTo(owned);
        return owned;
    }
}
