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

    /// <summary>Allocates an exact-size owned byte buffer that must outlive the current span.</summary>
    /// <param name="length">Exact buffer length.</param>
    /// <returns>An owned byte array of the requested length.</returns>
#pragma warning disable ZA0302 // ZA0302: exact-size owned buffer escape; the caller fills the buffer and retains ownership.
    internal static byte[] Owned(int length) => new byte[length];
#pragma warning restore ZA0302

    /// <summary>Copies a span into an exact-size owned byte buffer.</summary>
    /// <param name="source">Source bytes to copy.</param>
    /// <returns>An owned byte array containing the source bytes.</returns>
    internal static byte[] CopyToOwned(ReadOnlySpan<byte> source)
    {
        var owned = Owned(source.Length);
        source.CopyTo(owned);
        return owned;
    }
}
