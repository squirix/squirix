using System;

namespace Squirix.Server.Utils;

/// <summary>Helpers for exact-size owned byte buffers.</summary>
internal static class BufferEx
{
    /// <summary>Allocates an exact-size owned byte buffer that must outlive the current span.</summary>
    /// <param name="length">Exact buffer length.</param>
    /// <returns>An owned byte array of the requested length.</returns>
    internal static byte[] Owned(int length) => new byte[length];

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
