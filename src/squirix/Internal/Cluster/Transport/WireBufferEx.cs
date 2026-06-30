using System;
using System.Buffers;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Stackalloc / ArrayPool scratch helpers for exact-size owned byte buffers.</summary>
internal static class WireBufferEx
{
    private const int StackallocThreshold = 256;

    internal static byte[] EncodeToOwned<TState>(int length, TState state, Action<TState, Span<byte>> write)
    {
        ArgumentNullException.ThrowIfNull(write);
        if (length <= StackallocThreshold)
        {
            Span<byte> scratch = stackalloc byte[length];
            write(state, scratch);
            return CopyToOwned(scratch);
        }

        var rented = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            var span = rented.AsSpan(0, length);
            write(state, span);
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
