using System;
using System.Buffers;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Stackalloc / ArrayPool helpers for test encode buffers.</summary>
internal static class BufferKit
{
    private const int StackallocThreshold = 256;

    /// <summary>Rents or stackalloc-encodes, then copies into an owned array for test fixtures.</summary>
    /// <typeparam name="TState">Encode state passed without closure capture.</typeparam>
    /// <param name="length">Logical byte length.</param>
    /// <param name="state">Encode state.</param>
    /// <param name="write">Writes the encoded bytes into the scratch buffer.</param>
    /// <returns>An owned copy of the encoded bytes.</returns>
    internal static byte[] ToOwnedBytes<TState>(int length, TState state, Action<TState, Span<byte>> write)
    {
        ArgumentNullException.ThrowIfNull(write);
        if (length <= StackallocThreshold)
        {
            Span<byte> buffer = stackalloc byte[length];
            write(state, buffer);
            return CopyToOwned(buffer);
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

    internal static void WithBuffer<TState>(int length, TState state, Action<TState, Span<byte>> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        if (length <= StackallocThreshold)
        {
            Span<byte> buffer = stackalloc byte[length];
            action(state, buffer);
            return;
        }

        var rented = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            action(state, rented.AsSpan(0, length));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    internal static byte[] CopyToOwned(ReadOnlySpan<byte> source)
    {
        // ZA0302: owned test fixture escape; scratch came from stackalloc or ArrayPool.
#pragma warning disable ZA0302
        var owned = new byte[source.Length];
#pragma warning restore ZA0302
        source.CopyTo(owned);
        return owned;
    }
}
