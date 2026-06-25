using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;

namespace Squirix.Server.TestKit.Testing;

/// <summary>Stackalloc / ArrayPool helpers for test encode buffers.</summary>
[SuppressMessage("Design", "MA0182:Avoid unused internal types", Justification = "Used by Squirix.Server.UnitTests via InternalsVisibleTo.")]
internal static class BufferKit
{
    private const int StackallocThreshold = 256;

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
#pragma warning disable ZA0302 // owned test fixture escape; length is bounded by stackalloc threshold here
            var owned = new byte[length];
#pragma warning restore ZA0302
            buffer.CopyTo(owned);
            return owned;
        }

        var rented = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            var span = rented.AsSpan(0, length);
            write(state, span);
#pragma warning disable ZA0302 // owned test fixture escape after pool-backed encode scratch
            var owned = new byte[length];
#pragma warning restore ZA0302
            span.CopyTo(owned);
            return owned;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
