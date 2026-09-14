using System;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Stackalloc / plain array helpers for test encode buffers.</summary>
internal static class BufferKit
{
    private const int StackallocThreshold = 256;

    internal static byte[] CopyToOwned(ReadOnlySpan<byte> source)
    {
        var owned = new byte[source.Length];
        source.CopyTo(owned);
        return owned;
    }

    /// <summary>Stackalloc-encodes or writes into a plain array, then returns an owned copy for test fixtures.</summary>
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

        var owned = new byte[length];
        write(state, owned);
        return owned;
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

        action(state, new byte[length]);
    }
}
