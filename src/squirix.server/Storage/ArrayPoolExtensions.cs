using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;

namespace Squirix.Server.Storage;

/// <summary>Return helpers for pooled arrays that carried operation data.</summary>
internal static class ArrayPoolExtensions
{
    /// <summary>Clears the buffer and returns it to the pool.</summary>
    /// <param name="pool">The pool the buffer was rented from.</param>
    /// <param name="buffer">The rented buffer.</param>
    /// <typeparam name="T">The buffer element type.</typeparam>
    /// <remarks>
    /// Pooled buffers carry operation payloads; returning them uncleared would leak that data into unrelated
    /// operations renting the same array. Direct <see cref="ArrayPool{ T }.Return" /> is banned
    /// (BannedSymbols.Storage.txt) so every return goes through this wrapper.
    /// </remarks>
    [SuppressMessage("BannedApis", "RS0030", Justification = "This wrapper is the sanctioned cleared-return path.")]
    internal static void ReturnCleared<T>(this ArrayPool<T> pool, T[] buffer)
    {
        Array.Clear(buffer);
        pool.Return(buffer);
    }
}
