using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Squirix.Benchmarks.Support;

/// <summary>
/// Throw-helper methods for benchmark harness guards that have no BCL throw helper.
/// Each check stays inlineable in the caller while the throwing path lives in a non-inlined method.
/// </summary>
internal static class BenchmarkThrowHelper
{
    /// <summary>Returns <paramref name="value" /> when the lease is not disposed.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="value">The required value.</param>
    /// <param name="owner">The disposed owner name for exceptions.</param>
    /// <returns><paramref name="value" />.</returns>
    /// <exception cref="ObjectDisposedException">Thrown when <paramref name="value" /> is null.</exception>
    public static T Disposed<T>(T? value, string owner)
        where T : class
    {
        if (value == null)
            ThrowDisposed(owner);

        return value;
    }

    /// <summary>Returns <paramref name="value" /> when it is not null.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="value">The required value.</param>
    /// <param name="message">The exception message.</param>
    /// <returns><paramref name="value" />.</returns>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="value" /> is null.</exception>
    public static T Required<T>(T? value, string message)
        where T : class
    {
        if (value == null)
            ThrowInvalidOperation(message);

        return value;
    }

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowDisposed(string owner) => throw new ObjectDisposedException(owner);

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowInvalidOperation(string message) => throw new InvalidOperationException(message);
}
