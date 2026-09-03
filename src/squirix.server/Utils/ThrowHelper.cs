using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Squirix.Server.Utils;

/// <summary>
/// Throw-helper methods for fail-fast guards that have no BCL throw helper
/// (<c language="csharp">InvalidOperationException</c> state checks and similar).
/// Each check stays inlineable in the caller while the throwing path lives in a non-inlined method.
/// </summary>
internal static class ThrowHelper
{
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

    /// <summary>Returns <paramref name="value" /> when it has a value.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="value">The required value.</param>
    /// <param name="message">The exception message.</param>
    /// <returns><paramref name="value" />.</returns>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="value" /> is null.</exception>
    public static T RequiredValue<T>(T? value, string message)
        where T : struct
    {
        if (value == null)
            ThrowInvalidOperation(message);

        return value.Value;
    }

    /// <summary>Throws <paramref name="exception" />.</summary>
    /// <typeparam name="T">The expression type, so the call can sit inside an expression.</typeparam>
    /// <param name="exception">The exception to throw.</param>
    /// <returns>Never returns; the return type lets the call sit inside an expression.</returns>
    /// <exception cref="Exception">Always thrown: <paramref name="exception" />.</exception>
    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static T Throw<T>(Exception exception) => throw exception;

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowInvalidOperation(string message) => throw new InvalidOperationException(message);
}
