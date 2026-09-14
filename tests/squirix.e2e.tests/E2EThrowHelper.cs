using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Squirix.E2ETests;

/// <summary>
/// Throw-helper methods for end-to-end test guards that have no BCL throw helper.
/// Each check stays inlineable in the caller while the throwing path lives in a non-inlined method.
/// </summary>
internal static class E2EThrowHelper
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

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowInvalidOperation(string message) => throw new InvalidOperationException(message);
}
