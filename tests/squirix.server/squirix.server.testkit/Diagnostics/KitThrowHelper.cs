using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Squirix.Server.TestKit.Diagnostics;

/// <summary>Throw-helper methods for test kits that may not reach the server's own helpers.</summary>
internal static class KitThrowHelper
{
    /// <summary>Returns <paramref name="value" /> when it is not null.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="value">The required value.</param>
    /// <param name="message">The exception message.</param>
    /// <returns><paramref name="value" />.</returns>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="value" /> is null.</exception>
    internal static T Required<T>(T? value, string message)
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
