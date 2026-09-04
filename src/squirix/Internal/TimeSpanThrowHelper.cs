using System;
using System.Runtime.CompilerServices;

namespace Squirix.Internal;

/// <summary>
/// Throw-helper extensions for <see cref="TimeSpan" /> range guards. The BCL
/// <c language="csharp">ArgumentOutOfRangeException.ThrowIf*</c> helpers require
/// <c language="csharp">INumberBase&lt;T&gt;</c>, which <see cref="TimeSpan" /> does not implement,
/// so range checks against <see cref="TimeSpan.Zero" /> use these helpers instead.
/// Each check stays inlineable in the caller while the throwing path lives in a non-inlined method.
/// </summary>
internal static class TimeSpanThrowHelper
{
    /// <summary>Throws <see cref="ArgumentOutOfRangeException" /> when <paramref name="value" /> is negative.</summary>
    /// <param name="value">The value to validate.</param>
    /// <param name="paramName">The caller parameter name for exceptions.</param>
    /// <param name="message">The exception message. Uses a default message when null.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void ThrowIfNegative(this TimeSpan value, string paramName, string? message = null)
    {
        if (value < TimeSpan.Zero)
            ThrowNegative(value, paramName, message ?? "Value cannot be negative.");
    }

    /// <summary>Throws <see cref="ArgumentOutOfRangeException" /> when <paramref name="value" /> is zero or negative.</summary>
    /// <param name="value">The value to validate.</param>
    /// <param name="paramName">The caller parameter name for exceptions.</param>
    /// <param name="message">The exception message. Uses a default message when null.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void ThrowIfNegativeOrZero(this TimeSpan value, string paramName, string? message = null)
    {
        if (value <= TimeSpan.Zero)
            ThrowNegativeOrZero(value, paramName, message ?? "Value must be greater than zero.");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowNegative(TimeSpan value, string paramName, string message) => throw new ArgumentOutOfRangeException(paramName, value, message);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowNegativeOrZero(TimeSpan value, string paramName, string message) => throw new ArgumentOutOfRangeException(paramName, value, message);
}
