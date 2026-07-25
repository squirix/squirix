using System;
using System.Globalization;

namespace Squirix.TestKit;

/// <summary>Cached invariant digit strings for small non-negative integers used in tests.</summary>
public static class InvariantIndexStrings
{
    private const int CachedNonNegativeCount = 1024;

    private static readonly string[] CachedNonNegative = CreateCachedNonNegative();

    /// <summary>Formats a non-negative integer with invariant culture, reusing cached strings for 0..1023.</summary>
    /// <param name="value">The value to format.</param>
    /// <returns>An invariant digit string.</returns>
    public static string Format(int value)
    {
        if (value is >= 0 and < CachedNonNegativeCount)
            return CachedNonNegative[value];

        return value.ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>Formats a non-negative long with invariant culture, reusing cached strings for 0..1023.</summary>
    /// <param name="value">The value to format.</param>
    /// <returns>An invariant digit string.</returns>
    public static string Format(long value)
    {
        if (value is >= 0 and < CachedNonNegativeCount)
            return CachedNonNegative[Convert.ToInt32(value)];

        return value.ToString(CultureInfo.InvariantCulture);
    }

    private static string[] CreateCachedNonNegative()
    {
        var values = new string[CachedNonNegativeCount];
        for (var i = 0; i < values.Length; i++)
            values[i] = i.ToString(CultureInfo.InvariantCulture);

        return values;
    }
}
