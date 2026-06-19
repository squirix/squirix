using System;
using System.Globalization;

namespace Squirix.Server.Utils;

/// <summary>Produces PII-safe representations of cache keys for use in structured logs and transport errors.</summary>
internal static class CacheKeySanitizer
{
    private const int FullDisplayThreshold = 8;
    private const int MaxPrefixLength = 4;

    /// <summary>
    /// Returns a safe representation of <paramref name="key" /> suitable for log messages.
    /// </summary>
    /// <param name="key">The cache key to sanitize.</param>
    /// <returns>A PII-safe hint string.</returns>
    public static string Sanitize(string? key)
    {
        if (string.IsNullOrEmpty(key))
            return "(empty)";

        if (key.Length <= FullDisplayThreshold)
            return key;

        var len = key.Length;
        var digitCount = CountDecimalDigits(len);
        Span<char> dest = stackalloc char[MaxPrefixLength + 8 + digitCount + 1];
        key.AsSpan(0, MaxPrefixLength).CopyTo(dest);
        "***[len=".AsSpan().CopyTo(dest[MaxPrefixLength..]);
        const int writtenStart = MaxPrefixLength + 8;
        _ = len.TryFormat(dest.Slice(writtenStart, digitCount), out _, provider: CultureInfo.InvariantCulture);
        dest[writtenStart + digitCount] = ']';
        return new string(dest);
    }

    private static int CountDecimalDigits(int value)
    {
        var digits = 1;
        while (value >= 10)
        {
            value /= 10;
            digits++;
        }

        return digits;
    }
}
