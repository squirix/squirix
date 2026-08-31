using System;
using System.Globalization;

namespace Squirix.Server.Utils;

/// <summary>Cached invariant digit strings for small non-negative integers used in telemetry tags.</summary>
internal static class InvariantDigitStrings
{
    private const int CachedD6Count = 10_000;
    private const int CachedNonNegativeCount = 1024;
    private static readonly string[] CachedNonNegative = CreateCachedNonNegative();
    private static readonly string[] CachedD6 = CreateCachedD6();

    internal static string Format(double value) => value.ToString("G17", CultureInfo.InvariantCulture);

    internal static string Format(int value)
    {
        if (value is >= 0 and < CachedNonNegativeCount)
            return CachedNonNegative[value];

        return value.ToString(CultureInfo.InvariantCulture);
    }

    internal static string Format(long value)
    {
        if (value is >= 0 and < CachedNonNegativeCount)
            return CachedNonNegative[Convert.ToInt32(value)];

        return value.ToString(CultureInfo.InvariantCulture);
    }

    internal static string Format(ulong value)
    {
        if (value < CachedNonNegativeCount)
            return CachedNonNegative[Convert.ToInt32(value)];

        return value.ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>Formats <paramref name="value" /> as a zero-padded D6 string (journal/snapshot segment indexes).</summary>
    /// <param name="value">The value to format.</param>
    /// <returns>A D6 invariant digit string.</returns>
    internal static string FormatD6(int value)
    {
        if (value is >= 0 and < CachedD6Count)
            return CachedD6[value];

        return value.ToString("D6", CultureInfo.InvariantCulture);
    }

    /// <summary>Builds <c language="csharp">https://{host}:{port}</c> in a single allocation.</summary>
    /// <param name="host">Host name or address.</param>
    /// <param name="port">TCP port.</param>
    /// <returns>An absolute HTTPS origin string.</returns>
    internal static string FormatHttpsOrigin(string host, int port)
    {
        ArgumentNullException.ThrowIfNull(host);

        var ipv6 = host.Contains(':', StringComparison.Ordinal);
        var digitLength = CountDigits(port);
        return string.Create(
            8 + (ipv6 ? host.Length + 2 : host.Length) + 1 + digitLength,
            (host, port, ipv6),
            static (span, state) =>
            {
                "https://".AsSpan().CopyTo(span);
                var offset = 8;
                if (state.ipv6)
                    span[offset++] = '[';
                state.host.AsSpan().CopyTo(span[offset..]);
                offset += state.host.Length;
                if (state.ipv6)
                    span[offset++] = ']';
                span[offset] = ':';
                _ = state.port.TryFormat(span[(offset + 1)..], out _, provider: CultureInfo.InvariantCulture);
            });
    }

    private static int CountDigits(int value)
    {
        if (value == 0)
            return 1;

        if (value < 0)
            return CountDigits(-value) + 1;

        var digits = 0;
        var remaining = value;
        while (remaining > 0)
        {
            digits++;
            remaining /= 10;
        }

        return digits;
    }

    private static string[] CreateCachedD6()
    {
        var values = new string[CachedD6Count];
        for (var i = 0; i < values.Length; i++)
            values[i] = i.ToString("D6", CultureInfo.InvariantCulture);

        return values;
    }

    private static string[] CreateCachedNonNegative()
    {
        var values = new string[CachedNonNegativeCount];
        for (var i = 0; i < values.Length; i++)
            values[i] = i.ToString(CultureInfo.InvariantCulture);

        return values;
    }
}
