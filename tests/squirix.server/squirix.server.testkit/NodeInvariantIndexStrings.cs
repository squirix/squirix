using System;
using System.Globalization;

namespace Squirix.Server.TestKit;

/// <summary>Cached and single-allocation invariant index/string helpers for tests and benchmarks.</summary>
public static class NodeInvariantIndexStrings
{
    private const int CachedNonNegativeCount = 1024;
    private static readonly string[] CachedNonNegative = CreateCachedNonNegative();
    private static readonly string[] CachedD4 = CreateCachedPadded(4, 10_000);

    private static readonly string[] CachedD6 = CreateCachedPadded(6, 10_000);

    private static readonly string[] CachedD8 = CreateCachedPadded(8, 10_000);

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

    /// <summary>Formats <paramref name="index" /> as a zero-padded D4 string.</summary>
    /// <param name="index">The index to format.</param>
    /// <returns>A D4 invariant digit string.</returns>
    public static string FormatD4(int index)
    {
        if (index is >= 0 and < 10_000)
            return CachedD4[index];

        return index.ToString("D4", CultureInfo.InvariantCulture);
    }

    /// <summary>Formats <paramref name="index" /> as a zero-padded D6 string (journal/snapshot segment indexes).</summary>
    /// <param name="index">The index to format.</param>
    /// <returns>A D6 invariant digit string.</returns>
    public static string FormatD6(int index)
    {
        if (index is >= 0 and < 10_000)
            return CachedD6[index];

        return index.ToString("D6", CultureInfo.InvariantCulture);
    }

    /// <summary>Formats <paramref name="index" /> as a zero-padded D8 string.</summary>
    /// <param name="index">The index to format.</param>
    /// <returns>A D8 invariant digit string.</returns>
    public static string FormatD8(int index)
    {
        if (index is >= 0 and < 10_000)
            return CachedD8[index];

        return index.ToString("D8", CultureInfo.InvariantCulture);
    }

    /// <summary>Builds <c>https://{host}:{port}{absolutePath}</c> in a single allocation.</summary>
    /// <param name="host">Host name or address.</param>
    /// <param name="port">TCP port.</param>
    /// <param name="absolutePath">Absolute path beginning with <c>/</c>.</param>
    /// <returns>An absolute HTTPS URL.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="host" /> or <paramref name="absolutePath" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="absolutePath" /> is empty or does not begin with <c>/</c>.</exception>
    public static string FormatHttpsAbsolute(string host, int port, string absolutePath)
    {
        ArgumentNullException.ThrowIfNull(host);
        ArgumentNullException.ThrowIfNull(absolutePath);
        if (absolutePath.Length is 0 || absolutePath[0] is not '/')
            throw new ArgumentException("Absolute path must begin with '/'.", nameof(absolutePath));

        var digitLength = CountDigits(port);
        return string.Create(
            8 + host.Length + 1 + digitLength + absolutePath.Length,
            (host, port, absolutePath),
            static (span, state) =>
            {
                "https://".AsSpan().CopyTo(span);
                state.host.AsSpan().CopyTo(span[8..]);
                var afterHost = 8 + state.host.Length;
                span[afterHost] = ':';
                _ = state.port.TryFormat(span[(afterHost + 1)..], out var written, provider: CultureInfo.InvariantCulture);
                state.absolutePath.AsSpan().CopyTo(span[(afterHost + 1 + written)..]);
            });
    }

    /// <summary>Builds <c>https://{host}:{port}</c> in a single allocation.</summary>
    /// <param name="host">Host name or address.</param>
    /// <param name="port">TCP port.</param>
    /// <returns>An absolute HTTPS origin string.</returns>
    public static string FormatHttpsOrigin(string host, int port) => FormatOrigin("https", host, port);

    /// <summary>Builds <c>{scheme}://{host}:{port}</c> in a single allocation.</summary>
    /// <param name="scheme">URI scheme such as <c>https</c> or <c>http</c>.</param>
    /// <param name="host">Host name or address.</param>
    /// <param name="port">TCP port.</param>
    /// <returns>An absolute origin string.</returns>
    public static string FormatOrigin(string scheme, string host, int port)
    {
        ArgumentNullException.ThrowIfNull(scheme);
        ArgumentNullException.ThrowIfNull(host);

        var digitLength = CountDigits(port);
        return string.Create(
            scheme.Length + 3 + host.Length + 1 + digitLength,
            (scheme, host, port),
            static (span, state) =>
            {
                state.scheme.AsSpan().CopyTo(span);
                span[state.scheme.Length] = ':';
                span[state.scheme.Length + 1] = '/';
                span[state.scheme.Length + 2] = '/';
                state.host.AsSpan().CopyTo(span[(state.scheme.Length + 3)..]);
                span[state.scheme.Length + 3 + state.host.Length] = ':';
                _ = state.port.TryFormat(span[(state.scheme.Length + 4 + state.host.Length)..], out _, provider: CultureInfo.InvariantCulture);
            });
    }

    /// <summary>Builds <c>{prefix}{guid:N}</c> in a single allocation.</summary>
    /// <param name="prefix">Literal prefix.</param>
    /// <returns>The composed name.</returns>
    public static string FormatPrefixedGuidN(string prefix)
    {
        ArgumentNullException.ThrowIfNull(prefix);

        var guid = Guid.NewGuid();
        return string.Create(
            prefix.Length + 32,
            (prefix, guid),
            static (span, state) =>
            {
                state.prefix.AsSpan().CopyTo(span);
                _ = state.guid.TryFormat(span[state.prefix.Length..], out _, "N");
            });
    }

    /// <summary>Builds <c>{prefix}{middle}{suffix}</c> in a single allocation.</summary>
    /// <param name="prefix">Literal prefix.</param>
    /// <param name="middle">Middle segment (for example a random file name).</param>
    /// <param name="suffix">Literal suffix.</param>
    /// <returns>The composed name.</returns>
    public static string FormatPrefixedMiddleSuffix(string prefix, string middle, string suffix)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentNullException.ThrowIfNull(middle);
        ArgumentNullException.ThrowIfNull(suffix);

        return string.Create(
            prefix.Length + middle.Length + suffix.Length,
            (prefix, middle, suffix),
            static (span, state) =>
            {
                state.prefix.AsSpan().CopyTo(span);
                state.middle.AsSpan().CopyTo(span[state.prefix.Length..]);
                state.suffix.AsSpan().CopyTo(span[(state.prefix.Length + state.middle.Length)..]);
            });
    }

    /// <summary>Builds <c>/c mklink /J "{link}" "{target}"</c> in a single allocation.</summary>
    /// <param name="linkPath">Junction link path.</param>
    /// <param name="targetPath">Junction target path.</param>
    /// <returns>cmd.exe arguments for <c>mklink /J</c>.</returns>
    public static string FormatMklinkJunctionArguments(string linkPath, string targetPath)
    {
        ArgumentNullException.ThrowIfNull(linkPath);
        ArgumentNullException.ThrowIfNull(targetPath);

        const string head = "/c mklink /J \"";
        const string mid = "\" \"";
        const string tail = "\"";
        return string.Create(
            head.Length + linkPath.Length + mid.Length + targetPath.Length + tail.Length,
            (linkPath, targetPath),
            static (span, state) =>
            {
                head.AsSpan().CopyTo(span);
                var at = head.Length;
                state.linkPath.AsSpan().CopyTo(span[at..]);
                at += state.linkPath.Length;
                mid.AsSpan().CopyTo(span[at..]);
                at += mid.Length;
                state.targetPath.AsSpan().CopyTo(span[at..]);
                at += state.targetPath.Length;
                tail.AsSpan().CopyTo(span[at..]);
            });
    }

    /// <summary>Builds <c>{prefix}:{index}</c> in a single allocation.</summary>
    /// <param name="prefix">Key prefix.</param>
    /// <param name="index">Numeric suffix.</param>
    /// <returns>The composed key.</returns>
    public static string FormatPrefixed(string prefix, int index)
    {
        ArgumentNullException.ThrowIfNull(prefix);

        var digitLength = CountDigits(index);
        return string.Create(
            prefix.Length + 1 + digitLength,
            (prefix, index),
            static (span, state) =>
            {
                state.prefix.AsSpan().CopyTo(span);
                span[state.prefix.Length] = ':';
                _ = state.index.TryFormat(span[(state.prefix.Length + 1)..], out _, provider: CultureInfo.InvariantCulture);
            });
    }

    /// <summary>Builds <c>{prefix}:{index}</c> with a fixed pad format in a single allocation.</summary>
    /// <param name="prefix">Key prefix.</param>
    /// <param name="index">Numeric suffix.</param>
    /// <param name="format">A standard numeric format such as <c>D5</c> or <c>D10</c>.</param>
    /// <param name="width">Zero-pad width matching <paramref name="format" />.</param>
    /// <returns>The composed key.</returns>
    public static string FormatPrefixedPadded(string prefix, int index, string format, int width)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentNullException.ThrowIfNull(format);
        ArgumentOutOfRangeException.ThrowIfLessThan(width, 1);

        return string.Create(
            prefix.Length + 1 + width,
            (prefix, index, format),
            static (span, state) =>
            {
                state.prefix.AsSpan().CopyTo(span);
                span[state.prefix.Length] = ':';
                _ = state.index.TryFormat(span[(state.prefix.Length + 1)..], out _, state.format, CultureInfo.InvariantCulture);
            });
    }

    private static int CountDigits(int value)
    {
        if (value is 0)
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

    private static string[] CreateCachedNonNegative()
    {
        var values = new string[CachedNonNegativeCount];
        for (var i = 0; i < values.Length; i++)
            values[i] = i.ToString(CultureInfo.InvariantCulture);

        return values;
    }

    private static string[] CreateCachedPadded(int width, int count)
    {
        var format = width switch
        {
            4 => "D4",
            6 => "D6",
            8 => "D8",
            10 => "D10",
            _ => throw new ArgumentOutOfRangeException(nameof(width), width, "Unsupported pad width."),
        };
        var values = new string[count];
        for (var i = 0; i < values.Length; i++)
            values[i] = i.ToString(format, CultureInfo.InvariantCulture);

        return values;
    }
}
