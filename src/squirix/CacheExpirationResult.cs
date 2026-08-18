using System;
using Squirix.Attributes;

namespace Squirix;

/// <summary>Outcome of a cache expiration lookup.</summary>
/// <param name="Found">Indicates whether the key was present and not expired.</param>
/// <param name="HasExpiration">Indicates whether the live entry has an expiration.</param>
/// <param name="Expiration">The remaining expiration when the live entry has expiration.</param>
[Immutable]
public readonly record struct CacheExpirationResult(bool Found, bool HasExpiration, TimeSpan? Expiration) : IComparable<TimeSpan>, IComparable
{
    /// <summary>Gets the remaining expiration when the live entry has expiration.</summary>
    public TimeSpan? Value => Expiration;

    /// <summary>Compares the remaining expiration to a time span.</summary>
    /// <param name="result">Expiration result.</param>
    /// <param name="value">Time span to compare.</param>
    public static bool operator >(CacheExpirationResult result, TimeSpan value)
    {
        return result.CompareExpirationTo(value) > 0;
    }

    /// <summary>Compares the remaining expiration to a time span.</summary>
    /// <param name="result">Expiration result.</param>
    /// <param name="value">Time span to compare.</param>
    public static bool operator >=(CacheExpirationResult result, TimeSpan value)
    {
        return result.CompareExpirationTo(value) >= 0;
    }

    /// <summary>Compares the remaining expiration to a time span.</summary>
    /// <param name="result">Expiration result.</param>
    /// <param name="value">Time span to compare.</param>
    public static bool operator <(CacheExpirationResult result, TimeSpan value)
    {
        return result.CompareExpirationTo(value) < 0;
    }

    /// <summary>Compares the remaining expiration to a time span.</summary>
    /// <param name="result">Expiration result.</param>
    /// <param name="value">Time span to compare.</param>
    public static bool operator <=(CacheExpirationResult result, TimeSpan value)
    {
        return result.CompareExpirationTo(value) <= 0;
    }

    /// <summary>Compares the remaining expiration to a time span.</summary>
    /// <param name="other">Time span to compare.</param>
    /// <returns>A negative value when the remaining expiration is less than <paramref name="other" />, zero when equal, or a positive value when greater.</returns>
    public int CompareTo(TimeSpan other) => CompareExpirationTo(other);

    /// <summary>Compares the remaining expiration to a boxed time span.</summary>
    /// <param name="obj">Time span to compare, or null.</param>
    /// <returns>A negative value when the remaining expiration is less than <paramref name="obj" />, zero when equal, or a positive value when greater.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="obj" /> is not a <see cref="TimeSpan" />.</exception>
    public int CompareTo(object? obj)
    {
        if (obj == null)
            return 1;
        if (obj is TimeSpan span)
            return CompareTo(span);
        throw new ArgumentException("Object must be a TimeSpan.", nameof(obj));
    }

    private int CompareExpirationTo(TimeSpan value) => Expiration?.CompareTo(value) ?? -1;
}
