using System;

namespace Squirix;

/// <summary>
/// Outcome of a cache expiration lookup.
/// </summary>
/// <param name="Found">Indicates whether the key was present and not expired.</param>
/// <param name="HasExpiration">Indicates whether the live entry has an expiration.</param>
/// <param name="Expiration">The remaining expiration when the live entry has expiration.</param>
public readonly record struct CacheExpirationResult(bool Found, bool HasExpiration, TimeSpan? Expiration) : IComparable<TimeSpan>
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
    /// <param name="value">Time span to compare.</param>
    /// <returns>A negative value when the remaining expiration is less than <paramref name="value" />, zero when equal, or a positive value when greater.</returns>
    public int CompareTo(TimeSpan value) => CompareExpirationTo(value);

    private int CompareExpirationTo(TimeSpan value) => Expiration?.CompareTo(value) ?? -1;
}
