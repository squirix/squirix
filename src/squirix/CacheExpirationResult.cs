using System;

namespace Squirix;

/// <summary>
/// Outcome of a cache expiration lookup.
/// </summary>
/// <param name="Found">Indicates whether the key was present and not expired.</param>
/// <param name="HasExpiration">Indicates whether the live entry has an expiration.</param>
/// <param name="Expiration">The remaining expiration when the live entry has expiration.</param>
public readonly record struct CacheExpirationResult(bool Found, bool HasExpiration, TimeSpan? Expiration)
{
    /// <summary>Gets the remaining expiration when the live entry has expiration.</summary>
    public TimeSpan? Value => Expiration;

    /// <summary>Compares the remaining expiration to a time span.</summary>
    /// <param name="result">Expiration result.</param>
    /// <param name="value">Time span to compare.</param>
    public static bool operator <=(CacheExpirationResult result, TimeSpan value)
    {
        return result.Expiration <= value;
    }

    /// <summary>Compares the remaining expiration to a time span.</summary>
    /// <param name="result">Expiration result.</param>
    /// <param name="value">Time span to compare.</param>
    public static bool operator >=(CacheExpirationResult result, TimeSpan value)
    {
        return result.Expiration >= value;
    }

    /// <summary>Compares the remaining expiration to a time span.</summary>
    /// <param name="result">Expiration result.</param>
    /// <param name="value">Time span to compare.</param>
    public static bool operator <(CacheExpirationResult result, TimeSpan value)
    {
        return result.Expiration < value;
    }

    /// <summary>Compares the remaining expiration to a time span.</summary>
    /// <param name="result">Expiration result.</param>
    /// <param name="value">Time span to compare.</param>
    public static bool operator >(CacheExpirationResult result, TimeSpan value)
    {
        return result.Expiration > value;
    }
}
