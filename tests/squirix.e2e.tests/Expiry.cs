using System;

namespace Squirix.E2ETests;

/// <summary>Concise factories for <see cref="CacheEntryOptions" /> used across integration tests.</summary>
internal static class Expiry
{
    /// <summary>Creates options that expire after the supplied relative duration.</summary>
    /// <param name="after">Time until expiry.</param>
    /// <returns>Cache entry options.</returns>
    public static CacheEntryOptions In(TimeSpan after) => new() { Expiration = after };

    /// <summary>Creates options that expire at the supplied absolute point in time.</summary>
    /// <param name="when">Absolute expiry time.</param>
    /// <returns>Cache entry options.</returns>
    public static CacheEntryOptions At(DateTimeOffset when) => new() { ExpiresAt = when };
}
