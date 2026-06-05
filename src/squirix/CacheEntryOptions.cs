using System;

namespace Squirix;

/// <summary>
/// Options used when creating a cache entry from a value.
/// </summary>
public sealed class CacheEntryOptions
{
    /// <summary>
    /// Gets the relative expiration to apply to the entry.
    /// </summary>
    public TimeSpan? Expiration { get; init; }

    /// <summary>
    /// Gets the absolute expiration timestamp to apply to the entry.
    /// </summary>
    public DateTimeOffset? ExpiresAt { get; init; }
}
