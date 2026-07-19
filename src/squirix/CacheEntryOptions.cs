using System;

namespace Squirix;

/// <summary>Options used when creating a cache entry from a value.</summary>
/// <remarks>
/// Set <see cref="Expiration" /> or <see cref="ExpiresAt" /> to attach a TTL. When both are unset, the entry is stored
/// without expiration and does not expire by TTL.
/// </remarks>
public sealed class CacheEntryOptions
{
    /// <summary>Gets the relative expiration to apply to the entry.</summary>
    public TimeSpan? Expiration { get; init; }

    /// <summary>Gets the absolute expiration timestamp to apply to the entry.</summary>
    public DateTimeOffset? ExpiresAt { get; init; }
}
