using System;
using Squirix.Attributes;

namespace Squirix;

/// <summary>Options used when creating a cache entry from a value.</summary>
/// <remarks>
/// Set <see cref="Expiration" /> or <see cref="ExpiresAt" /> to attach a TTL. When both are unset, the entry is stored
/// without expiration and does not expire by TTL.
/// </remarks>
[Immutable]
public sealed class CacheEntryOptions
{
    /// <summary>Gets the relative expiration to apply to the entry.</summary>
    public TimeSpan? Expiration { get; init; }

    /// <summary>Gets the absolute expiration timestamp to apply to the entry, on the client clock.</summary>
    /// <remarks>
    /// The client turns it into the time left before it when the write is sent, so the entry lives that long whatever the server clock
    /// reads. A timestamp that is not in the future on the client clock is rejected with <see cref="ArgumentOutOfRangeException" />.
    /// </remarks>
    public DateTimeOffset? ExpiresAt { get; init; }
}
