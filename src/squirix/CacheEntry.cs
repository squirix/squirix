using System;
using Squirix.Attributes;

namespace Squirix;

/// <summary>Represents a cache item stored in Squirix. Contains the typed value and optional expiration metadata.</summary>
/// <typeparam name="T">The value type stored in the entry. Can be a primitive or a POCO serialized by the configured serializer.</typeparam>
[Immutable]
public sealed class CacheEntry<T>
{
    /// <summary>Initializes a new instance of the <see cref="CacheEntry{T}" /> class.</summary>
    public CacheEntry()
    {
    }

    /// <summary>
    /// Gets the relative expiration, measured from the entry write time. An entry read from the cache does not fill it: its deadline is
    /// reported through <see cref="ExpiresUtc" /> only.
    /// </summary>
    public TimeSpan? Expiration { get; init; }

    /// <summary>
    /// Gets the absolute UTC expiration time. The entry expires at the earliest of this time and the
    /// <see cref="Expiration" /> deadline. On an entry read from the cache it is on the client clock: the time the server reports
    /// the entry has left, added to the client time of the read.
    /// </summary>
    public DateTime? ExpiresUtc { get; init; }

    /// <summary>Gets the value to store. May be <see langword="null" />.</summary>
    public required T? Value { get; init; }
}
