using System;
using Squirix.Attributes;

namespace Squirix;

/// <summary>Represents a cache item stored in Squirix. Contains the typed value and optional expiration metadata.</summary>
/// <typeparam name="T">The value type stored in the entry. Can be a primitive or a POCO serialized by the configured serializer.</typeparam>
[Immutable]
public sealed class CacheEntry<T>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CacheEntry{T}" /> class.
    /// </summary>
    public CacheEntry()
    {
    }

    /// <summary>
    /// Gets the relative expiration, measured from the entry write time. The entry expires at the earliest of this
    /// deadline and <see cref="ExpiresUtc" />, so the two combine instead of overriding each other.
    /// </summary>
    public TimeSpan? Expiration { get; init; }

    /// <summary>
    /// Gets the absolute UTC expiration time. The entry expires at the earliest of this time and the
    /// <see cref="Expiration" /> deadline.
    /// </summary>
    public DateTime? ExpiresUtc { get; init; }

    /// <summary>
    /// Gets the value to store. May be <see langword="null" />.
    /// </summary>
    public required T? Value { get; init; }
}
