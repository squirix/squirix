using System;

namespace Squirix;

/// <summary>
/// Represents a cache item stored in Squirix. Contains the typed value and optional expiration metadata.
/// </summary>
/// <typeparam name="T">
/// The value type stored in the entry. Can be a primitive or a POCO serialized by the configured serializer.
/// </typeparam>
public sealed class CacheEntry<T>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CacheEntry{T}" /> class.
    /// </summary>
    public CacheEntry()
    {
    }

    /// <summary>
    /// Gets the absolute UTC expiration time. If set and reached, the entry is considered expired.
    /// Ignored if <see cref="Expiration" /> is provided.
    /// </summary>
    public DateTime? ExpiresUtc { get; init; }

    /// <summary>
    /// Gets the relative expiration. If provided, it takes precedence over <see cref="ExpiresUtc" />.
    /// </summary>
    public TimeSpan? Expiration { get; init; }

    /// <summary>
    /// Gets the value to store. May be <c>null</c>.
    /// </summary>
    public required T? Value { get; init; }
}
