using System;
using System.Collections.Frozen;

namespace Squirix.Server;

/// <summary>
/// Represents a cache item in the server runtime. Contains the typed value, expiration metadata,
/// and optional extension-facing entry metadata (tags and monotonic version).
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
        Version = 1;
    }

    /// <summary>
    /// Gets the relative expiration. If provided, it takes precedence over <see cref="ExpiresUtc" />.
    /// </summary>
    public TimeSpan? Expiration { get; init; }

    /// <summary>
    /// Gets the absolute UTC expiration time. If set and reached, the entry is considered expired.
    /// Ignored if <see cref="Expiration" /> is provided.
    /// </summary>
    public DateTime? ExpiresUtc { get; init; }

    /// <summary>
    /// Gets optional user-defined tags for extension packages (for example tag invalidation).
    /// Not part of the v0.1 basic <c>Squirix</c> client contract.
    /// </summary>
    public FrozenDictionary<string, string>? Tags { get; init; }

    /// <summary>
    /// Gets the value to store. May be <c>null</c>.
    /// </summary>
    public required T? Value { get; init; }

    /// <summary>
    /// Gets the monotonic entry version used by extension packages for optimistic concurrency.
    /// Not part of the v0.1 basic <c>Squirix</c> client contract.
    /// </summary>
    public long Version
    {
        get;
        init
        {
            if (value < 1)
                throw new ArgumentOutOfRangeException(nameof(value), "Version must be >= 1.");

            field = value;
        }
    }
}
