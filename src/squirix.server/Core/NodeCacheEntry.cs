using System;
using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using Squirix.Server.Attributes;

namespace Squirix.Server.Core;

/// <summary>
/// Represents a cache item in the server runtime. Contains the typed value, expiration metadata,
/// and optional extension-facing entry metadata (tags and monotonic version).
/// </summary>
/// <typeparam name="T">The value type stored in the entry. Can be a primitive or a POCO serialized by the configured serializer.</typeparam>
[Immutable]
public sealed class NodeCacheEntry<T>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="NodeCacheEntry{T}" /> class.
    /// </summary>
    public NodeCacheEntry()
    {
        Version = 1;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="NodeCacheEntry{T}" /> class.
    /// </summary>
    /// <param name="value">The value to store.</param>
    /// <param name="version">The monotonic entry version.</param>
    /// <param name="expiresUtc">The absolute UTC expiration time.</param>
    /// <param name="expiration">The relative expiration.</param>
    /// <param name="tags">Optional user-defined tags.</param>
    [SetsRequiredMembers]
    public NodeCacheEntry(T? value, long version = 1, DateTime? expiresUtc = null, TimeSpan? expiration = null, FrozenDictionary<string, string>? tags = null)
    {
        Value = value;
        Version = version;
        ExpiresUtc = expiresUtc;
        Expiration = expiration;
        Tags = tags;
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
    /// Gets optional user-defined tags for extension packages (for example tag invalidation).
    /// Not part of the v0.1 basic <c language="csharp">Squirix</c> client contract.
    /// </summary>
    public FrozenDictionary<string, string>? Tags { get; }

    /// <summary>
    /// Gets the value to store. May be <see langword="null" />.
    /// </summary>
    public required T? Value { get; init; }

    /// <summary>
    /// Gets the monotonic entry version used by extension packages for optimistic concurrency.
    /// Not part of the v0.1 basic <c language="csharp">Squirix</c> client contract.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is less than 1.</exception>
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

    internal object? Normalize()
    {
        if (Value is null or bool or string or byte[] or sbyte or byte or short or ushort or int or uint or long or float or double or decimal or JsonElement)
            return Value;

        // Serialize through object? so STJ resolves the runtime type, not the declared entry type T;
        // otherwise base/interface-declared entries lose derived properties before persistence.
        return SerializerProvider.Instance.SerializeToElement<object?>(Value);
    }
}
