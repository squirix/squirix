using System;

namespace Squirix;

/// <summary>
/// Outcome of a cache entry lookup.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
/// <param name="Found">Indicates whether the key was present and not expired.</param>
/// <param name="Entry">The cache entry when found.</param>
public readonly record struct CacheEntryResult<T>(bool Found, CacheEntry<T>? Entry)
{
    /// <summary>Gets the absolute expiration time when found.</summary>
    public DateTime? ExpiresUtc => Entry?.ExpiresUtc;

    /// <summary>Gets the entry value when found.</summary>
    public T? Value => Entry is null ? default : Entry.Value;
}
