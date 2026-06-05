using System;

namespace Squirix;

/// <summary>
/// Thrown when a cache mutation conflicts with an existing live entry.
/// </summary>
public sealed class CacheConflictException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CacheConflictException" /> class for a conflicting key.
    /// </summary>
    /// <param name="key">The cache key that already exists.</param>
    public CacheConflictException(string key)
        : base($"Key already exists: {key}")
    {
        Key = key ?? throw new ArgumentNullException(nameof(key));
    }

    /// <summary>
    /// Gets the conflicting cache key.
    /// </summary>
    public string Key { get; }
}
