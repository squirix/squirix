using System;
using System.Collections.Frozen;
using Squirix.Server.Attributes;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>An entry’s value, version, expiry and tags observed in a single read.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
/// <param name="Value">The cached value.</param>
/// <param name="Version">The version.</param>
/// <param name="ExpiresUtc">The deadline, or <see langword="null" /> for none.</param>
/// <param name="Tags">The tags.</param>
[Immutable]
internal readonly record struct CacheEntryRead<T>(T? Value, long Version, DateTime? ExpiresUtc, FrozenDictionary<string, string>? Tags)
{
    /// <summary>Builds the entry a read returns.</summary>
    /// <returns>A new entry with these fields.</returns>
    internal NodeCacheEntry<T> ToEntry() => new(Value, Version, ExpiresUtc, tags: Tags);
}
