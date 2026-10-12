using Squirix.Server.Attributes;
using Squirix.Server.Core;

namespace Squirix.Server.Node.Services;

/// <summary>One stored entry of the node together with the replica group that owns its key.</summary>
/// <param name="GroupId">The group that owns the key.</param>
/// <param name="CacheName">The cache the key belongs to.</param>
/// <param name="Key">The key.</param>
/// <param name="Entry">The stored entry; <see cref="NodeCacheEntry{T}.ExpiresUtc" /> is its absolute deadline.</param>
[Immutable]
internal readonly record struct ReplicaGroupEntry(string GroupId, string CacheName, string Key, NodeCacheEntry<object?> Entry);
