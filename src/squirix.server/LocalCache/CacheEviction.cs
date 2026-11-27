using System;
using System.Collections.Generic;
using Squirix.Server.Attributes;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>Maintains eviction order per namespace and owner using the physical cache’s write lock.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
[Mutable]
internal sealed class CacheEviction<T>
{
    private readonly EvictionOptions _eviction;
    private readonly CacheExpiryAuthority _expiry;
    private readonly Func<CacheKey, string>? _groupForKey;
    private readonly Dictionary<(string Namespace, string Group), EvictionScope<T>>? _scopes;
    private readonly SlotStore<T> _store;

    internal CacheEviction(EvictionOptions eviction, CacheExpiryAuthority expiry, Func<CacheKey, string>? groupForKey, SlotStore<T> store)
    {
        _eviction = eviction;
        _expiry = expiry;
        _groupForKey = groupForKey;
        _store = store;
        if (!eviction.Enabled)
            return;
        if (eviction.Capacity <= 0 || (eviction.EvictOnMemoryPressure && (eviction.Capacity != null || eviction.Policy != EvictionPolicyType.Lru)))
            throw new ArgumentOutOfRangeException(nameof(eviction));
        _scopes = [];
    }

    internal bool Enabled => _scopes != null;

    internal bool HasCapacity => _eviction.Capacity != null;

    /// <summary>Returns the key to evict for an insertion under the leader's serialized commit gate.</summary>
    /// <param name="key">The key being inserted.</param>
    /// <returns>The victim, or null if this write needs no eviction.</returns>
    internal CacheKey? SelectCapacityVictimLocked(CacheKey key)
    {
        if (!HasCapacity || _scopes == null || _store.TryGetLocked(key, out _) ||
            !_scopes.TryGetValue(ScopeOf(key), out var scope) || scope.Count < _eviction.Capacity)
            return null;

        return scope.Victim()?.Occupant.Key ?? key;
    }

    internal CacheKey? SelectPressureVictimLocked(string group) =>
        _eviction.EvictOnMemoryPressure && _scopes!.TryGetValue((string.Empty, group), out var scope)
            ? scope.Victim()?.Occupant.Key
            : null;

    internal CacheSlot<T> Add(CacheKey key, int hash, NodeCacheEntry<T> entry, long? bytes, out CacheSlot<T> removed)
    {
        removed = default;
        var scopeKey = ScopeOf(key);
        if (!_scopes!.TryGetValue(scopeKey, out var scope))
        {
            scope = new EvictionScope<T>(_eviction.Policy);
            _scopes.Add(scopeKey, scope);
        }

        EvictionNode<T>? victim = null;
        if (_expiry == CacheExpiryAuthority.LocalClock && scope.Count >= _eviction.Capacity)
        {
            victim = scope.Victim();
            if (victim == null)
                return default;
        }

        var node = new EvictionNode<T>(scope);
        scope.Add(node);
        try
        {
            node.Occupant = _store.AddLocked(key, hash, entry, node, bytes);
        }
        catch
        {
            scope.Remove(node);
            if (scope.Count == 0)
                _ = _scopes.Remove(scopeKey);
            throw;
        }

        if (victim == null)
            return node.Occupant;

        _store.RemoveLocked(victim.Occupant);
        removed = victim.Occupant;

        return node.Occupant;
    }

    internal void Remove(in CacheSlot<T> node, EvictionNode<T> bounded)
    {
        bounded.Scope.Remove(bounded);
        if (bounded.Scope.Count == 0 && _scopes != null)
            _ = _scopes.Remove(ScopeOf(node.Key));
    }

    private (string Namespace, string Group) ScopeOf(CacheKey key) => (_eviction.EvictOnMemoryPressure ? string.Empty : key.Namespace, _groupForKey?.Invoke(key) ?? string.Empty);
}
