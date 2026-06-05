using System;
using System.Collections.Generic;
using System.Threading;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>
/// Tracks per-key ordering and frequency metadata used for capacity-based eviction (LRU, LFU, FIFO).
/// </summary>
internal sealed class LocalEvictionIndex
{
    private readonly Lock _lock = new();
    private readonly Dictionary<CacheKey, (LinkedListNode<CacheKey> Node, long Freq)> _meta = [];
    private readonly EvictionOptions _options;
    private readonly LinkedList<CacheKey> _order = [];

    internal LocalEvictionIndex(EvictionOptions options)
    {
        _options = options;
    }

    /// <summary>
    /// Gets the bounded capacity limit when configured.
    /// </summary>
    internal int? BoundedCapacity => _options.Capacity;

    internal void TouchExisting(CacheKey key)
    {
        if (_options.Capacity is null)
            return;

        lock (_lock)
        {
            if (!_meta.TryGetValue(key, out var m))
                return;

            switch (_options.Policy)
            {
                case EvictionPolicyType.Fifo:
                    break;

                case EvictionPolicyType.Lru:
                    _order.Remove(m.Node);
                    var newNode = _order.AddFirst(key);
                    _meta[key] = (newNode, m.Freq + 1);
                    break;

                case EvictionPolicyType.Lfu:
                    _meta[key] = (m.Node, m.Freq + 1);
                    break;

                default:
                    _order.Remove(m.Node);
                    var nn = _order.AddFirst(key);
                    _meta[key] = (nn, m.Freq + 1);
                    break;
            }
        }
    }

    internal void TrackNew(CacheKey key)
    {
        if (_options.Capacity is null)
            return;

        lock (_lock)
        {
            if (_meta.ContainsKey(key))
                return;

            var node = _order.AddFirst(key);
            _meta[key] = (node, 1);
        }
    }

    internal bool TryPopEvictionVictim(out CacheKey victim)
    {
        victim = default;
        if (_options.Capacity is null)
            return false;

        lock (_lock)
        {
            if (_meta.Count == 0)
                return false;

            var candidate = _options.Policy switch
            {
                EvictionPolicyType.Fifo => _order.Last?.Value,
                EvictionPolicyType.Lru => _order.Last?.Value,
                EvictionPolicyType.Lfu => GetLeastFrequentlyUsedKey(),
                _ => throw new InvalidOperationException($"Unsupported eviction policy: {_options.Policy}."),
            };

            if (candidate is not { } selected)
                return false;

            victim = selected;

            if (!_meta.TryGetValue(victim, out var metadata))
                return true;
            _order.Remove(metadata.Node);
            _ = _meta.Remove(victim);
        }

        return true;
    }

    internal void Untrack(CacheKey key)
    {
        if (_options.Capacity is null)
            return;

        lock (_lock)
        {
            if (!_meta.TryGetValue(key, out var m))
                return;

            _order.Remove(m.Node);
            _ = _meta.Remove(key);
        }
    }

    private CacheKey? GetLeastFrequentlyUsedKey()
    {
        CacheKey? chosen = null;
        var minFrequency = long.MaxValue;

        foreach (var pair in _meta)
        {
            if (pair.Value.Freq >= minFrequency)
                continue;

            minFrequency = pair.Value.Freq;
            chosen = pair.Key;
        }

        return chosen;
    }
}
