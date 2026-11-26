using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

internal sealed partial class PhysicalCache<T>
{
    [ThreadSafe]
    private sealed class BoundedNode : Node
    {
        private int _position;
        private long _stamp;

        private BoundedNode(CacheKey key, NodeCacheEntry<T> entry, EvictionScope scope)
            : base(key, entry.Value, entry.ExpiresUtc, entry.Version, entry.Tags)
        {
            Scope = scope;
        }

        internal EvictionScope Scope { get; }

        internal BoundedNode? Older { get; set; }

        internal BoundedNode? Newer { get; set; }

        internal int Position
        {
            get => Volatile.Read(ref _position);
            set => Volatile.Write(ref _position, value);
        }

        internal long Stamp => Volatile.Read(ref _stamp);

        internal static (string Namespace, string Group) ScopeOf(PhysicalCache<T> owner, CacheKey key) => (owner._eviction.EvictOnMemoryPressure ? string.Empty : key.Namespace, owner._groupForKey?.Invoke(key) ?? string.Empty);

        internal static BoundedNode? Create(PhysicalCache<T> owner, CacheKey key, NodeCacheEntry<T> entry)
        {
            var scopeKey = ScopeOf(owner, key);
            if (!owner._scopes!.TryGetValue(scopeKey, out var scope))
            {
                scope = new EvictionScope(owner._eviction.Policy);
                owner._scopes.Add(scopeKey, scope);
            }

            BoundedNode? victim = null;
            if (owner._expiry == CacheExpiryAuthority.LocalClock && scope.Count >= owner._eviction.Capacity)
            {
                victim = scope.Victim();
                if (victim == null)
                    return null;
            }

            var node = new BoundedNode(key, entry, scope);
            scope.Add(node);
            if (victim != null)
            {
                owner._store.RemoveLocked(victim);
                owner.UntrackLocked(victim);
            }

            return node;
        }

        internal static void RecordAccess(Node node)
        {
            if (node is BoundedNode bounded)
                bounded.Scope.Touch(bounded);
        }

        internal void MarkRecent() => Volatile.Write(ref _stamp, Stopwatch.GetTimestamp());
    }

    /// <summary>Tracks entries of one namespace and original owner. Only LFU's first access mutates its list from a reader.</summary>
    [ThreadSafe]
    private sealed class EvictionScope
    {
        private const int SampleSize = 16;
        private readonly Lock _gate = new();
        private readonly List<BoundedNode>? _sample;
        private readonly EvictionPolicyType _policy;
        private int _cursor;
        private BoundedNode? _oldest;
        private BoundedNode? _newest;

        internal EvictionScope(EvictionPolicyType policy)
        {
            _policy = policy;
            _sample = policy == EvictionPolicyType.Lru ? [] : null;
        }

        internal int Count { get; private set; }

        internal void Add(BoundedNode node)
        {
            Count++;
            if (_sample != null)
            {
                node.MarkRecent();
                node.Position = _sample.Count;
                _sample.Add(node);
                return;
            }

            lock (_gate)
            {
                node.Older = _newest;
                if (_newest != null)
                    _newest.Newer = node;
                else
                    _oldest = node;
                _newest = node;
            }
        }

        internal void Touch(BoundedNode node)
        {
            if (_sample != null)
                node.MarkRecent();
            else if (_policy == EvictionPolicyType.Lfu && node.Position >= 0)
                Unlink(node);
        }

        internal void Remove(BoundedNode node)
        {
            Count--;
            if (_sample == null)
            {
                Unlink(node);
                return;
            }

            var last = _sample[^1];
            var position = node.Position;
            _sample[position] = last;
            last.Position = position;
            _sample.RemoveAt(_sample.Count - 1);
            if (_sample.Capacity > SampleSize && _sample.Count <= _sample.Capacity / 4)
                _sample.TrimExcess();
        }

        internal BoundedNode? Victim()
        {
            if (_sample == null)
            {
                lock (_gate)
                    return _oldest;
            }

            var count = _sample.Count;
            var samples = Math.Min(count, SampleSize);
            var position = _cursor < count ? _cursor : 0;
            var oldest = _sample[position];
            var stamp = oldest.Stamp;
            for (var i = 1; i < samples; i++)
            {
                position = position + 1 == count ? 0 : position + 1;
                var candidate = _sample[position];
                var observed = candidate.Stamp;
                if (observed < stamp)
                {
                    stamp = observed;
                    oldest = candidate;
                }
            }

            _cursor = position + 1;
            return oldest;
        }

        private void Unlink(BoundedNode node)
        {
            lock (_gate)
            {
                if (node.Position < 0)
                    return;

                if (node.Older != null)
                    node.Older.Newer = node.Newer;
                else
                    _oldest = node.Newer;
                if (node.Newer != null)
                    node.Newer.Older = node.Older;
                else
                    _newest = node.Older;

                node.Position = -1;
                node.Older = null;
                node.Newer = null;
            }
        }
    }
}
