using System;
using System.Collections.Generic;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>Tracks entries of one namespace and original owner. Only LFU’s first access mutates its list from a reader.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
[ThreadSafe]
internal sealed class EvictionScope<T>
{
    private const int SampleSize = 16;
    private readonly Lock _gate = new();
    private readonly List<EvictionNode<T>>? _sample;
    private readonly EvictionPolicyType _policy;
    private int _cursor;
    private EvictionNode<T>? _oldest;
    private EvictionNode<T>? _newest;

    internal EvictionScope(EvictionPolicyType policy)
    {
        _policy = policy;
        _sample = policy == EvictionPolicyType.Lru ? [] : null;
    }

    internal int Count { get; private set; }

    internal void Add(EvictionNode<T> node)
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

    internal void Touch(EvictionNode<T> node)
    {
        if (_sample != null)
            node.MarkRecent();
        else if (_policy == EvictionPolicyType.Lfu && node.Position >= 0)
            Unlink(node);
    }

    internal void Remove(EvictionNode<T> node)
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

    internal EvictionNode<T>? Victim()
    {
        if (_sample == null)
        {
            lock (_gate)
                return _oldest;
        }

        var count = _sample.Count;
        if (count == 0)
            return null;
        var samples = Math.Min(count, SampleSize);
        var position = _cursor < count ? _cursor : 0;
        var oldest = _sample[position];
        var stamp = oldest.Stamp;
        for (var i = 1; i < samples; i++)
        {
            position = position + 1 == count ? 0 : position + 1;
            var candidate = _sample[position];
            var observed = candidate.Stamp;
            if (observed >= stamp)
                continue;
            stamp = observed;
            oldest = candidate;
        }

        _cursor = position + 1;
        return oldest;
    }

    private void Unlink(EvictionNode<T> node)
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
