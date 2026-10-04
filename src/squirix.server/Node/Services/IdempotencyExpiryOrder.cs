using System;
using System.Collections.Generic;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Services;

/// <summary>Orders idempotency records for expiry and capacity eviction so that both cost the work they do, not the store size.</summary>
/// <remarks>
/// Not thread-safe: the owning store calls it under its gate. An entry carries the insertion sequence of the record it was made for;
/// a record that was removed or replaced leaves a stale entry, recognised through <see cref="LiveLookup" /> and skipped
/// when it reaches a head. <see cref="Compact" /> bounds the stale entries by the store size.
/// <para>
/// A record created in this process expires once both its monotonic and its wall age pass the retention. Its monotonic age is
/// non-increasing in creation order, so the first stage pops the records whose monotonic age passed and hands them to the second,
/// which orders by wall creation time (restored records join it directly) and releases those whose wall age passed too. Wall time
/// is not ordered along insertion (clock steps, restored records), hence the heap instead of a plain queue.
/// </para>
/// </remarks>
[Mutable]
internal sealed class IdempotencyExpiryOrder
{
    private const int MinCompactionEntries = 64;

    private readonly Queue<Entry> _completedCreated = new();
    private readonly Queue<Entry> _completedRestored = new();
    private readonly Queue<Entry> _createdOrder = new();
    private readonly LiveLookup _lookup;
    private readonly TimeProvider _timeProvider;
    private readonly PriorityQueue<Entry, long> _wallOrder = new();

    internal IdempotencyExpiryOrder(TimeProvider timeProvider, LiveLookup lookup)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(lookup);
        _timeProvider = timeProvider;
        _lookup = lookup;
    }

    /// <summary>Resolves whether the record generation identified by <paramref name="sequence" /> is still the live record of <paramref name="key" />.</summary>
    /// <param name="key">The operation identifier.</param>
    /// <param name="sequence">The insertion sequence of the record generation.</param>
    /// <param name="createdUtc">The wall creation time of the live record.</param>
    /// <returns><see langword="true" /> when the key holds a record with that sequence.</returns>
    internal delegate bool LiveLookup(string key, long sequence, out DateTime createdUtc);

    /// <summary>Gets the number of entries, live and stale.</summary>
    internal int EntryCount => _createdOrder.Count + _completedCreated.Count + _completedRestored.Count + _wallOrder.Count;

    /// <summary>Registers a record that starts aging, which makes any earlier entry of the same key stale.</summary>
    /// <param name="key">The operation identifier.</param>
    /// <param name="sequence">The insertion sequence of the record.</param>
    /// <param name="createdTimestamp">The monotonic creation timestamp; <see langword="null" /> for a record restored from disk.</param>
    /// <param name="createdUtc">The wall creation time.</param>
    /// <param name="completed">Whether the record is a completed outcome, which capacity eviction may take.</param>
    internal void Add(string key, long sequence, long? createdTimestamp, DateTime createdUtc, bool completed)
    {
        if (createdTimestamp is { } stamp)
        {
            var entry = new Entry(key, sequence, stamp);
            _createdOrder.Enqueue(entry);
            if (completed)
                _completedCreated.Enqueue(entry);

            return;
        }

        var restored = new Entry(key, sequence, 0);
        _wallOrder.Enqueue(restored, createdUtc.Ticks);
        if (completed)
            _completedRestored.Enqueue(restored);
    }

    /// <summary>Takes the next record whose retention passed, in expiry order.</summary>
    /// <param name="retention">The retention.</param>
    /// <param name="utcNow">The server clock wall time.</param>
    /// <param name="timestamp">The server clock monotonic timestamp.</param>
    /// <param name="key">The operation identifier of the expired record; the caller removes it from the store.</param>
    /// <returns><see langword="true" /> when a record expired.</returns>
    internal bool TryTakeExpired(TimeSpan retention, DateTime utcNow, long timestamp, out string key)
    {
        while (_createdOrder.TryPeek(out var head))
        {
            if (!_lookup(head.Key, head.Sequence, out var createdUtc))
            {
                _ = _createdOrder.Dequeue();
                continue;
            }

            if (_timeProvider.GetElapsedTime(head.Stamp, timestamp) <= retention)
                break;

            _ = _createdOrder.Dequeue();
            _wallOrder.Enqueue(head, createdUtc.Ticks);
        }

        while (_wallOrder.TryPeek(out var entry, out _))
        {
            if (!_lookup(entry.Key, entry.Sequence, out var createdUtc))
            {
                _ = _wallOrder.Dequeue();
                continue;
            }

            if (utcNow - createdUtc <= retention)
                break;

            _ = _wallOrder.Dequeue();
            key = entry.Key;
            return true;
        }

        key = string.Empty;
        return false;
    }

    /// <summary>Finds the completed outcome inserted first among those older than <paramref name="minAge" />.</summary>
    /// <param name="minAge">The age the outcome must pass; <see langword="null" /> accepts any age.</param>
    /// <param name="utcNow">The server clock wall time.</param>
    /// <param name="timestamp">The server clock monotonic timestamp.</param>
    /// <param name="key">The operation identifier of the outcome; the caller removes it from the store.</param>
    /// <returns><see langword="true" /> when an outcome qualifies.</returns>
    internal bool TryFindOldestCompleted(TimeSpan? minAge, DateTime utcNow, long timestamp, out string key)
    {
        var found = false;
        key = string.Empty;
        var oldestSequence = long.MaxValue;

        // Created in this process: the monotonic age never grows along the queue, so only the first live entry can qualify.
        while (_completedCreated.TryPeek(out var head))
        {
            if (!_lookup(head.Key, head.Sequence, out _))
            {
                _ = _completedCreated.Dequeue();
                continue;
            }

            if (minAge is not { } age || _timeProvider.GetElapsedTime(head.Stamp, timestamp) > age)
            {
                found = true;
                key = head.Key;
                oldestSequence = head.Sequence;
            }

            break;
        }

        while (_completedRestored.TryPeek(out var restoredHead) && !_lookup(restoredHead.Key, restoredHead.Sequence, out _))
            _ = _completedRestored.Dequeue();

        // Restored from disk: aged by wall time, which is not ordered along the queue, so take the first qualifying live entry.
        foreach (var entry in _completedRestored)
        {
            if (entry.Sequence >= oldestSequence)
                break;

            if (!_lookup(entry.Key, entry.Sequence, out var createdUtc) || (minAge is { } restoredAge && utcNow - createdUtc <= restoredAge))
                continue;

            found = true;
            key = entry.Key;
            break;
        }

        return found;
    }

    /// <summary>Drops the stale entries of every structure that holds more than twice <paramref name="liveRecordCount" /> entries.</summary>
    /// <param name="liveRecordCount">The number of records in the store.</param>
    internal void Compact(int liveRecordCount)
    {
        var limit = (liveRecordCount * 2) + MinCompactionEntries;
        if (_createdOrder.Count > limit)
            CompactQueue(_createdOrder);

        if (_completedCreated.Count > limit)
            CompactQueue(_completedCreated);

        if (_completedRestored.Count > limit)
            CompactQueue(_completedRestored);

        if (_wallOrder.Count > limit)
            CompactHeap(liveRecordCount);
    }

    private void CompactQueue(Queue<Entry> queue)
    {
        for (var remaining = queue.Count; remaining > 0; remaining--)
        {
            var entry = queue.Dequeue();
            if (_lookup(entry.Key, entry.Sequence, out _))
                queue.Enqueue(entry);
        }
    }

    private void CompactHeap(int liveRecordCount)
    {
        var survivors = new List<(Entry Element, long Priority)>(liveRecordCount);
        foreach (var (entry, ticks) in _wallOrder.UnorderedItems)
        {
            if (_lookup(entry.Key, entry.Sequence, out _))
                survivors.Add((entry, ticks));
        }

        _wallOrder.Clear();
        _wallOrder.EnqueueRange(survivors);
    }

    /// <summary>An order entry: the key, the sequence identifying the record generation it was made for, and the monotonic creation timestamp.</summary>
    /// <param name="Key">The operation identifier.</param>
    /// <param name="Sequence">The insertion sequence of the record; an entry whose key now holds another sequence is stale.</param>
    /// <param name="Stamp">The monotonic creation timestamp; unused for restored records.</param>
    private readonly record struct Entry(string Key, long Sequence, long Stamp);
}
