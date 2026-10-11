using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Orders resolved idempotency outcomes by birth so that expiry costs the work it does, not the store size.</summary>
/// <typeparam name="TKey">The identity of a record.</typeparam>
/// <remarks>
///     <para>
///     Not thread-safe: the owning store calls it under its gate. An outcome is born at its anchor minus the age it had there, on the
///     monotonic clock, so outcomes expire in birth order and only the head of the heap is ever examined. Births are not ordered along
///     insertion (a restored or replicated outcome carries an older age), hence a heap instead of a plain queue.
///     </para>
///     <para>
///     An entry carries the sequence of the record generation it was made for. A record that was removed or replaced leaves a stale
///     entry, recognised through <see cref="LiveLookup" /> and skipped when it reaches the head; <see cref="Compact" /> bounds the stale
///     entries by the store size.
///     </para>
///     <para>
///     The heap key is the birth in <see cref="TimeSpan" /> ticks, so the conversion can misplace two records whose births differ by less
///     than one clock tick. The head check uses the real age of the live record, so this only delays an expiry by less than a tick.
///     Records of the same birth leave oldest sequence first. The key cannot overflow: the time since the clock started is not negative
///     and an age is at most the span of <see cref="DateTime" />, well below <see cref="long.MaxValue" /> ticks.
///     </para>
/// </remarks>
[Mutable]
internal sealed class GroupOutcomeOrder<TKey>
    where TKey : notnull
{
    private const int MinCompactionEntries = 64;

    private readonly TimeProvider _clock;
    private readonly PriorityQueue<Entry, (long Birth, long Sequence)> _heap = new();
    private readonly LiveLookup _lookup;
    private readonly TimeSpan _minRetention;
    private readonly TimeSpan _retention;
    private long _lastSequence;

    /// <summary>Initializes a new instance of the <see cref="GroupOutcomeOrder{TKey}" /> class.</summary>
    /// <param name="clock">The clock the anchors were read from.</param>
    /// <param name="retention">The retention; with <see cref="TimeSpan.MaxValue" /> nothing expires.</param>
    /// <param name="minRetention">
    /// The minimum retention, not greater than <paramref name="retention" />; with <see cref="TimeSpan.MaxValue" /> (so the retention is unbounded too)
    /// nothing expires, nothing can be taken early, and nothing is tracked.
    /// </param>
    /// <param name="lookup">Resolves whether an entry still belongs to the live record of its key.</param>
    internal GroupOutcomeOrder(TimeProvider clock, TimeSpan retention, TimeSpan minRetention, LiveLookup lookup)
    {
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(lookup);
        _clock = clock;
        _retention = retention;
        _minRetention = minRetention;
        _lookup = lookup;
    }

    /// <summary>Resolves whether the record generation identified by <paramref name="sequence" /> is still the live resolved record of <paramref name="key" />.</summary>
    /// <param name="key">The identity of the record.</param>
    /// <param name="sequence">The sequence of the record generation.</param>
    /// <param name="age">The age of the live record on the monotonic clock.</param>
    /// <returns><see langword="true" /> when the key holds a resolved record with that sequence.</returns>
    internal delegate bool LiveLookup(TKey key, long sequence, out TimeSpan age);

    /// <summary>Gets the number of entries, live and stale.</summary>
    internal int EntryCount => _heap.Count;

    /// <summary>Takes a sequence for a record that is about to start aging.</summary>
    /// <returns>A sequence no earlier record generation has.</returns>
    internal long NextSequence() => ++_lastSequence;

    /// <summary>Registers a resolved record, which makes any earlier entry of the same key stale.</summary>
    /// <param name="key">The identity of the record.</param>
    /// <param name="sequence">The sequence of the record, from <see cref="NextSequence" />.</param>
    /// <param name="anchorTimestamp">The monotonic timestamp the age is counted from.</param>
    /// <param name="ageAtAnchor">The age the record had at <paramref name="anchorTimestamp" />.</param>
    internal void Add(TKey key, long sequence, long anchorTimestamp, TimeSpan ageAtAnchor)
    {
        if (_minRetention == TimeSpan.MaxValue)
            return;

        _heap.Enqueue(new Entry(key, sequence), (_clock.GetElapsedTime(0L, anchorTimestamp).Ticks - ageAtAnchor.Ticks, sequence));
    }

    /// <summary>Drops every entry.</summary>
    internal void Clear()
    {
        _heap.Clear();
        _heap.TrimExcess();
    }

    /// <summary>Takes the oldest live record whose retention elapsed.</summary>
    /// <param name="key">The identity of the expired record; the caller removes it from the store.</param>
    /// <returns><see langword="true" /> when a record expired.</returns>
    internal bool TryTakeExpired([MaybeNullWhen(false)] out TKey key) => TryTakeOldest(_retention, out key);

    /// <summary>Takes the oldest live record when it is at least <paramref name="minimumAge" /> old.</summary>
    /// <param name="minimumAge">The age the oldest record must have reached.</param>
    /// <param name="key">The identity of the taken record; the caller removes it from the store.</param>
    /// <returns><see langword="true" /> when the oldest live record is old enough; the head is the oldest, so no other record can qualify.</returns>
    internal bool TryTakeOldest(TimeSpan minimumAge, [MaybeNullWhen(false)] out TKey key)
    {
        while (_heap.TryPeek(out var entry, out _))
        {
            if (_lookup(entry.Key, entry.Sequence, out var age))
            {
                if (age < minimumAge)
                    break;

                _ = _heap.Dequeue();
                key = entry.Key;
                return true;
            }

            _ = _heap.Dequeue();
        }

        key = default;
        return false;
    }

    /// <summary>Drops the stale entries when the heap holds more than twice <paramref name="liveRecordCount" /> entries.</summary>
    /// <param name="liveRecordCount">The number of records in the store.</param>
    internal void Compact(int liveRecordCount)
    {
        if (_heap.Count <= (liveRecordCount * 2) + MinCompactionEntries)
            return;

        var survivors = new List<(Entry Element, (long Birth, long Sequence) Priority)>(liveRecordCount);
        foreach (var (entry, birth) in _heap.UnorderedItems)
        {
            if (_lookup(entry.Key, entry.Sequence, out _))
                survivors.Add((entry, birth));
        }

        _heap.Clear();
        _heap.EnqueueRange(survivors);
        _heap.TrimExcess();
    }

    /// <summary>An order entry: the key and the sequence identifying the record generation it was made for.</summary>
    /// <param name="Key">The identity of the record.</param>
    /// <param name="Sequence">The sequence of the record; an entry whose key now holds another sequence is stale.</param>
    [Immutable]
    private readonly record struct Entry(TKey Key, long Sequence);
}
