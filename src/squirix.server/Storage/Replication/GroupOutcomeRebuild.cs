using System;
using System.Collections.Generic;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>
/// The state of one rebuild of the outcomes of the committed group log entries: the resolved outcomes in ascending log index order, which
/// the rebuild evicts from the oldest.
/// </summary>
/// <remarks>
/// Not thread-safe: <see cref="GroupIdempotencyState" /> uses it under its gate. The order is built when the first eviction needs it and is
/// dropped with the rebuild, so a retried rebuild never reuses a stale one.
/// </remarks>
[Mutable]
internal sealed class GroupOutcomeRebuild
{
    private Queue<(GroupOperationKey Key, ulong LogIndex)>? _queue;

    /// <summary>The bytes of the outcomes this rebuild restored, all with newer log indexes than the one it restores next.</summary>
    private long _restoredBytes;

    /// <summary>Restores one rebuilt outcome into <paramref name="records" />.</summary>
    /// <param name="records">The records of the store.</param>
    /// <param name="order">The expiry order of the store.</param>
    /// <param name="record">The resolved record rebuilt from the entry.</param>
    /// <param name="age">How long ago the outcome was decided.</param>
    /// <param name="limits">The bounds and the clock of the store.</param>
    /// <returns>What became of the outcome.</returns>
    /// <remarks>
    ///     <para>
    ///     Outcomes with older log indexes give way to it for the capacity, and for the byte bound when they are past the minimum retention.
    ///     </para>
    ///     <para>
    ///     An outcome younger than the minimum retention is never refused for bytes, as the live store never refuses a committed outcome.
    ///     An older one is refused (<see cref="GroupOutcomeRestoreResult.Full" />) before anything is evicted when the older outcomes could
    ///     not make room for it, unless the store charges nothing else.
    ///     </para>
    /// </remarks>
    internal GroupOutcomeRestoreResult Restore(
        GroupOutcomeTable records,
        GroupOutcomeOrder<GroupOperationKey> order,
        in GroupIdempotencyRecord record,
        TimeSpan age,
        in GroupOutcomeLimits limits)
    {
        age = age > TimeSpan.Zero ? age : TimeSpan.Zero;
        if (limits.Retention != TimeSpan.MaxValue && age >= limits.Retention)
            return GroupOutcomeRestoreResult.Expired;

        var key = GroupOperationKey.Of(in record);
        var known = records.TryGetValue(key, out var existing);
        if (known && (existing.Record.IsUnresolved || existing.Record.LogIndex >= record.LogIndex))
            return GroupOutcomeRestoreResult.Retained;

        // The record of the identity, if any, is older and is replaced, so it charges nothing against the restored outcome.
        var size = GroupOutcomeTable.SizeOf(in record);
        var others = records.Bytes - (known ? existing.Bytes : 0L);
        var young = age < limits.MinRetention;

        // Every pin and every outcome this rebuild restored has a newer log index and stays: when they leave no room, evicting the
        // older outcomes would not help.
        var newer = Math.Min(others, records.PendingBytes + _restoredBytes);
        if (!young && newer > 0L && newer + size > limits.MaxOutcomeBytes)
            return GroupOutcomeRestoreResult.Full;

        if (!known && records.Count >= limits.Capacity + records.ExemptPins && !TryEvictOlderThan(records, record.LogIndex, TimeSpan.Zero, limits.Clock))
            return GroupOutcomeRestoreResult.Full;

        if (!TryMakeByteRoom(records, in record, size, in limits) && !young)
            return GroupOutcomeRestoreResult.Full;

        var anchor = limits.Clock.GetTimestamp();
        var sequence = order.NextSequence();
        order.Add(key, sequence, anchor, age);
        records.Put(key, new GroupStoredOutcome(record, anchor, age, sequence, size));
        _restoredBytes += size;
        return GroupOutcomeRestoreResult.Restored;
    }

    /// <summary>Evicts the oldest resolved outcomes until the store holds no more than <paramref name="capacity" /> records or none is left.</summary>
    /// <param name="records">The records of the store.</param>
    /// <param name="capacity">The number of records to keep at most.</param>
    /// <param name="clock">The clock the ages are counted on.</param>
    internal void TrimTo(GroupOutcomeTable records, int capacity, TimeProvider clock)
    {
        // Outcomes admitted after the first eviction are not in the queue built then: order the whole store afresh.
        _queue = null;
        var evicted = true;
        while (evicted && records.Count > capacity)
            evicted = TryEvictOlderThan(records, ulong.MaxValue, TimeSpan.Zero, clock);
    }

    /// <summary>Tells whether an outcome fits the byte bound beside the other records, or is the only record that charges bytes.</summary>
    /// <param name="records">The records of the store.</param>
    /// <param name="key">The identity being restored, whose older record it replaces.</param>
    /// <param name="size">The bytes of the outcome.</param>
    /// <param name="maxBytes">The byte bound.</param>
    /// <returns><see langword="true" /> when the outcome fits.</returns>
    private static bool Fits(GroupOutcomeTable records, GroupOperationKey key, long size, long maxBytes)
    {
        var others = records.Bytes - (records.TryGetValue(key, out var same) ? same.Bytes : 0L);
        return others == 0L || others + size <= maxBytes;
    }

    private static Queue<(GroupOperationKey Key, ulong LogIndex)> Order(GroupOutcomeTable records)
    {
        var resolved = new List<(GroupOperationKey Key, ulong LogIndex)>();
        foreach (var stored in records.Values)
        {
            if (stored.Record.IsResolved)
                resolved.Add((GroupOperationKey.Of(stored.Record), stored.Record.LogIndex));
        }

        resolved.Sort(static (left, right) => left.LogIndex.CompareTo(right.LogIndex));
        return new Queue<(GroupOperationKey Key, ulong LogIndex)>(resolved);
    }

    /// <summary>
    /// Evicts the outcomes with the oldest log indexes, older than <paramref name="record" /> and past the minimum retention, until it fits
    /// the byte bound.
    /// </summary>
    /// <param name="records">The records of the store.</param>
    /// <param name="record">The outcome being restored.</param>
    /// <param name="size">Its bytes.</param>
    /// <param name="limits">The bounds and the clock of the store.</param>
    /// <returns>
    /// <see langword="true" /> when it fits; otherwise the oldest older outcome is younger than the minimum retention, which happens only
    /// under clock skew, and a young <paramref name="record" /> is kept past the bound.
    /// </returns>
    private bool TryMakeByteRoom(GroupOutcomeTable records, in GroupIdempotencyRecord record, long size, in GroupOutcomeLimits limits)
    {
        var key = GroupOperationKey.Of(in record);
        while (!Fits(records, key, size, limits.MaxOutcomeBytes))
        {
            if (!TryEvictOlderThan(records, record.LogIndex, limits.MinRetention, limits.Clock))
                return false;
        }

        return true;
    }

    /// <summary>Evicts the retained outcome with the oldest log index when it is older than <paramref name="logIndex" /> and at least <paramref name="minAge" /> old.</summary>
    /// <param name="records">The records of the store.</param>
    /// <param name="logIndex">The log index the evicted outcome must be below.</param>
    /// <param name="minAge">The age the evicted outcome must have reached.</param>
    /// <param name="clock">The clock the ages are counted on.</param>
    /// <returns><see langword="true" /> when an outcome was evicted.</returns>
    private bool TryEvictOlderThan(GroupOutcomeTable records, ulong logIndex, TimeSpan minAge, TimeProvider clock)
    {
        var queue = _queue ??= Order(records);
        while (queue.TryPeek(out var head))
        {
            var current = records.TryGetValue(head.Key, out var stored) && stored.Record.IsResolved && stored.Record.LogIndex == head.LogIndex;
            if (current && (head.LogIndex >= logIndex || stored.Age(clock) < minAge))
                return false;

            _ = queue.Dequeue();
            if (current)
                return records.TryDrop(head.Key, out _);
        }

        return false;
    }
}
