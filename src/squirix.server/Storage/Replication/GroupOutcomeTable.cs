using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>The records of a group idempotency store, the bytes they are charged, and the pins that sit outside the capacity.</summary>
/// <remarks>
/// Not thread-safe: <see cref="GroupIdempotencyState" /> uses it under its gate. Every change goes through <see cref="Put" />,
/// <see cref="TryDrop" /> or <see cref="Clear" />, and a record gives back the <see cref="GroupStoredOutcome.Bytes" /> it was stored with,
/// so <see cref="Bytes" />, <see cref="PendingBytes" /> and <see cref="ExemptPins" /> always sum the records held, whatever path added or
/// removed them.
/// </remarks>
[Mutable]
internal sealed class GroupOutcomeTable
{
    private readonly Dictionary<GroupOperationKey, GroupStoredOutcome> _records = [];

    /// <summary>Gets the number of records, unresolved ones included.</summary>
    internal int Count => _records.Count;

    /// <summary>Gets the records; the identity of each is <see cref="GroupOperationKey.Of" /> of its record.</summary>
    internal Dictionary<GroupOperationKey, GroupStoredOutcome>.ValueCollection Values => _records.Values;

    /// <summary>Gets the bytes charged by every record.</summary>
    internal long Bytes { get; private set; }

    /// <summary>Gets the bytes charged by the unresolved records.</summary>
    internal long PendingBytes { get; private set; }

    /// <summary>Gets the number of unresolved records of a kind that answers no retry; they are held but not against the capacity.</summary>
    internal int ExemptPins { get; private set; }

    /// <summary>Tells whether a record kind answers client retries: only those keep an outcome and count against the capacity and the byte bound.</summary>
    /// <param name="kind">The record kind.</param>
    /// <returns><see langword="false" /> for an expiration tombstone or a leader-term no-op; otherwise <see langword="true" />.</returns>
    internal static bool AnswersRetries(GroupRecordKind kind) => kind is not (GroupRecordKind.Expiration or GroupRecordKind.LeaderTerm);

    /// <summary>Returns the snapshot bytes of a resolved record.</summary>
    /// <param name="record">The record.</param>
    /// <returns>Its encoded size in a snapshot.</returns>
    internal static long SizeOf(in GroupIdempotencyRecord record) =>
        GroupSnapshotStore.OutcomeEncodedLength(record.OperationScope, record.OperationId, record.OperationFingerprint.Length, record.OutcomePayload.Length);

    /// <summary>Removes every record.</summary>
    internal void Clear()
    {
        _records.Clear();
        Bytes = 0L;
        PendingBytes = 0L;
        ExemptPins = 0;
    }

    /// <summary>Exports the resolved records for a snapshot.</summary>
    /// <param name="clock">The clock the anchors were read from.</param>
    /// <param name="capturedUtc">The capture time, on the clock the snapshot stamps.</param>
    /// <returns>The resolved records, each resolution time restated as the capture time minus its age.</returns>
    internal List<GroupIdempotencyRecord> ExportResolved(TimeProvider clock, DateTime capturedUtc)
    {
        var result = new List<GroupIdempotencyRecord>();
        foreach (var stored in _records.Values)
        {
            if (stored.Record.IsResolved)
                result.Add(stored.Export(clock, capturedUtc));
        }

        return result;
    }

    /// <summary>Determines whether some record carried by a journal index at or below <paramref name="index" /> is not yet resolved.</summary>
    /// <param name="index">The highest journal index to consider.</param>
    /// <returns><see langword="true" /> when an unresolved record is carried at or below <paramref name="index" />.</returns>
    internal bool HasUnresolvedThrough(ulong index)
    {
        foreach (var stored in _records.Values)
        {
            if (stored.Record.IsUnresolved && stored.Record.LogIndex <= index)
                return true;
        }

        return false;
    }

    /// <summary>Stores a record under its identity, replacing the record there.</summary>
    /// <param name="key">The identity of the record.</param>
    /// <param name="stored">The record to store.</param>
    internal void Put(GroupOperationKey key, in GroupStoredOutcome stored)
    {
        ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(_records, key, out var replaced);
        if (replaced)
            Charge(in slot, -1);

        slot = stored;
        Charge(in stored, 1);
    }

    /// <summary>Removes every record carried by a journal index at or above <paramref name="fromIndex" />.</summary>
    /// <param name="fromIndex">The first journal index of the truncated tail.</param>
    /// <returns>The number of records removed.</returns>
    internal int ReleaseFrom(ulong fromIndex)
    {
        if (_records.Count == 0)
            return 0;

        var released = new List<GroupOperationKey>();
        foreach (var stored in _records.Values)
        {
            if (stored.Record.LogIndex >= fromIndex)
                released.Add(GroupOperationKey.Of(stored.Record));
        }

        for (var i = 0; i < released.Count; i++)
            _ = TryDrop(released[i], out _);

        return released.Count;
    }

    /// <summary>Replaces the records with the snapshot outcomes still inside their retention and the records a retained journal suffix carries.</summary>
    /// <param name="records">The committed outcomes carried by the snapshot, all resolved.</param>
    /// <param name="capturedUtc">When the snapshot captured its outcomes, on the clock that stamped their resolution times.</param>
    /// <param name="retainedLogIndexes">Journal indexes retained after the snapshot boundary; the records they carry stay authoritative.</param>
    /// <param name="order">The expiry order, rebuilt for the new records.</param>
    /// <param name="limits">The retention and the clock of the store.</param>
    /// <remarks>
    /// The age of a snapshot outcome is the capture time minus its resolution time, both read from the one clock that wrote the snapshot, so
    /// this node's wall clock is never compared with another node's. A resolution time after the capture time counts as age zero.
    /// </remarks>
    internal void ReplaceWithSnapshot(
        IReadOnlyList<GroupIdempotencyRecord> records,
        DateTime capturedUtc,
        HashSet<ulong> retainedLogIndexes,
        GroupOutcomeOrder<GroupOperationKey> order,
        in GroupOutcomeLimits limits)
    {
        var retained = new List<GroupStoredOutcome>();
        foreach (var stored in _records.Values)
        {
            if (retainedLogIndexes.Contains(stored.Record.LogIndex))
                retained.Add(stored);
        }

        Clear();
        order.Clear();
        var anchor = limits.Clock.GetTimestamp();
        for (var i = 0; i < records.Count; i++)
        {
            var record = records[i];
            var age = capturedUtc - record.ResolvedUtc!.Value;
            age = age > TimeSpan.Zero ? age : TimeSpan.Zero;
            if (limits.Retention == TimeSpan.MaxValue || age < limits.Retention)
                Put(GroupOperationKey.Of(in record), new GroupStoredOutcome(record, anchor, age, order.NextSequence(), SizeOf(in record)));
        }

        for (var i = 0; i < retained.Count; i++)
            Put(GroupOperationKey.Of(retained[i].Record), retained[i]);

        // The table was rebuilt: order its resolved records afresh, which drops every entry made for the old one.
        foreach (var stored in _records.Values)
        {
            if (stored.Record.IsResolved)
                order.Add(GroupOperationKey.Of(stored.Record), stored.Sequence, stored.AnchorTimestamp, stored.AgeAtAnchor);
        }
    }

    /// <summary>Removes the record of an identity.</summary>
    /// <param name="key">The identity of the record.</param>
    /// <param name="stored">The removed record.</param>
    /// <returns><see langword="true" /> when a record was removed.</returns>
    internal bool TryDrop(GroupOperationKey key, out GroupStoredOutcome stored)
    {
        if (!_records.Remove(key, out stored))
            return false;

        Charge(in stored, -1);
        return true;
    }

    /// <summary>Gets the record of an identity.</summary>
    /// <param name="key">The identity of the record.</param>
    /// <param name="stored">The record when one is held.</param>
    /// <returns><see langword="true" /> when a record is held.</returns>
    internal bool TryGetValue(GroupOperationKey key, out GroupStoredOutcome stored) => _records.TryGetValue(key, out stored);

    private void Charge(in GroupStoredOutcome stored, int sign)
    {
        var unresolved = stored.Record.IsUnresolved;
        Bytes += sign * stored.Bytes;
        PendingBytes += unresolved ? sign * stored.Bytes : 0L;
        ExemptPins += unresolved && !AnswersRetries(stored.Record.Kind) ? sign : 0;
    }
}
