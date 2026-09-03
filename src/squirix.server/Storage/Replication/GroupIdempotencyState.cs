using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Durable in-memory idempotency outcomes for one replica group.</summary>
/// <remarks>
///     <para>
///     Records are keyed by <c language="csharp">(operation scope, operation id)</c>. A record is first reserved as unresolved while its
///     journal entry is still being appended or committed; it becomes resolved only when the exact outcome and resolution
///     timestamp are supplied. Unresolved records never expire and are never evicted to free capacity: they represent an
///     in-flight operation whose outcome is not yet durable.
///     </para>
///     <para>
///     Retention is counted from the resolution timestamp through the injected <see cref="TimeProvider" />. A resolved
///     outcome stays retrievable until its retention elapses; eviction and capacity accounting consider only resolved
///     records past retention, so at capacity a new reservation is rejected instead of evicting a live outcome.
///     </para>
/// </remarks>
[ThreadSafe]
internal sealed class GroupIdempotencyState
{
    /// <summary>The default retention window for resolved outcomes.</summary>
    internal static readonly TimeSpan DefaultRetention = TimeSpan.FromHours(1);

    private readonly Dictionary<GroupOperationKey, GroupIdempotencyRecord> _records;
    private readonly TimeSpan _retention;
    private readonly Lock _sync = new();
    private readonly TimeProvider _timeProvider;

    /// <summary>Initializes a new instance of the <see cref="GroupIdempotencyState" /> class.</summary>
    /// <param name="capacity">The maximum number of retained records; new reservations are rejected at capacity.</param>
    /// <param name="retention">
    /// How long a resolved outcome remains retrievable after it is resolved. <see cref="TimeSpan.Zero" /> means resolved
    /// records expire on the next <see cref="Expire" /> sweep (immediate expiration), and <see cref="TimeSpan.MaxValue" />
    /// provides unbounded retention. This matches <see cref="FollowerLogOptions.IdempotencyRetention" />, where
    /// <see
    ///     langword="null" />
    /// selects the default window and <see cref="TimeSpan.Zero" /> is the explicit immediate-expiration
    /// sentinel — callers no longer normalize <see cref="TimeSpan.Zero" /> to a default window.
    /// </param>
    /// <param name="timeProvider">The injected time source used to advance retention.</param>
    internal GroupIdempotencyState(int capacity, TimeSpan retention, TimeProvider timeProvider)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity), "Idempotency capacity must be positive.");

        if (retention < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(retention), "Idempotency retention must be non-negative.");

        Capacity = capacity;
        _retention = retention;
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        _records = [];
    }

    /// <summary>Initializes a new instance of the <see cref="GroupIdempotencyState" /> class.</summary>
    /// <param name="capacity">The maximum number of retained records; new reservations are rejected at capacity.</param>
    /// <param name="retention">How long a resolved outcome remains retrievable after it is resolved.</param>
    internal GroupIdempotencyState(int capacity, TimeSpan retention)
        : this(capacity, retention, TimeProvider.System)
    {
    }

    /// <summary>Gets the maximum number of retained idempotency records.</summary>
    private int Capacity { get; }

    /// <summary>Evicts resolved records whose retention window has elapsed; unresolved records are never evicted.</summary>
    /// <remarks>The eviction relies on the injected time source, so tests advance virtual time deterministically.</remarks>
    internal void Expire()
    {
        lock (_sync)
            ExpireCore();
    }

    /// <summary>Exports only resolved records for inclusion in a group snapshot.</summary>
    /// <returns>The resolved records currently retained.</returns>
    internal IReadOnlyList<GroupIdempotencyRecord> ExportResolved()
    {
        lock (_sync)
        {
            ExpireCore();
            var result = new List<GroupIdempotencyRecord>();
            foreach (var pair in _records)
            {
                if (pair.Value.IsResolved)
                    result.Add(pair.Value);
            }

            return result;
        }
    }

    /// <summary>
    /// Looks up a resolvable outcome by operation identity, returning <see cref="GroupIdempotencyLookup.Mismatch" />
    /// when the stored fingerprint differs and <see cref="GroupIdempotencyLookup.Miss" /> when no record is retained.
    /// </summary>
    /// <param name="scope">The operation scope.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="operationFingerprint">The canonical request fingerprint.</param>
    /// <param name="record">The retained record when the lookup succeeds.</param>
    /// <returns>The lookup outcome.</returns>
    internal GroupIdempotencyLookup Lookup(string scope, string operationId, ReadOnlyMemory<byte> operationFingerprint, out GroupIdempotencyRecord record)
    {
        lock (_sync)
        {
            ExpireCore();
            if (!_records.TryGetValue(new GroupOperationKey(scope, operationId), out record))
                return GroupIdempotencyLookup.Miss;

            if (record.OperationFingerprint.Span.SequenceEqual(operationFingerprint.Span))
                return record.IsResolved ? GroupIdempotencyLookup.Found : GroupIdempotencyLookup.Unresolved;
            record = default;
            return GroupIdempotencyLookup.Mismatch;
        }
    }

    /// <summary>Releases every record carried by a journal index at or above <paramref name="fromIndex" />.</summary>
    /// <remarks>
    /// Called after durable truncation: the journal no longer retains the durable source of those reservations, so
    /// retaining them in memory would claim outcomes whose origin was durably discarded.
    /// </remarks>
    /// <param name="fromIndex">The first journal index of the truncated tail.</param>
    /// <returns>The number of records released.</returns>
    internal int ReleaseFromIndex(ulong fromIndex)
    {
        lock (_sync)
        {
            if (_records.Count == 0)
                return 0;

            var released = new List<GroupOperationKey>();
            foreach (var pair in _records)
            {
                if (pair.Value.LogIndex >= fromIndex)
                    released.Add(pair.Key);
            }

            for (var i = 0; i < released.Count; i++)
                _ = _records.Remove(released[i]);
            return released.Count;
        }
    }

    /// <summary>Reserves an unresolved record for an operation, rejecting at capacity when the id is new.</summary>
    /// <param name="scope">The operation scope.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="operationFingerprint">The canonical request fingerprint.</param>
    /// <param name="kind">The record kind.</param>
    /// <param name="logIndex">The journal index that carries the record.</param>
    /// <param name="term">The term in which the record was appended.</param>
    /// <returns>
    /// <see cref="GroupIdempotencyReserveResult.Success" /> when the reservation was created or already retained,
    /// <see cref="GroupIdempotencyReserveResult.FingerprintMismatch" /> when a record exists with a different
    /// fingerprint, or <see cref="GroupIdempotencyReserveResult.CapacityExceeded" /> when the key is new and the
    /// store is at capacity.
    /// </returns>
    internal GroupIdempotencyReserveResult Reserve(string scope, string operationId, ReadOnlyMemory<byte> operationFingerprint, GroupRecordKind kind, ulong logIndex, ulong term)
    {
        lock (_sync)
        {
            ExpireCore();
            var key = new GroupOperationKey(scope, operationId);
            if (_records.TryGetValue(key, out var existing))
            {
                if (!existing.OperationFingerprint.Span.SequenceEqual(operationFingerprint.Span))
                    return GroupIdempotencyReserveResult.FingerprintMismatch;

                // The same fingerprint may be re-reserved at a new journal index or term when the operation is
                // re-appended (e.g., after a log roll or truncation re-writes it). Refresh the stored coordinates so
                // TryResolve can match and resolve the record instead of leaving a stale, unresolvable entry that
                // would pin capacity for the whole retention window. A resolved record already carries its durable
                // outcome, so its original coordinates are kept intact.
                if (existing.IsUnresolved && (existing.LogIndex != logIndex || existing.Term != term))
                    _records[key] = existing with { LogIndex = logIndex, Term = term };

                return GroupIdempotencyReserveResult.Success;
            }

            if (_records.Count >= Capacity)
                return GroupIdempotencyReserveResult.CapacityExceeded;

            _records[key] = new GroupIdempotencyRecord(
                scope,
                operationId,
                operationFingerprint.ToArray(),
                ReadOnlyMemory<byte>.Empty,
                kind,
                _timeProvider.GetUtcNow().UtcDateTime,
                null,
                logIndex,
                term);
            return GroupIdempotencyReserveResult.Success;
        }
    }

    /// <summary>Restores snapshot outcomes and idempotency records carried by a retained journal suffix.</summary>
    /// <remarks>
    ///     <para>
    ///     Every <paramref name="records" /> outcome must already be resolved; the retained journal suffix identified by
    ///     <paramref name="retainedLogIndexes" /> is merged so that records living past the snapshot boundary stay
    ///     authoritative. The combined distinct <c language="csharp">(scope, operation id)</c> count is rejected when it exceeds
    ///     <see cref="Capacity" />, so a valid installation never loses an in-flight outcome.
    ///     </para>
    /// </remarks>
    /// <param name="records">The committed outcomes carried by the snapshot.</param>
    /// <param name="retainedLogIndexes">Journal indexes retained after the snapshot boundary.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="records" /> or <paramref name="retainedLogIndexes" /> is null.</exception>
    /// <exception cref="InvalidDataException">Thrown when a snapshot outcome is not resolved or the combined set exceeds capacity.</exception>
    internal void RestoreFromSnapshot(IReadOnlyList<GroupIdempotencyRecord> records, IReadOnlyList<ulong> retainedLogIndexes)
    {
        ArgumentNullException.ThrowIfNull(records);
        ArgumentNullException.ThrowIfNull(retainedLogIndexes);
        lock (_sync)
        {
            ExpireCore();
            ThrowIfOutcomeUnresolved(records);
            var now = _timeProvider.GetUtcNow().UtcDateTime;
            var surviving = FilterExpiredSnapshot(records, now);
            var retained = CollectRetained(surviving, retainedLogIndexes);
            MergeRestored(surviving, retained);
        }
    }

    /// <summary>Restores only the committed outcomes from a snapshot, retaining no journal suffix.</summary>
    /// <remarks>
    /// This overload replaces the whole store: every retained record, including unresolved reservations, is
    /// discarded because no journal suffix is retained. Call it only on an empty store, such as during startup
    /// recovery. To preserve records carried by a retained suffix, call the overload that takes
    /// <c language="csharp">retainedLogIndexes</c>.
    /// </remarks>
    /// <param name="records">The committed outcomes carried by the snapshot.</param>
    internal void RestoreFromSnapshot(IReadOnlyList<GroupIdempotencyRecord> records) => RestoreFromSnapshot(records, []);

    /// <summary>Restores snapshot outcomes and retained records only when the combined set fits the capacity.</summary>
    /// <remarks>
    ///     <para>
    ///     Behaves exactly like <see cref="RestoreFromSnapshot(IReadOnlyList{GroupIdempotencyRecord}, IReadOnlyList{ulong})" />
    ///     except that an over-capacity combined set returns <see langword="false" /> instead of throwing, so callers can
    ///     refuse atomically: no concurrent reservation can slip between the capacity check and the merge.
    ///     </para>
    /// </remarks>
    /// <param name="records">The committed outcomes carried by the snapshot.</param>
    /// <param name="retainedLogIndexes">Journal indexes retained after the snapshot boundary.</param>
    /// <returns><see langword="true" /> when the restore was applied; <see langword="false" /> when the combined set exceeds capacity.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="records" /> or <paramref name="retainedLogIndexes" /> is null.</exception>
    /// <exception cref="InvalidDataException">Thrown when a snapshot outcome is not resolved.</exception>
    internal bool TryRestoreFromSnapshot(IReadOnlyList<GroupIdempotencyRecord> records, IReadOnlyList<ulong> retainedLogIndexes)
    {
        ArgumentNullException.ThrowIfNull(records);
        ArgumentNullException.ThrowIfNull(retainedLogIndexes);
        lock (_sync)
        {
            ExpireCore();
            ThrowIfOutcomeUnresolved(records);
            var now = _timeProvider.GetUtcNow().UtcDateTime;
            var surviving = FilterExpiredSnapshot(records, now);
            var retained = CollectRetainedRecords([.. retainedLogIndexes]);
            if (DistinctKeyCount(surviving, retained) > Capacity)
                return false;

            MergeRestored(surviving, retained);
            return true;
        }
    }

    /// <summary>Determines whether restoring snapshot outcomes with a retained suffix would fit the capacity, without mutating state.</summary>
    /// <remarks>
    /// Mirrors the accounting of <see cref="TryRestoreFromSnapshot" /> so callers can refuse before any durable
    /// mutation. Expired records are filtered out with the same retention rule <see cref="Expire" /> applies, but
    /// the stored records are left untouched. Must be called under the same external serialization discipline as
    /// the restore itself.
    /// </remarks>
    /// <param name="records">The committed outcomes carried by the snapshot.</param>
    /// <param name="retainedLogIndexes">Journal indexes that would remain authoritative after the restore.</param>
    /// <returns><see langword="true" /> when the combined set fits the configured capacity.</returns>
    internal bool WouldRestoreFit(IReadOnlyList<GroupIdempotencyRecord> records, IReadOnlyList<ulong> retainedLogIndexes)
    {
        lock (_sync)
        {
            ThrowIfOutcomeUnresolved(records);
            var now = _timeProvider.GetUtcNow().UtcDateTime;
            var surviving = FilterExpiredSnapshot(records, now);
            var expiredKeys = new HashSet<GroupOperationKey>(CollectExpiredKeys(now));
            var retainedSet = new HashSet<ulong>(retainedLogIndexes);
            var retained = new List<GroupIdempotencyRecord>();
            foreach (var pair in _records)
            {
                if (retainedSet.Contains(pair.Value.LogIndex) && !expiredKeys.Contains(pair.Key))
                    retained.Add(pair.Value);
            }

            return DistinctKeyCount(surviving, retained) <= Capacity;
        }
    }

    /// <summary>Resolves an existing record with the exact outcome bytes and a resolution timestamp.</summary>
    /// <param name="scope">The operation scope.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="outcomePayload">The exact resolved outcome bytes.</param>
    /// <param name="logIndex">The journal index that carries the record.</param>
    /// <param name="term">The term in which the record was appended.</param>
    /// <returns><see langword="true" /> when the record existed with matching coordinates and was resolved; otherwise <see langword="false" />.</returns>
    internal bool TryResolve(string scope, string operationId, ReadOnlyMemory<byte> outcomePayload, ulong logIndex, ulong term)
    {
        lock (_sync)
        {
            ExpireCore();
            var key = new GroupOperationKey(scope, operationId);
            if (!_records.TryGetValue(key, out var record))
                return false;

            if (record.LogIndex != logIndex || record.Term != term)
                return false;

            // A resolved record already carries its durable outcome; re-resolution must never overwrite it.
            if (record.IsResolved)
                return false;

            _records[key] = record.Resolve(outcomePayload.ToArray(), _timeProvider.GetUtcNow().UtcDateTime);
            return true;
        }
    }

    /// <summary>Counts the distinct <c language="csharp">(scope, operation id)</c> keys across the snapshot outcomes and retained records.</summary>
    /// <param name="records">The committed outcomes carried by the snapshot.</param>
    /// <param name="retained">The in-memory records still authoritative after installation.</param>
    /// <returns>The number of distinct keys.</returns>
    private static int DistinctKeyCount(List<GroupIdempotencyRecord> records, List<GroupIdempotencyRecord> retained)
    {
        var combined = new HashSet<GroupOperationKey>();
        for (var i = 0; i < records.Count; i++)
            _ = combined.Add(new GroupOperationKey(records[i].OperationScope, records[i].OperationId));

        for (var i = 0; i < retained.Count; i++)
            _ = combined.Add(new GroupOperationKey(retained[i].OperationScope, retained[i].OperationId));

        return combined.Count;
    }

    /// <summary>Throws when any snapshot outcome has not been resolved yet.</summary>
    /// <param name="records">The committed outcomes carried by the snapshot.</param>
    /// <exception cref="InvalidDataException">Thrown when a snapshot outcome is not resolved.</exception>
    private static void ThrowIfOutcomeUnresolved(IReadOnlyList<GroupIdempotencyRecord> records)
    {
        for (var i = 0; i < records.Count; i++)
        {
            if (records[i].ResolvedUtc == null)
                throw new InvalidDataException("Snapshot outcome must be resolved.");
        }
    }

    private List<GroupOperationKey> CollectExpiredKeys(DateTime now)
    {
        var expired = new List<GroupOperationKey>();
        foreach (var (key, record) in _records)
        {
            if (!record.IsUnresolved && now - record.ResolvedUtc!.Value >= _retention)
                expired.Add(key);
        }

        return expired;
    }

    /// <summary>Collects the retained records surviving the snapshot boundary and validates the combined capacity.</summary>
    /// <param name="surviving">The snapshot outcomes still inside their retention window.</param>
    /// <param name="retainedLogIndexes">Journal indexes retained after the snapshot boundary.</param>
    /// <returns>The in-memory records still authoritative after installation.</returns>
    /// <exception cref="InvalidDataException">Thrown when the combined distinct key count exceeds capacity.</exception>
    private List<GroupIdempotencyRecord> CollectRetained(List<GroupIdempotencyRecord> surviving, IReadOnlyList<ulong> retainedLogIndexes)
    {
        var retainedSet = new HashSet<ulong>(retainedLogIndexes);
        var retained = CollectRetainedRecords(retainedSet);
        var distinct = DistinctKeyCount(surviving, retained);
        if (distinct > Capacity)
            throw new InvalidDataException($"Snapshot and retained records ({distinct}) exceed configured idempotency capacity ({Capacity}).");
        return retained;
    }

    /// <summary>Collects the in-memory records whose journal index survives the snapshot boundary.</summary>
    /// <param name="retainedSet">The retained journal indexes after the snapshot boundary.</param>
    /// <returns>The in-memory records still authoritative after installation.</returns>
    private List<GroupIdempotencyRecord> CollectRetainedRecords(HashSet<ulong> retainedSet)
    {
        var retained = new List<GroupIdempotencyRecord>();
        foreach (var pair in _records)
        {
            if (retainedSet.Contains(pair.Value.LogIndex))
                retained.Add(pair.Value);
        }

        return retained;
    }

    private void ExpireCore()
    {
        if (_retention == TimeSpan.MaxValue)
            return;

        if (_records.Count == 0)
            return;

        var expired = CollectExpiredKeys(_timeProvider.GetUtcNow().UtcDateTime);

        for (var i = 0; i < expired.Count; i++)
            _ = _records.Remove(expired[i]);
    }

    /// <summary>Keeps only the snapshot outcomes still inside their retention window, using the injected time source.</summary>
    /// <param name="records">The committed outcomes carried by the snapshot (concrete list for devirtualized iteration).</param>
    /// <param name="now">The current UTC time from the injected <see cref="TimeProvider" />.</param>
    /// <returns>The surviving outcomes; unexpired resolved outcomes are retained, expired ones dropped.</returns>
    private List<GroupIdempotencyRecord> FilterExpiredSnapshot(IReadOnlyList<GroupIdempotencyRecord> records, DateTime now)
    {
        var surviving = new List<GroupIdempotencyRecord>(records.Count);
        for (var i = 0; i < records.Count; i++)
        {
            var record = records[i];
            if (_retention == TimeSpan.MaxValue || now - record.ResolvedUtc!.Value < _retention)
                surviving.Add(record);
        }

        return surviving;
    }

    /// <summary>Merges the surviving snapshot outcomes and retained records into the in-memory store.</summary>
    /// <param name="surviving">The snapshot outcomes still inside their retention window.</param>
    /// <param name="retained">The in-memory records still authoritative after installation.</param>
    private void MergeRestored(List<GroupIdempotencyRecord> surviving, List<GroupIdempotencyRecord> retained)
    {
        _records.Clear();
        for (var i = 0; i < surviving.Count; i++)
        {
            var record = surviving[i];
            _records[new GroupOperationKey(record.OperationScope, record.OperationId)] = record;
        }

        for (var i = 0; i < retained.Count; i++)
        {
            var record = retained[i];
            var key = new GroupOperationKey(record.OperationScope, record.OperationId);
            _records[key] = record;
        }
    }

    /// <summary>Identity of a retained idempotency record.</summary>
    /// <param name="Scope">The operation scope.</param>
    /// <param name="OperationId">The operation identifier.</param>
    [Immutable]
    private readonly record struct GroupOperationKey(string Scope, string OperationId);
}
