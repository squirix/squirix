using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

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
///     Retention is counted from resolution on the monotonic clock of the injected <see cref="TimeProvider" />, so a wall-clock
///     step never ages a record. A resolved outcome stays retrievable until its retention elapses; eviction and capacity
///     accounting consider only resolved records past retention, so at capacity a new reservation is rejected instead of
///     evicting a live outcome.
///     </para>
///     <para>
///     A snapshot carries the time its outcomes were captured on the same clock that stamped their resolution times, so a
///     restoring node takes the age of each outcome from that one clock and keeps counting it on its own monotonic clock. Node
///     clocks never meet in a subtraction. The time a snapshot spends at rest or in transit does not count toward retention:
///     an outcome may live longer than its window after a restore, never shorter.
///     </para>
/// </remarks>
[ThreadSafe]
internal sealed class GroupIdempotencyState
{
    /// <summary>The default retention window for resolved outcomes.</summary>
    internal static readonly TimeSpan DefaultRetention = TimeSpan.FromHours(1);

    private readonly Dictionary<GroupOperationKey, StoredRecord> _records;
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

        retention.ThrowIfNegative(nameof(retention), "Idempotency retention must be non-negative.");

        Capacity = capacity;
        _retention = retention;
        ArgumentNullException.ThrowIfNull(timeProvider);
        _timeProvider = timeProvider;
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
    internal int Capacity { get; }

    /// <summary>Gets a value indicating whether the store holds <see cref="Capacity" /> records, so nothing more can be restored or reserved.</summary>
    internal bool IsFull
    {
        get
        {
            lock (_sync)
                return _records.Count >= Capacity;
        }
    }

    /// <summary>Evicts resolved records whose retention window has elapsed; unresolved records are never evicted.</summary>
    /// <remarks>The eviction relies on the injected time source, so tests advance virtual time deterministically.</remarks>
    internal void Expire()
    {
        lock (_sync)
            ExpireCore();
    }

    /// <summary>Exports only resolved records for inclusion in a group snapshot.</summary>
    /// <param name="capturedUtc">The capture time on the clock the exported resolution times are stamped on.</param>
    /// <returns>
    /// The resolved records currently retained. Each resolution time is restated as <paramref name="capturedUtc" /> minus the age
    /// of the record on the monotonic clock, so the age a snapshot carries is unaffected by a wall-clock step on this node.
    /// </returns>
    internal IReadOnlyList<GroupIdempotencyRecord> ExportResolved(out DateTime capturedUtc)
    {
        lock (_sync)
        {
            ExpireCore();

            // Snapshots encode times in whole milliseconds: capture on one, and each export rounds the age up to one, so the age a
            // snapshot carries is never shorter than the true age.
            var now = _timeProvider.GetUtcNow().UtcDateTime;
            capturedUtc = now.AddTicks(-(now.Ticks % TimeSpan.TicksPerMillisecond));
            var result = new List<GroupIdempotencyRecord>();
            foreach (var stored in _records.Values)
            {
                if (stored.Record.IsResolved)
                    result.Add(stored.Export(_timeProvider, capturedUtc));
            }

            return result;
        }
    }

    /// <summary>Determines whether some record carried by a journal index at or below <paramref name="index" /> is not yet resolved.</summary>
    /// <param name="index">The highest journal index to consider.</param>
    /// <returns><see langword="true" /> when an unresolved record is carried at or below <paramref name="index" />.</returns>
    /// <remarks>
    /// A snapshot exports resolved outcomes only, so compacting through <paramref name="index" /> while this holds would drop an
    /// in-flight outcome together with its journal frame.
    /// </remarks>
    internal bool HasUnresolvedThrough(ulong index)
    {
        lock (_sync)
        {
            foreach (var stored in _records.Values)
            {
                if (stored.Record.IsUnresolved && stored.Record.LogIndex <= index)
                    return true;
            }

            return false;
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
    internal GroupIdempotencyLookup Lookup(string scope, string operationId, ReadOnlySpan<byte> operationFingerprint, out GroupIdempotencyRecord record)
    {
        lock (_sync)
        {
            ExpireCore();
            record = default;
            if (!_records.TryGetValue(new GroupOperationKey(scope, operationId), out var stored))
                return GroupIdempotencyLookup.Miss;

            record = stored.Record;
            if (record.OperationFingerprint.Span.SequenceEqual(operationFingerprint))
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
                if (pair.Value.Record.LogIndex >= fromIndex)
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
    internal GroupIdempotencyReserveResult Reserve(string scope, string operationId, ReadOnlySpan<byte> operationFingerprint, GroupRecordKind kind, ulong logIndex, ulong term)
    {
        lock (_sync)
        {
            ExpireCore();
            var key = new GroupOperationKey(scope, operationId);
            if (_records.TryGetValue(key, out var stored))
            {
                var existing = stored.Record;
                if (!existing.OperationFingerprint.Span.SequenceEqual(operationFingerprint))
                    return GroupIdempotencyReserveResult.FingerprintMismatch;

                // The same fingerprint may be re-reserved at a new journal index or term when the operation is
                // re-appended (e.g., after a log roll or truncation re-writes it). Refresh the stored coordinates so
                // TryResolve can match and resolve the record instead of leaving a stale, unresolvable entry that
                // would pin capacity for the whole retention window. A resolved record already carries its durable
                // outcome, so its original coordinates are kept intact.
                if (existing.IsUnresolved && (existing.LogIndex != logIndex || existing.Term != term))
                    _records[key] = stored with { Record = existing with { LogIndex = logIndex, Term = term } };

                return GroupIdempotencyReserveResult.Success;
            }

            if (_records.Count >= Capacity)
                return GroupIdempotencyReserveResult.CapacityExceeded;

            var memory = BufferEx.CopyToOwned(operationFingerprint);
            var record = new GroupIdempotencyRecord(scope, operationId, memory, ReadOnlyMemory<byte>.Empty, kind, _timeProvider.GetUtcNow().UtcDateTime, null, logIndex, term);
            _records[key] = new StoredRecord(record, 0L, TimeSpan.Zero);
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
    /// <param name="capturedUtc">When the snapshot captured its outcomes, on the clock that stamped their resolution times.</param>
    /// <param name="retainedLogIndexes">Journal indexes retained after the snapshot boundary.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="records" /> or <paramref name="retainedLogIndexes" /> is null.</exception>
    /// <exception cref="InvalidDataException">Thrown when a snapshot outcome is not resolved or the combined set exceeds capacity.</exception>
    internal void RestoreFromSnapshot(IReadOnlyList<GroupIdempotencyRecord> records, DateTime capturedUtc, IReadOnlyList<ulong> retainedLogIndexes)
    {
        ArgumentNullException.ThrowIfNull(records);
        ArgumentNullException.ThrowIfNull(retainedLogIndexes);
        lock (_sync)
        {
            ExpireCore();
            ThrowIfOutcomeUnresolved(records);
            var surviving = AnchorSurviving(records, capturedUtc);
            var retained = CollectRetainedRecords([.. retainedLogIndexes]);
            var distinct = DistinctKeyCount(surviving, retained);
            if (distinct > Capacity)
                throw new InvalidDataException($"Snapshot and retained records ({distinct}) exceed configured idempotency capacity ({Capacity}).");

            MergeRestored(surviving, retained);
        }
    }

    /// <summary>Restores the resolved outcome of a committed log entry that no snapshot carries.</summary>
    /// <param name="record">The resolved record rebuilt from the entry.</param>
    /// <param name="age">How long ago the outcome was decided; a negative age counts as zero.</param>
    /// <returns>
    /// <see langword="true" /> when the outcome was restored; <see langword="false" /> when its identity is already retained, its age is
    /// past retention, or the store is at capacity.
    /// </returns>
    /// <exception cref="ArgumentException">The record is not resolved.</exception>
    /// <remarks>A record already retained, a snapshot outcome or a pinned tail entry, is kept: it is at least as authoritative.</remarks>
    internal bool TryRestoreOutcome(in GroupIdempotencyRecord record, TimeSpan age)
    {
        if (record.IsUnresolved)
            throw new ArgumentException("A restored outcome must be resolved.", nameof(record));

        // No sweep here: a start restores many outcomes in a row, and the next lookup or reservation sweeps anyway.
        lock (_sync)
        {
            age = age > TimeSpan.Zero ? age : TimeSpan.Zero;
            var admitted = (_retention == TimeSpan.MaxValue || age < _retention) && _records.Count < Capacity;
            return admitted && _records.TryAdd(GroupOperationKey.Of(in record), new StoredRecord(record, _timeProvider.GetTimestamp(), age));
        }
    }

    /// <summary>Releases one reservation only when it is still unresolved and has the expected durable coordinates.</summary>
    /// <param name="scope">Operation scope.</param>
    /// <param name="operationId">Operation identifier.</param>
    /// <param name="logIndex">Expected log index.</param>
    /// <param name="term">Expected term.</param>
    /// <returns><see langword="true" /> when the unresolved reservation was removed.</returns>
    internal bool TryReleaseUnresolved(string scope, string operationId, ulong logIndex, ulong term)
    {
        lock (_sync)
        {
            var key = new GroupOperationKey(scope, operationId);
            var known = _records.TryGetValue(key, out var stored);
            var record = stored.Record;
            var releasable = known && !record.IsResolved && record.LogIndex == logIndex && record.Term == term;
            return releasable && _records.Remove(key);
        }
    }

    /// <summary>Resolves an existing record with the exact outcome bytes and a resolution timestamp.</summary>
    /// <param name="scope">The operation scope.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="outcomePayload">The exact resolved outcome bytes.</param>
    /// <param name="logIndex">The journal index that carries the record.</param>
    /// <param name="term">The term in which the record was appended.</param>
    /// <returns><see langword="true" /> when the record existed with matching coordinates and was resolved; otherwise <see langword="false" />.</returns>
    internal bool TryResolve(string scope, string operationId, ReadOnlySpan<byte> outcomePayload, ulong logIndex, ulong term)
    {
        lock (_sync)
        {
            ExpireCore();
            var key = new GroupOperationKey(scope, operationId);
            if (!_records.TryGetValue(key, out var stored))
                return false;

            var record = stored.Record;
            if (record.LogIndex != logIndex || record.Term != term)
                return false;

            // A resolved record already carries its durable outcome; re-resolution must never overwrite it.
            if (record.IsResolved)
                return false;

            var resolved = record.Resolve(BufferEx.CopyToOwned(outcomePayload), _timeProvider.GetUtcNow().UtcDateTime);
            _records[key] = new StoredRecord(resolved, _timeProvider.GetTimestamp(), TimeSpan.Zero);
            return true;
        }
    }

    /// <summary>Restores snapshot outcomes and retained records only when the combined set fits the capacity.</summary>
    /// <remarks>
    ///     <para>
    ///     Behaves exactly like <see cref="RestoreFromSnapshot(IReadOnlyList{GroupIdempotencyRecord}, DateTime, IReadOnlyList{ulong})" />
    ///     except that an over-capacity combined set returns <see langword="false" /> instead of throwing, so callers can
    ///     refuse atomically: no concurrent reservation can slip between the capacity check and the merge.
    ///     </para>
    /// </remarks>
    /// <param name="records">The committed outcomes carried by the snapshot.</param>
    /// <param name="capturedUtc">When the snapshot captured its outcomes, on the clock that stamped their resolution times.</param>
    /// <param name="retainedLogIndexes">Journal indexes retained after the snapshot boundary.</param>
    /// <returns><see langword="true" /> when the restore was applied; <see langword="false" /> when the combined set exceeds capacity.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="records" /> or <paramref name="retainedLogIndexes" /> is null.</exception>
    /// <exception cref="InvalidDataException">Thrown when a snapshot outcome is not resolved.</exception>
    internal bool TryRestoreFromSnapshot(IReadOnlyList<GroupIdempotencyRecord> records, DateTime capturedUtc, IReadOnlyList<ulong> retainedLogIndexes)
    {
        ArgumentNullException.ThrowIfNull(records);
        ArgumentNullException.ThrowIfNull(retainedLogIndexes);
        lock (_sync)
        {
            ExpireCore();
            ThrowIfOutcomeUnresolved(records);
            var surviving = AnchorSurviving(records, capturedUtc);
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
    /// <param name="capturedUtc">When the snapshot captured its outcomes, on the clock that stamped their resolution times.</param>
    /// <param name="retainedLogIndexes">Journal indexes that would remain authoritative after the restore.</param>
    /// <returns><see langword="true" /> when the combined set fits the configured capacity.</returns>
    internal bool WouldRestoreFit(IReadOnlyList<GroupIdempotencyRecord> records, DateTime capturedUtc, IReadOnlyList<ulong> retainedLogIndexes)
    {
        lock (_sync)
        {
            ThrowIfOutcomeUnresolved(records);
            var surviving = AnchorSurviving(records, capturedUtc);
            var expiredKeys = new HashSet<GroupOperationKey>(CollectExpiredKeys());
            var retainedSet = new HashSet<ulong>(retainedLogIndexes);
            var retained = new List<StoredRecord>();
            foreach (var pair in _records)
            {
                if (retainedSet.Contains(pair.Value.Record.LogIndex) && !expiredKeys.Contains(pair.Key))
                    retained.Add(pair.Value);
            }

            return DistinctKeyCount(surviving, retained) <= Capacity;
        }
    }

    /// <summary>Counts the distinct <c language="csharp">(scope, operation id)</c> keys across the snapshot outcomes and retained records.</summary>
    /// <param name="records">The committed outcomes carried by the snapshot.</param>
    /// <param name="retained">The in-memory records still authoritative after installation.</param>
    /// <returns>The number of distinct keys.</returns>
    private static int DistinctKeyCount(List<StoredRecord> records, List<StoredRecord> retained)
    {
        var combined = new HashSet<GroupOperationKey>();
        for (var i = 0; i < records.Count; i++)
            _ = combined.Add(GroupOperationKey.Of(records[i].Record));

        for (var i = 0; i < retained.Count; i++)
            _ = combined.Add(GroupOperationKey.Of(retained[i].Record));

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

    private List<GroupOperationKey> CollectExpiredKeys()
    {
        var expired = new List<GroupOperationKey>();
        if (_retention == TimeSpan.MaxValue)
            return expired;

        foreach (var (key, stored) in _records)
        {
            if (stored.Record.IsResolved && stored.Age(_timeProvider) >= _retention)
                expired.Add(key);
        }

        return expired;
    }

    /// <summary>Collects the in-memory records whose journal index survives the snapshot boundary.</summary>
    /// <param name="retainedSet">The retained journal indexes after the snapshot boundary.</param>
    /// <returns>The in-memory records still authoritative after installation.</returns>
    private List<StoredRecord> CollectRetainedRecords(HashSet<ulong> retainedSet)
    {
        var retained = new List<StoredRecord>();
        foreach (var stored in _records.Values)
        {
            if (retainedSet.Contains(stored.Record.LogIndex))
                retained.Add(stored);
        }

        return retained;
    }

    private void ExpireCore()
    {
        if (_retention == TimeSpan.MaxValue)
            return;

        if (_records.Count == 0)
            return;

        var expired = CollectExpiredKeys();

        for (var i = 0; i < expired.Count; i++)
            _ = _records.Remove(expired[i]);
    }

    /// <summary>Keeps the snapshot outcomes still inside their retention window and anchors their age on this node's monotonic clock.</summary>
    /// <param name="records">The committed outcomes carried by the snapshot.</param>
    /// <param name="capturedUtc">When the snapshot captured its outcomes, on the clock that stamped their resolution times.</param>
    /// <returns>The surviving outcomes, each carrying the age it had at capture.</returns>
    /// <remarks>
    /// The age is the capture time minus the resolution time, both read from the one clock that wrote the snapshot, so this node's
    /// wall clock is never compared with another node's. A resolution time after the capture time counts as age zero.
    /// </remarks>
    private List<StoredRecord> AnchorSurviving(IReadOnlyList<GroupIdempotencyRecord> records, DateTime capturedUtc)
    {
        var anchor = _timeProvider.GetTimestamp();
        var surviving = new List<StoredRecord>(records.Count);
        for (var i = 0; i < records.Count; i++)
        {
            var record = records[i];
            var age = capturedUtc - record.ResolvedUtc!.Value;
            age = age > TimeSpan.Zero ? age : TimeSpan.Zero;
            if (_retention == TimeSpan.MaxValue || age < _retention)
                surviving.Add(new StoredRecord(record, anchor, age));
        }

        return surviving;
    }

    /// <summary>Merges the surviving snapshot outcomes and retained records into the in-memory store.</summary>
    /// <param name="surviving">The snapshot outcomes still inside their retention window.</param>
    /// <param name="retained">The in-memory records still authoritative after installation.</param>
    private void MergeRestored(List<StoredRecord> surviving, List<StoredRecord> retained)
    {
        _records.Clear();
        for (var i = 0; i < surviving.Count; i++)
            _records[GroupOperationKey.Of(surviving[i].Record)] = surviving[i];

        for (var i = 0; i < retained.Count; i++)
            _records[GroupOperationKey.Of(retained[i].Record)] = retained[i];
    }

    /// <summary>Identity of a retained idempotency record.</summary>
    /// <param name="Scope">The operation scope.</param>
    /// <param name="OperationId">The operation identifier.</param>
    [Immutable]
    private readonly record struct GroupOperationKey(string Scope, string OperationId)
    {
        /// <summary>Returns the identity of a record.</summary>
        /// <param name="record">The record.</param>
        /// <returns>Its <c language="csharp">(scope, operation id)</c> key.</returns>
        internal static GroupOperationKey Of(in GroupIdempotencyRecord record) => new(record.OperationScope, record.OperationId);
    }

    /// <summary>A retained record with the monotonic anchor its retention is counted from.</summary>
    /// <param name="Record">The record.</param>
    /// <param name="AnchorTimestamp">The monotonic timestamp the age is counted from; unused while the record is unresolved.</param>
    /// <param name="AgeAtAnchor">The age the record already had at <paramref name="AnchorTimestamp" />, carried over from a snapshot.</param>
    [Immutable]
    private readonly record struct StoredRecord(GroupIdempotencyRecord Record, long AnchorTimestamp, TimeSpan AgeAtAnchor)
    {
        /// <summary>Returns how long ago the record was resolved, on the monotonic clock.</summary>
        /// <param name="clock">The clock the anchor was read from.</param>
        /// <returns>The age the record was restored with plus the time since it was anchored.</returns>
        internal TimeSpan Age(TimeProvider clock) => AgeAtAnchor + clock.GetElapsedTime(AnchorTimestamp);

        /// <summary>Returns the record to export, its resolution time restated as the capture time minus its age.</summary>
        /// <param name="clock">The clock the anchor was read from.</param>
        /// <param name="capturedUtc">The capture time, on the clock the snapshot stamps.</param>
        /// <returns>The record whose resolution time is at its age, rounded up to a whole millisecond, before the capture; it saturates at the earliest date.</returns>
        internal GroupIdempotencyRecord Export(TimeProvider clock, DateTime capturedUtc)
        {
            var age = Age(clock);
            var partial = age.Ticks % TimeSpan.TicksPerMillisecond;
            age = partial == 0 || age > TimeSpan.MaxValue - TimeSpan.FromMilliseconds(1) ? age : age.Add(TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond - partial));
            var resolvedUtc = age < capturedUtc - DateTime.MinValue ? capturedUtc - age : DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Utc);
            return Record with { ResolvedUtc = resolvedUtc };
        }
    }
}
