using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
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
///     step never ages a record. A resolved outcome is always retrievable for the minimum retention, and up to the
///     retention while the table has room. At capacity a new reservation takes the place of the oldest resolved outcome, one eviction per
///     admission, when that outcome is at least as old as the minimum retention; otherwise the reservation is rejected instead of evicting
///     a younger outcome. With the minimum retention equal to the retention, which is the default, no unexpired outcome is ever evicted.
///     </para>
///     <para>
///     The capacity bounds new reservations only. The outcome of a committed entry is never refused for capacity: an entry applied on a
///     follower group, and the outcomes a snapshot or a retained log suffix restores, are kept even when the store then holds more records
///     than its capacity, until retention ages them out. Refusing one would fail the group log over an entry that is already committed.
///     An applied entry does trim the store: while the store holds more than its capacity it evicts the oldest resolved outcomes, other than
///     its own, that are at least as old as the minimum retention, and it stops at the first outcome younger than that. Outcomes of the same
///     age leave oldest resolution first.
///     </para>
///     <para>
///     The store is bounded in bytes as well as in records, since a group snapshot carries every retained outcome and is refused above its
///     size limit. A resolved outcome is charged the bytes it takes in a snapshot (<see cref="GroupSnapshotStore.OutcomeEncodedLength" />),
///     a size taken from the record contents alone, so it is the same on every node and after a restore. A pin is charged the size of the
///     outcome its entry will resolve with, which the leader decided before it reserved, so operations in flight at the same time see each
///     other's outcomes and cannot together pass the bound; a pin of a kind that answers no retry is charged nothing. The size is computed
///     once, when the record is stored, and the same size is given back when it leaves.
///     </para>
///     <para>
///     The byte bound works as the capacity does and evicts in the same order: a new reservation is admitted when its outcome fits after
///     the oldest resolved outcomes past the minimum retention made way, or when the store charges nothing else, so a single outcome larger
///     than the bound is admitted alone. An outcome stored on resolution or on apply is never refused: it evicts the oldest outcomes past the
///     minimum retention, other than its own, until it fits. A rebuild keeps the outcomes with the newest log indexes that fit, and never
///     drops one younger than the minimum retention for bytes. A full store sustains as many outcome bytes per minimum retention as the
///     bound holds.
///     </para>
///     <para>
///     Expiration tombstones (<see cref="GroupRecordKind.Expiration" />) and leader-term no-ops (<see cref="GroupRecordKind.LeaderTerm" />)
///     answer no client retry: their pin holds the identity only while the entry is in flight, never counts against the capacity, and the
///     record is dropped once the entry resolves, so no such outcome is retained, exported, or restored.
///     </para>
///     <para>
///     A snapshot carries the time its outcomes were captured on the same clock that stamped their resolution times, so a
///     restoring node takes the age of each outcome from that one clock and keeps counting it on its own monotonic clock. Node
///     clocks never meet in a subtraction. The time a snapshot spends at rest or in transit does not count toward retention:
///     an outcome may live longer than its window after a restore, never shorter.
///     </para>
///     <para>
///     While the outcomes of the committed log entries are rebuilt after a restart, the store keeps the outcomes with the newest log
///     indexes: a rebuilt outcome takes the place of the retained outcome with the oldest log index when the store is full, and replaces
///     an older outcome of the same identity. A pinned (unresolved) record is never displaced. An outcome a snapshot restored therefore
///     stays replayable only while no newer outcome needs its place.
///     </para>
///     <para>
///     The eviction argument is that an outcome with an older log index is the one nearest to its retention. Two caveats hold: pins
///     admitted past the capacity (already logged) can make the live and the unresolved records together exceed it, so a later restart
///     may trim live outcomes; and the order assumes the resolution time does not decrease with the log index, which leaders with skewed
///     clocks can break across a change of leader.
///     </para>
/// </remarks>
[ThreadSafe]
internal sealed class GroupIdempotencyState
{
    /// <summary>The default bound on the snapshot bytes of the retained outcomes: the default snapshot limit less <see cref="OutcomeHeadroomBytes" />.</summary>
    internal const long DefaultMaxOutcomeBytes = GroupSnapshotStore.DefaultMaxSnapshotBytes - OutcomeHeadroomBytes;

    /// <summary>The part of the default snapshot limit the default byte bound leaves free: one closed replication message, 16 MiB.</summary>
    /// <remarks>
    /// No single outcome is larger than the message that carries its entry. The headroom takes the snapshot header and the outcomes the bound
    /// does not refuse: a follower that ages an outcome later than the leader because of clock skew, and the young outcomes a rebuild keeps.
    /// </remarks>
    internal const long OutcomeHeadroomBytes = 16 * 1024 * 1024;

    /// <summary>The default retention window for resolved outcomes.</summary>
    internal static readonly TimeSpan DefaultRetention = TimeSpan.FromHours(1);

    private readonly GroupOutcomeOrder<GroupOperationKey> _order;
    private readonly OutcomeTable _records;
    private readonly TimeSpan _retention;
    private readonly Lock _sync = new();
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// The number of unresolved records of a kind that answers no retry; they sit in <see cref="_records" /> but not against the capacity.
    /// Guarded by <see cref="_sync" />.
    /// </summary>
    /// <remarks>
    /// Such a record is only ever added by <see cref="Reserve" /> and never resolved in place, and expiry and rebuild eviction touch resolved
    /// records only, so every path that removes or replaces an unresolved record keeps the count.
    /// </remarks>
    private int _exemptPins;

    private int _outcomesRebuilt;

    /// <summary>The state of the rebuild in progress; <see langword="null" /> outside a rebuild. Guarded by <see cref="_sync" />.</summary>
    private OutcomeRebuild? _rebuild;

    /// <summary>The outcomes given up to make room since the store was created; written under <see cref="_sync" />.</summary>
    private long _capacityEvictions;

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
    /// <param name="minRetention">
    /// How long a resolved outcome is always retrievable: a full table gives up its oldest outcome to a new operation only once that outcome is this old.
    /// <see langword="null" /> selects <paramref name="retention" />, which never evicts an unexpired outcome.
    /// </param>
    /// <param name="maxOutcomeBytes">The most snapshot bytes the resolved outcomes take; it bounds new reservations as the capacity does.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The capacity or the byte bound is not positive, or a retention is negative or <paramref name="minRetention" /> exceeds <paramref name="retention" />.
    /// </exception>
    internal GroupIdempotencyState(int capacity, TimeSpan retention, TimeProvider timeProvider, TimeSpan? minRetention = null, long maxOutcomeBytes = DefaultMaxOutcomeBytes)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity), "Idempotency capacity must be positive.");

        if (maxOutcomeBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxOutcomeBytes), "Idempotency outcome byte bound must be positive.");

        retention.ThrowIfNegative(nameof(retention), "Idempotency retention must be non-negative.");
        var floor = minRetention ?? retention;
        floor.ThrowIfNegative(nameof(minRetention), "Idempotency minimum retention must be non-negative.");
        if (floor > retention)
            throw new ArgumentOutOfRangeException(nameof(minRetention), "Idempotency minimum retention must not exceed the retention.");

        Capacity = capacity;
        MaxOutcomeBytes = maxOutcomeBytes;
        _retention = retention;
        MinRetention = floor;
        ArgumentNullException.ThrowIfNull(timeProvider);
        _timeProvider = timeProvider;
        _records = new OutcomeTable();
        _order = new GroupOutcomeOrder<GroupOperationKey>(
            timeProvider,
            retention,
            floor,
            (key, sequence, out age) =>
            {
                // Only a resolved record has a sequence, so an unresolved or replaced record never matches.
                var live = _records.TryGetValue(key, out var stored) && stored.Sequence == sequence;
                age = live ? stored.Age(timeProvider) : TimeSpan.Zero;
                return live;
            });
    }

    /// <summary>Gets the maximum number of retained idempotency records.</summary>
    internal int Capacity { get; }

    /// <summary>Gets how long a resolved outcome is always retrievable.</summary>
    internal TimeSpan MinRetention { get; }

    /// <summary>Gets the most snapshot bytes the retained outcomes take before a new reservation needs an older outcome to make way.</summary>
    internal long MaxOutcomeBytes { get; }

    /// <summary>Gets the bytes charged against <see cref="MaxOutcomeBytes" /> now: the snapshot bytes of the resolved outcomes and the expected outcomes of the pins.</summary>
    internal long OutcomeBytes
    {
        get
        {
            lock (_sync)
                return _records.Bytes;
        }
    }

    /// <summary>
    /// Gets a value indicating whether the outcomes of the committed log entries were rebuilt into this store since the log was opened.
    /// Until then a miss may only mean the outcome is still on the log.
    /// </summary>
    internal bool OutcomesRebuilt => Volatile.Read(ref _outcomesRebuilt) != 0;

    /// <summary>Gets the number of resolved outcomes given up to make room, not expired, since the store was created.</summary>
    /// <remarks>
    /// It counts the oldest outcome a full store gives up to admit an operation, the outcomes an applied entry trims while the store is over
    /// its capacity, and the outcomes given up to keep the outcome bytes within <see cref="MaxOutcomeBytes" />; a rebuild of the committed
    /// log entries keeps the newest outcomes it has room for and counts nothing.
    /// </remarks>
    internal long CapacityEvictions => Interlocked.Read(ref _capacityEvictions);

    /// <summary>Records that the outcomes of the committed log entries are rebuilt, so a miss now means no outcome is retained.</summary>
    /// <remarks>Pins admitted past the capacity while the rebuild ran are made up for here: the outcomes with the oldest log indexes are evicted until the store fits.</remarks>
    internal void MarkOutcomesRebuilt()
    {
        lock (_sync)
        {
            (_rebuild ?? new OutcomeRebuild()).TrimTo(this, Capacity + _exemptPins);
            _rebuild = null;
        }

        Volatile.Write(ref _outcomesRebuilt, 1);
    }

    /// <summary>Starts a rebuild of the outcomes of the committed log entries, dropping the state of a rebuild that failed part-way.</summary>
    internal void BeginOutcomeRebuild()
    {
        lock (_sync)
            _rebuild = new OutcomeRebuild();
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
            foreach (var stored in _records.Values)
            {
                if (stored.Record.LogIndex >= fromIndex)
                    released.Add(GroupOperationKey.Of(stored.Record));
            }

            for (var i = 0; i < released.Count; i++)
            {
                if (_records.TryDrop(released[i], out var stored))
                    AdjustExemptPins(stored.Record, -1);
            }

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
    /// <param name="options">Whether the log already holds the entry, and the length of the outcome it will resolve with.</param>
    /// <returns>
    /// <see cref="GroupIdempotencyReserveResult.Success" /> when the reservation was created or already retained,
    /// <see cref="GroupIdempotencyReserveResult.FingerprintMismatch" /> when a record exists with a different
    /// fingerprint, or <see cref="GroupIdempotencyReserveResult.CapacityExceeded" /> when the key is new, not
    /// already logged, and the store is at capacity or its byte bound.
    /// </returns>
    internal GroupIdempotencyReserveResult Reserve(
        string scope,
        string operationId,
        ReadOnlySpan<byte> operationFingerprint,
        GroupRecordKind kind,
        ulong logIndex,
        ulong term,
        GroupReserveOptions options = default)
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
                if (stored.TryMoveTo(logIndex, term, out var moved))
                    _records.Put(key, in moved);

                return GroupIdempotencyReserveResult.Success;
            }

            // The capacity and the byte bound bind only a new operation that answers retries, which is charged the outcome it will resolve with.
            var answers = AnswersRetries(kind);
            var charged = answers ? GroupSnapshotStore.OutcomeEncodedLength(scope, operationId, operationFingerprint.Length, options.OutcomeLength) : 0L;
            var admitted = options.AlreadyLogged || !answers || TryMakeRoom(1, charged);
            if (!admitted)
                return GroupIdempotencyReserveResult.CapacityExceeded;

            var memory = BufferEx.CopyToOwned(operationFingerprint);
            var record = new GroupIdempotencyRecord(scope, operationId, memory, ReadOnlyMemory<byte>.Empty, kind, _timeProvider.GetUtcNow().UtcDateTime, null, logIndex, term);
            _records.Put(key, new StoredRecord(record, 0L, TimeSpan.Zero, 0L, charged));
            AdjustExemptPins(in record, 1);

            return GroupIdempotencyReserveResult.Success;
        }
    }

    /// <summary>Restores snapshot outcomes and idempotency records carried by a retained journal suffix.</summary>
    /// <remarks>
    ///     <para>
    ///     Every <paramref name="records" /> outcome must already be resolved; the retained journal suffix identified by
    ///     <paramref name="retainedLogIndexes" /> is merged so that records living past the snapshot boundary stay
    ///     authoritative. Every record restored is a committed outcome or a record the store already holds, so the restore is never
    ///     refused for <see cref="Capacity" />, which bounds new reservations only: the store may hold more records afterward. Callers
    ///     that restore after a durable change, such as a published snapshot or a rewritten log, rely on that.
    ///     </para>
    /// </remarks>
    /// <param name="records">The committed outcomes carried by the snapshot.</param>
    /// <param name="capturedUtc">When the snapshot captured its outcomes, on the clock that stamped their resolution times.</param>
    /// <param name="retainedLogIndexes">Journal indexes retained after the snapshot boundary.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="records" /> or <paramref name="retainedLogIndexes" /> is null.</exception>
    /// <exception cref="InvalidDataException">Thrown when a snapshot outcome is not resolved.</exception>
    internal void RestoreFromSnapshot(IReadOnlyList<GroupIdempotencyRecord> records, DateTime capturedUtc, IReadOnlyList<ulong> retainedLogIndexes)
    {
        ArgumentNullException.ThrowIfNull(records);
        ArgumentNullException.ThrowIfNull(retainedLogIndexes);
        lock (_sync)
        {
            ExpireCore();
            GroupIdempotencyRecord.ThrowIfOutcomeUnresolved(records);
            var surviving = AnchorSurviving(records, capturedUtc);
            var retained = CollectRetainedRecords([.. retainedLogIndexes]);
            MergeRestored(surviving, retained);
        }
    }

    /// <summary>Restores the resolved outcome of a committed log entry, the newest log index winning over a snapshot outcome.</summary>
    /// <param name="record">The resolved record rebuilt from the entry.</param>
    /// <param name="age">How long ago the outcome was decided; a negative age counts as zero.</param>
    /// <returns>What became of the outcome.</returns>
    /// <exception cref="ArgumentException">The record is not resolved.</exception>
    /// <exception cref="InvalidOperationException">The outcomes were already rebuilt.</exception>
    /// <remarks>
    /// A pin, or an outcome of the same identity with the same or a newer log index, is kept (<see cref="GroupOutcomeRestoreResult.Retained" />);
    /// an older outcome of the same identity is replaced. At capacity, or while the outcome does not fit <see cref="MaxOutcomeBytes" />, the
    /// retained outcome with the oldest log index gives way when it is older than <paramref name="record" />; pins are never evicted. The
    /// rebuild reads the log newest first, so a <see cref="GroupOutcomeRestoreResult.Full" /> means no older outcome remains to give way.
    /// An outcome that does not fit the byte bound on its own is restored only into a store that retains no other outcome.
    /// </remarks>
    internal GroupOutcomeRestoreResult RestoreOutcome(in GroupIdempotencyRecord record, TimeSpan age)
    {
        if (record.IsUnresolved)
            throw new ArgumentException("A restored outcome must be resolved.", nameof(record));

        // An expiration or a leader-term no-op answers no retry, so its outcome is never restored; the rebuild reads on past it.
        if (!AnswersRetries(record.Kind))
            return GroupOutcomeRestoreResult.Expired;

        // No sweep here: a start restores many outcomes in a row, and the next lookup or reservation sweeps anyway.
        lock (_sync)
        {
            return OutcomesRebuilt
                ? ThrowHelper.Throw<GroupOutcomeRestoreResult>(new InvalidOperationException("The outcomes of the committed log entries are already rebuilt."))
                : (_rebuild ??= new OutcomeRebuild()).Restore(this, in record, age);
        }
    }

    /// <summary>Records the resolved outcome of a committed log entry a group applier applied.</summary>
    /// <param name="record">The resolved record built from the applied entry; its resolution time is the leader time of the decision.</param>
    /// <exception cref="ArgumentException">The record is not resolved.</exception>
    /// <exception cref="InvalidOperationException">The outcomes of the committed log entries are not rebuilt yet.</exception>
    /// <remarks>
    ///     <para>
    ///     A resolved outcome of the same identity with the same or a newer log index is kept. Otherwise the outcome is stored and replaces
    ///     the record of its identity, a pin included: the pin of the entry is resolved by it, and a pin of a later append of the same
    ///     operation gives way too, as the operation is committed and a retry must find its outcome.
    ///     </para>
    ///     <para>
    ///     Its age is counted from the decision time on this node's wall clock, as a rebuild counts it, so the outcome leaves the store
    ///     about when the leader's does. A decision time ahead of this clock counts as age zero, and an outcome already past retention is
    ///     swept on the next access.
    ///     </para>
    ///     <para>
    ///     The entry is committed and its effect is already in memory, so its outcome is never refused. It evicts only outcomes past the
    ///     minimum retention, so the store may hold more than <see cref="Capacity" /> records until retention ages them out.
    ///     </para>
    /// </remarks>
    internal void RecordCommittedOutcome(in GroupIdempotencyRecord record)
    {
        if (record.IsUnresolved)
            throw new ArgumentException("A committed outcome must be resolved.", nameof(record));

        if (!OutcomesRebuilt)
            throw new InvalidOperationException("The outcomes of the committed log entries are not rebuilt yet.");

        lock (_sync)
        {
            ExpireCore();
            var key = GroupOperationKey.Of(in record);
            var known = _records.TryGetValue(key, out var stored);
            if (known && stored.Record.IsResolved && stored.Record.LogIndex >= record.LogIndex)
                return;

            // The record of the identity, if any, is replaced or dropped below; it goes first so the trim never counts or evicts it.
            if (known)
            {
                AdjustExemptPins(stored.Record, -1);
                _ = _records.TryDrop(key, out _);
            }

            // An expiration or a leader-term no-op answers no retry: its pin, if any, was resolved by dropping it, and no outcome is kept.
            if (!AnswersRetries(record.Kind))
                return;

            // Make room for the outcome by trimming the oldest outcomes past the minimum retention; younger ones stay and the store exceeds its
            // capacity or its byte bound.
            var size = OutcomeTable.SizeOf(in record);
            _ = TryMakeRoom(int.MaxValue, size);

            var age = _timeProvider.GetUtcNow().UtcDateTime - record.ResolvedUtc!.Value;
            age = age > TimeSpan.Zero ? age : TimeSpan.Zero;
            var anchor = _timeProvider.GetTimestamp();
            var sequence = _order.NextSequence();
            _order.Add(key, sequence, anchor, age);
            _records.Put(key, new StoredRecord(record, anchor, age, sequence, size));
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
            if (!releasable || !_records.TryDrop(key, out _))
                return false;

            AdjustExemptPins(in record, -1);
            return true;
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

            // An expiration or a leader-term no-op answers no retry: resolving it drops the pin instead of retaining an outcome.
            if (!AnswersRetries(record.Kind))
            {
                AdjustExemptPins(in record, -1);
                return _records.TryDrop(key, out _);
            }

            // The entry is committed, so its outcome is never refused. Its pin already charges the expected outcome; only an outcome larger than
            // that trims the oldest outcomes past the minimum retention to fit. The pin is not in the order, so the trim never takes it.
            var resolved = record.Resolve(BufferEx.CopyToOwned(outcomePayload), _timeProvider.GetUtcNow().UtcDateTime);
            var size = OutcomeTable.SizeOf(in resolved);
            _ = TryMakeRoom(0, size - stored.Bytes);
            var anchor = _timeProvider.GetTimestamp();
            var sequence = _order.NextSequence();
            _order.Add(key, sequence, anchor, TimeSpan.Zero);
            _records.Put(key, new StoredRecord(resolved, anchor, TimeSpan.Zero, sequence, size));
            return true;
        }
    }

    /// <summary>Tells whether a record kind answers client retries: only those keep an outcome and count against the capacity.</summary>
    /// <param name="kind">The record kind.</param>
    /// <returns><see langword="false" /> for an expiration tombstone or a leader-term no-op; otherwise <see langword="true" />.</returns>
    private static bool AnswersRetries(GroupRecordKind kind) => kind is not (GroupRecordKind.Expiration or GroupRecordKind.LeaderTerm);

    /// <summary>Counts a record in or out of the exempt pins when it is one: an unresolved record of a kind that answers no retry, which sits outside the capacity.</summary>
    /// <param name="record">The record added to or removed from the store.</param>
    /// <param name="sign">1 when the record is added; -1 when it is removed.</param>
    /// <remarks>Called under <see cref="_sync" />.</remarks>
    private void AdjustExemptPins(in GroupIdempotencyRecord record, int sign) => _exemptPins += record.IsUnresolved && !AnswersRetries(record.Kind) ? sign : 0;

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

    /// <summary>
    /// Makes room for one record: while the store is at capacity, or the outcome bytes and <paramref name="incomingBytes" /> exceed
    /// <see cref="MaxOutcomeBytes" />, evicts the oldest resolved outcomes that are at least as old as the minimum retention.
    /// </summary>
    /// <param name="maxEvictions">The most outcomes to evict for the capacity; the byte bound evicts as many as it needs.</param>
    /// <param name="incomingBytes">The snapshot bytes of the outcome that needs the room.</param>
    /// <returns>
    /// <see langword="true" /> when the store is under its capacity or gave up an outcome for it, and the outcome fits the byte bound or the
    /// store charges nothing else.
    /// </returns>
    /// <remarks>Called under <see cref="_sync" />. Unresolved records are not in the order, so they are never evicted.</remarks>
    private bool TryMakeRoom(int maxEvictions, long incomingBytes)
    {
        var evicted = 0;
        while (evicted < maxEvictions && _records.Count - _exemptPins >= Capacity && _order.TryTakeOldest(MinRetention, out var oldest))
            evicted += _records.TryDrop(oldest, out _) ? 1 : 0;

        var counted = evicted > 0 || _records.Count - _exemptPins < Capacity;
        while (_records.Bytes + incomingBytes > MaxOutcomeBytes && _order.TryTakeOldest(MinRetention, out var oldest))
            evicted += _records.TryDrop(oldest, out _) ? 1 : 0;

        if (evicted > 0)
            _ = Interlocked.Add(ref _capacityEvictions, evicted);

        return counted && (_records.Bytes + incomingBytes <= MaxOutcomeBytes || _records.Bytes == 0L);
    }

    private void ExpireCore()
    {
        while (_order.TryTakeExpired(out var key))
            _ = _records.TryDrop(key, out _);

        _order.Compact(_records.Count);
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
                surviving.Add(new StoredRecord(record, anchor, age, _order.NextSequence(), OutcomeTable.SizeOf(in record)));
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
            _records.Put(GroupOperationKey.Of(surviving[i].Record), surviving[i]);

        for (var i = 0; i < retained.Count; i++)
            _records.Put(GroupOperationKey.Of(retained[i].Record), retained[i]);

        // The table was rebuilt: order its resolved records afresh, which drops every entry made for the old one.
        _exemptPins = 0;
        _order.Clear();
        foreach (var stored in _records.Values)
        {
            AdjustExemptPins(stored.Record, 1);
            if (stored.Record.IsResolved)
                _order.Add(GroupOperationKey.Of(stored.Record), stored.Sequence, stored.AnchorTimestamp, stored.AgeAtAnchor);
        }
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
    /// <param name="Sequence">The generation of a resolved record in the expiry order; <c language="csharp">0</c> while the record is unresolved.</param>
    /// <param name="Bytes">The bytes the record is charged against the byte bound, fixed when it is stored.</param>
    [Immutable]
    private readonly record struct StoredRecord(GroupIdempotencyRecord Record, long AnchorTimestamp, TimeSpan AgeAtAnchor, long Sequence, long Bytes)
    {
        /// <summary>Returns how long ago the record was resolved, on the monotonic clock.</summary>
        /// <param name="clock">The clock the anchor was read from.</param>
        /// <returns>The age the record was restored with plus the time since it was anchored.</returns>
        internal TimeSpan Age(TimeProvider clock) => AgeAtAnchor + clock.GetElapsedTime(AnchorTimestamp);

        /// <summary>Moves an unresolved record to the coordinates it is re-reserved at; a resolved record keeps its own.</summary>
        /// <param name="logIndex">The journal index that now carries the record.</param>
        /// <param name="term">The term in which the record was appended again.</param>
        /// <param name="moved">The record at the new coordinates when it moved; otherwise this record.</param>
        /// <returns><see langword="true" /> when the record is unresolved and its coordinates differ.</returns>
        internal bool TryMoveTo(ulong logIndex, ulong term, out StoredRecord moved)
        {
            var move = Record.IsUnresolved && (Record.LogIndex != logIndex || Record.Term != term);
            moved = move ? this with { Record = Record with { LogIndex = logIndex, Term = term } } : this;
            return move;
        }

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

    /// <summary>The records of the store and the bytes they are charged.</summary>
    /// <remarks>
    /// Not thread-safe: the store uses it under its gate. Every change goes through <see cref="Put" />, <see cref="TryDrop" /> or
    /// <see cref="Clear" />, and a record gives back the <see cref="StoredRecord.Bytes" /> it was stored with, so <see cref="Bytes" /> and
    /// <see cref="PendingBytes" /> always sum the records held, whatever path added or removed them.
    /// </remarks>
    [Mutable]
    private sealed class OutcomeTable
    {
        private readonly Dictionary<GroupOperationKey, StoredRecord> _records = [];

        /// <summary>Gets the number of records, unresolved ones included.</summary>
        internal int Count => _records.Count;

        /// <summary>Gets the records; the identity of each is <see cref="GroupOperationKey.Of" /> of its record.</summary>
        internal Dictionary<GroupOperationKey, StoredRecord>.ValueCollection Values => _records.Values;

        /// <summary>Gets the bytes charged by every record.</summary>
        internal long Bytes { get; private set; }

        /// <summary>Gets the bytes charged by the unresolved records.</summary>
        internal long PendingBytes { get; private set; }

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
        }

        /// <summary>Stores a record under its identity, replacing the record there.</summary>
        /// <param name="key">The identity of the record.</param>
        /// <param name="stored">The record to store.</param>
        internal void Put(GroupOperationKey key, in StoredRecord stored)
        {
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(_records, key, out var replaced);
            if (replaced)
                Charge(in slot, -1);

            slot = stored;
            Charge(in stored, 1);
        }

        /// <summary>Removes the record of an identity.</summary>
        /// <param name="key">The identity of the record.</param>
        /// <param name="stored">The removed record.</param>
        /// <returns><see langword="true" /> when a record was removed.</returns>
        internal bool TryDrop(GroupOperationKey key, out StoredRecord stored)
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
        internal bool TryGetValue(GroupOperationKey key, out StoredRecord stored) => _records.TryGetValue(key, out stored);

        private void Charge(in StoredRecord stored, int sign)
        {
            Bytes += sign * stored.Bytes;
            PendingBytes += stored.Record.IsUnresolved ? sign * stored.Bytes : 0L;
        }
    }

    /// <summary>The state of one rebuild: the resolved outcomes in ascending log index order, which the rebuild evicts from the oldest.</summary>
    /// <remarks>The order is built when the first eviction needs it and is dropped with the rebuild, so a retried rebuild never reuses a stale one.</remarks>
    private sealed class OutcomeRebuild
    {
        private Queue<(GroupOperationKey Key, ulong LogIndex)>? _queue;

        /// <summary>The bytes of the outcomes this rebuild restored, all with newer log indexes than the one it restores next.</summary>
        private long _restoredBytes;

        /// <summary>Restores one rebuilt outcome into <paramref name="store" />.</summary>
        /// <param name="store">The store; called under its gate.</param>
        /// <param name="record">The resolved record rebuilt from the entry.</param>
        /// <param name="age">How long ago the outcome was decided.</param>
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
        internal GroupOutcomeRestoreResult Restore(GroupIdempotencyState store, in GroupIdempotencyRecord record, TimeSpan age)
        {
            age = age > TimeSpan.Zero ? age : TimeSpan.Zero;
            if (store._retention != TimeSpan.MaxValue && age >= store._retention)
                return GroupOutcomeRestoreResult.Expired;

            var records = store._records;
            var key = GroupOperationKey.Of(in record);
            var known = records.TryGetValue(key, out var existing);
            if (known && (existing.Record.IsUnresolved || existing.Record.LogIndex >= record.LogIndex))
                return GroupOutcomeRestoreResult.Retained;

            // The record of the identity, if any, is older and is replaced, so it charges nothing against the restored outcome.
            var size = OutcomeTable.SizeOf(in record);
            var others = records.Bytes - (known ? existing.Bytes : 0L);
            var young = age < store.MinRetention;

            // Every pin and every outcome this rebuild restored has a newer log index and stays: when they leave no room, evicting the
            // older outcomes would not help.
            var newer = Math.Min(others, records.PendingBytes + _restoredBytes);
            if (!young && newer > 0L && newer + size > store.MaxOutcomeBytes)
                return GroupOutcomeRestoreResult.Full;

            if (!known && records.Count >= store.Capacity + store._exemptPins && !TryEvictOlderThan(store, record.LogIndex, TimeSpan.Zero))
                return GroupOutcomeRestoreResult.Full;

            if (!TryMakeByteRoom(store, in record, size) && !young)
                return GroupOutcomeRestoreResult.Full;

            var anchor = store._timeProvider.GetTimestamp();
            var sequence = store._order.NextSequence();
            store._order.Add(key, sequence, anchor, age);
            records.Put(key, new StoredRecord(record, anchor, age, sequence, size));
            _restoredBytes += size;
            return GroupOutcomeRestoreResult.Restored;
        }

        /// <summary>Evicts the oldest resolved outcomes until the store holds no more than <paramref name="capacity" /> records or none is left.</summary>
        /// <param name="store">The store; called under its gate.</param>
        /// <param name="capacity">The number of records to keep at most.</param>
        internal void TrimTo(GroupIdempotencyState store, int capacity)
        {
            // Outcomes admitted after the first eviction are not in the queue built then: order the whole store afresh.
            _queue = null;
            var evicted = true;
            while (evicted && store._records.Count > capacity)
                evicted = TryEvictOlderThan(store, ulong.MaxValue, TimeSpan.Zero);
        }

        /// <summary>Tells whether an outcome fits the byte bound beside the other records, or is the only record that charges bytes.</summary>
        /// <param name="records">The records of the store.</param>
        /// <param name="key">The identity being restored, whose older record it replaces.</param>
        /// <param name="size">The bytes of the outcome.</param>
        /// <param name="maxBytes">The byte bound.</param>
        /// <returns><see langword="true" /> when the outcome fits.</returns>
        private static bool Fits(OutcomeTable records, GroupOperationKey key, long size, long maxBytes)
        {
            var others = records.Bytes - (records.TryGetValue(key, out var same) ? same.Bytes : 0L);
            return others == 0L || others + size <= maxBytes;
        }

        private static Queue<(GroupOperationKey Key, ulong LogIndex)> Order(OutcomeTable records)
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
        /// <param name="store">The store; called under its gate.</param>
        /// <param name="record">The outcome being restored.</param>
        /// <param name="size">Its bytes.</param>
        /// <returns>
        /// <see langword="true" /> when it fits; otherwise the oldest older outcome is younger than the minimum retention, which happens only
        /// under clock skew, and a young <paramref name="record" /> is kept past the bound.
        /// </returns>
        private bool TryMakeByteRoom(GroupIdempotencyState store, in GroupIdempotencyRecord record, long size)
        {
            var key = GroupOperationKey.Of(in record);
            while (!Fits(store._records, key, size, store.MaxOutcomeBytes))
            {
                if (!TryEvictOlderThan(store, record.LogIndex, store.MinRetention))
                    return false;
            }

            return true;
        }

        /// <summary>Evicts the retained outcome with the oldest log index when it is older than <paramref name="logIndex" /> and at least <paramref name="minAge" /> old.</summary>
        /// <param name="store">The store; called under its gate.</param>
        /// <param name="logIndex">The log index the evicted outcome must be below.</param>
        /// <param name="minAge">The age the evicted outcome must have reached.</param>
        /// <returns><see langword="true" /> when an outcome was evicted.</returns>
        private bool TryEvictOlderThan(GroupIdempotencyState store, ulong logIndex, TimeSpan minAge)
        {
            var records = store._records;
            var queue = _queue ??= Order(records);
            while (queue.TryPeek(out var head))
            {
                var current = records.TryGetValue(head.Key, out var stored) && stored.Record.IsResolved && stored.Record.LogIndex == head.LogIndex;
                if (current && (head.LogIndex >= logIndex || stored.Age(store._timeProvider) < minAge))
                    return false;

                _ = queue.Dequeue();
                if (current)
                    return records.TryDrop(head.Key, out _);
            }

            return false;
        }
    }
}
