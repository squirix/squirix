using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Unified in-memory and durable idempotency store for mutating cache RPC outcomes.</summary>
/// <remarks>
/// Records are dated on the server clock and persisted with that wall time. A record created in this process also keeps the monotonic
/// timestamp of its creation and expires only once both its wall age and its monotonic age pass the retention, so a forward wall-clock
/// step cannot purge it early; a backward step extends its retention by the size of the step, which only strengthens deduplication. A
/// restored record has no monotonic origin and ages by wall time.
/// <para>
/// At capacity a new operation evicts the completed outcome inserted first among those older than the minimum retention; snapshots
/// keep that insertion order. That age is monotonic for a record created in this process, so a wall-clock step neither blocks nor
/// hastens eviction. A reservation is never evicted, and when no outcome is old enough the new operation is refused as retryable, so
/// within the retention a retry never runs alongside its first attempt. Recording an outcome and restoring records never refuse: they
/// evict the completed outcome inserted first, or exceed the capacity when every record is a reservation; a store over capacity evicts
/// one outcome per new operation and drains as records expire.
/// </para>
/// <para>
/// Expiry and eviction follow <see cref="IdempotencyExpiryOrder" /> instead of scanning the records: a call pops the expired, evicted
/// and stale entries at the heads of its queues and heaps, each entry once, and compaction bounds the stale entries by the record count.
/// </para>
/// </remarks>
[Mutable]
internal sealed class RpcMutationIdempotencyStore : IIdempotencySnapshotExporter
{
    private readonly Lock _capacityGate = new();

    /// <summary>
    /// Executions in flight for Started reservations acquired in this process, keyed like the records. A retry joins the execution
    /// instead of reporting an unknown outcome; intents restored from the journal or a snapshot never have an entry here.
    /// </summary>
    private readonly Dictionary<string, TaskCompletionSource> _executions = [with(StringComparer.Ordinal)];
    private readonly IdempotencyMetrics _metrics;
    private readonly string _nodeId;
    private readonly IdempotencyExpiryOrder _order;
    private readonly IdempotencyOptions _options;
    private readonly Dictionary<string, StoredRecord> _records = [with(StringComparer.Ordinal)];
    private readonly TimeProvider _timeProvider;
    private long _nextSequence;

    internal RpcMutationIdempotencyStore(IdempotencyOptions options, string nodeId, IdempotencyMetrics metrics, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentNullException.ThrowIfNull(metrics);
        options.Validate();
        _options = options;
        _nodeId = nodeId;
        _metrics = metrics;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _order = new IdempotencyExpiryOrder(_timeProvider, TryGetLive);
    }

    internal int ExecutionCount
    {
        get
        {
            lock (_capacityGate)
                return _executions.Count;
        }
    }

    /// <summary>Gets the number of entries, live and stale, held by the expiry order.</summary>
    internal int OrderEntryCount
    {
        get
        {
            lock (_capacityGate)
                return _order.EntryCount;
        }
    }

    internal int RecordCount
    {
        get
        {
            lock (_capacityGate)
                return _records.Count;
        }
    }

    void IIdempotencySnapshotExporter.ExportSnapshot(List<PersistedIdempotencyRecord> destination, DateTime utcNow)
    {
        ArgumentNullException.ThrowIfNull(destination);
        destination.Clear();

        lock (_capacityGate)
        {
            SweepExpiredLocked(utcNow, _timeProvider.GetTimestamp());

            // In insertion order, not dictionary order: a restore re-inserts in this order, and capacity eviction follows it.
            var ordered = new List<StoredRecord>(_records.Count);
            foreach (var pair in _records)
                ordered.Add(pair.Value);

            ordered.Sort(static (left, right) => left.Sequence.CompareTo(right.Sequence));

            // A reservation whose outcome frame is already enqueued exports that outcome: the frame is at or below the cut. A live
            // reservation that stamped no mutation frame has no write-ahead intent in the journal (a replicated write keeps its
            // outcome in the group log), so it is not exported as Started; restored records always are.
            foreach (var stored in CollectionsMarshal.AsSpan(ordered))
            {
                if (stored.Appended == null && stored.Record.State == IdempotencyRecordState.Started && stored is { CreatedTimestamp: not null, Stamped: false })
                    continue;

                destination.Add(stored.Appended ?? stored.Record);
            }
        }
    }

    /// <summary>Records the outcome of the attempt that holds the reservation of <paramref name="operationId" />.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="fingerprint">The deterministic mutation fingerprint.</param>
    /// <param name="responseBytes">The serialized response a retry replays.</param>
    /// <param name="reservation">The completion the reservation was acquired with.</param>
    /// <remarks>
    /// A reservation held by another attempt (this one's expired and a retry re-acquired the id) or an outcome already recorded is
    /// left as it is: a retry replays the first recorded outcome. The mutation already ran, so a missing record is admitted even at
    /// capacity.
    /// </remarks>
    internal void RecordSuccess(string operationId, string fingerprint, byte[] responseBytes, TaskCompletionSource? reservation)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
        ArgumentNullException.ThrowIfNull(responseBytes);

        lock (_capacityGate)
        {
            var (utcNow, timestamp) = ReadClock();
            SweepExpiredLocked(utcNow, timestamp);
            if (_records.TryGetValue(operationId, out var existing)
                && (existing.Record.State != IdempotencyRecordState.Started || !ReferenceEquals(existing.Reservation, reservation)))
                return;

            AdmitLocked(operationId, new StoredRecord(existing.Appended ?? CreateRecord(operationId, fingerprint, responseBytes, utcNow), timestamp, ++_nextSequence, null, null));
        }
    }

    /// <summary>Holds the outcome of a reservation whose outcome frame was just enqueued, under the journal mutation gate.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="fingerprint">The deterministic mutation fingerprint.</param>
    /// <param name="responseBytes">The serialized response.</param>
    /// <param name="reservation">The completion the reservation was acquired with.</param>
    /// <remarks>
    /// The record stays Started, so a live retry never replays an outcome before it is durable; a snapshot cut exports the held outcome
    /// instead, because replay skips the outcome frame at or below the cut.
    /// </remarks>
    internal void HoldAppendedOutcome(string operationId, string fingerprint, byte[] responseBytes, TaskCompletionSource? reservation)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
        ArgumentNullException.ThrowIfNull(responseBytes);

        lock (_capacityGate)
        {
            if (!_records.TryGetValue(operationId, out var existing)
                || existing.Record.State != IdempotencyRecordState.Started
                || !ReferenceEquals(existing.Reservation, reservation))
                return;

            _records[operationId] = existing with { Appended = CreateRecord(operationId, fingerprint, responseBytes, _timeProvider.GetUtcNow().UtcDateTime) };
        }
    }

    internal void RestoreRecord(string operationId, string fingerprint, ReadOnlyMemory<byte> responseBytes, DateTime createdUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);

        // Sweep and capacity-check against the server clock: the frame timestamp only dates the restored
        // record. Sweeping with a stale frame time would retain entries that are expired relative to now
        // and admit capacity pressure incorrectly.
        lock (_capacityGate)
        {
            var (utcNow, timestamp) = ReadClock();
            SweepExpiredLocked(utcNow, timestamp);
            AdmitLocked(operationId, new StoredRecord(CreateRestoredRecord(operationId, fingerprint, responseBytes, createdUtc), null, ++_nextSequence, null, null));
        }
    }

    /// <summary>Records a write-ahead intent and tracks its execution so a retry can join it instead of reporting an unknown outcome.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="fingerprint">The deterministic mutation fingerprint.</param>
    /// <param name="execution">
    /// Completion owned by the caller, registered when the reservation is acquired; the caller must pass it to
    /// <see cref="CompleteExecution" /> once the outcome is recorded or the intent released. <see langword="null" /> tracks nothing.
    /// </param>
    /// <param name="inFlight">
    /// For <see cref="IdempotencyReserveResult.AlreadyStarted" />, the execution of this process that owns the reservation;
    /// <see langword="null" /> when there is none to join (an intent rebuilt from the journal, or one whose execution already ended).
    /// </param>
    /// <returns>The reservation outcome for this caller.</returns>
    /// <exception cref="ServerOpIdMismatchException">When the stored fingerprint is non-null and differs.</exception>
    /// <exception cref="SquirixException">The store is full of reservations in flight and outcomes younger than the minimum retention.</exception>
    internal IdempotencyReserveResult ReserveIntent(string operationId, string fingerprint, TaskCompletionSource? execution, out Task? inFlight)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);

        inFlight = null;
        lock (_capacityGate)
        {
            var (utcNow, timestamp) = ReadClock();
            SweepExpiredLocked(utcNow, timestamp);

            if (_records.TryGetValue(operationId, out var existing))
            {
                ThrowIfFingerprintMismatch(existing.Record, fingerprint);
                if (existing.Record.State == IdempotencyRecordState.Completed)
                    return IdempotencyReserveResult.AlreadyCompleted;

                inFlight = _executions.GetValueOrDefault(operationId)?.Task;
                return IdempotencyReserveResult.AlreadyStarted;
            }

            MakeRoomForNewOperationLocked(utcNow, timestamp);
            var reserved = new StoredRecord(new PersistedIdempotencyRecord(operationId, fingerprint, utcNow), timestamp, ++_nextSequence, execution, null);
            _records[operationId] = reserved;
            _order.Add(operationId, reserved.Sequence, timestamp, utcNow, false);
            if (execution != null)
                _executions[operationId] = execution;

            return IdempotencyReserveResult.Acquired;
        }
    }

    /// <summary>Ends the execution registered by <see cref="ReserveIntent(string, string, TaskCompletionSource?, out Task?)" /> and wakes the retries joined to it.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="execution">The completion registered with the reservation.</param>
    /// <remarks>
    /// Call after the outcome is recorded or the intent released, so joined retries re-read a settled record. The completion always
    /// succeeds (it never faults), and it is signaled even when retention expiry or eviction already dropped the registration.
    /// </remarks>
    internal void CompleteExecution(string operationId, TaskCompletionSource execution)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentNullException.ThrowIfNull(execution);

        lock (_capacityGate)
        {
            // A reservation re-acquired after expiry, eviction or release owns a newer completion: leave it registered.
            if (_executions.TryGetValue(operationId, out var registered) && ReferenceEquals(registered, execution))
                _ = _executions.Remove(operationId);
        }

        _ = execution.TrySetResult();
    }

    /// <summary>Releases a write-ahead reservation when its execution failed without producing a durable outcome.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="fingerprint">The mutation fingerprint the reservation was acquired with.</param>
    /// <param name="reservation">The completion the reservation was acquired with; a reservation another attempt holds is kept.</param>
    internal void ReleaseIntent(string operationId, string fingerprint, TaskCompletionSource? reservation)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);

        lock (_capacityGate)
        {
            if (!_records.TryGetValue(operationId, out var existing))
                return;

            if (existing.Record.State != IdempotencyRecordState.Started || !ReferenceEquals(existing.Reservation, reservation))
                return;

            // A record restored from the journal or a snapshot has no monotonic origin and must outlive the failed attempt: its
            // mutation may already be durably committed.
            if (existing.CreatedTimestamp == null)
                return;

            if (!string.Equals(existing.Record.Fingerprint, fingerprint, StringComparison.Ordinal))
                return;

            _ = _records.Remove(operationId);
        }
    }

    /// <summary>Marks the live reservation of <paramref name="reservation" /> as write-ahead stamped, so a snapshot exports its Started record.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="reservation">The completion the reservation was acquired with.</param>
    /// <remarks>Called under the journal mutation gate right after the stamped frame was enqueued, so a snapshot cut never sees the frame without the mark.</remarks>
    internal void MarkStamped(string operationId, TaskCompletionSource? reservation)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);

        lock (_capacityGate)
        {
            if (_records.TryGetValue(operationId, out var existing)
                && existing.Record.State == IdempotencyRecordState.Started
                && ReferenceEquals(existing.Reservation, reservation))
                _records[operationId] = existing with { Stamped = true };
        }
    }

    /// <summary>Restores a write-ahead started record (write-ahead intent) reconstructed from a compaction/journal frame.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="fingerprint">The mutation fingerprint when the frame carries one; otherwise <see langword="null" /> or an empty string.</param>
    /// <param name="createdUtc">The frame timestamp.</param>
    internal void RestoreStarted(string operationId, string? fingerprint, DateTime createdUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);

        if (string.IsNullOrEmpty(fingerprint))
            fingerprint = null;

        // Sweep and capacity-check against the server clock; the frame timestamp only dates the restored record.
        lock (_capacityGate)
        {
            var (utcNow, timestamp) = ReadClock();
            SweepExpiredLocked(utcNow, timestamp);

            if (_records.ContainsKey(operationId))
                return;

            AdmitLocked(operationId, new StoredRecord(new PersistedIdempotencyRecord(operationId, fingerprint, createdUtc), null, ++_nextSequence, null, null));
        }
    }

    internal void RestoreSnapshotRecords(IReadOnlyList<PersistedIdempotencyRecord?> records)
    {
        ArgumentNullException.ThrowIfNull(records);

        lock (_capacityGate)
        {
            var (utcNow, timestamp) = ReadClock();
            SweepExpiredLocked(utcNow, timestamp);
            for (var i = 0; i < records.Count; i++)
            {
                var record = records[i] ?? ThrowHelper.Throw<PersistedIdempotencyRecord>(new ArgumentException("Idempotency record must not be null.", nameof(records)));
                AdmitLocked(record.OperationId, new StoredRecord(record, null, ++_nextSequence, null, null));
            }
        }
    }

    /// <summary>Drops the records whose retention passed on the server clock.</summary>
    internal void SweepExpired()
    {
        lock (_capacityGate)
        {
            var (utcNow, timestamp) = ReadClock();
            SweepExpiredLocked(utcNow, timestamp);
        }
    }

    internal bool TryReplay<TResponse>(string operationId, string fingerprint, MessageParser<TResponse> parser, out TResponse? response)
        where TResponse : IMessage<TResponse>
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
        ArgumentNullException.ThrowIfNull(parser);

        byte[] responseBytes;
        lock (_capacityGate)
        {
            var (utcNow, timestamp) = ReadClock();
            SweepExpiredLocked(utcNow, timestamp);

            if (!_records.TryGetValue(operationId, out var stored) || stored.Record.State == IdempotencyRecordState.Started)
            {
                response = default;
                return false;
            }

            ThrowIfFingerprintMismatch(stored.Record, fingerprint);
            responseBytes = stored.Record.ResponseBytes;
        }

        response = parser.ParseFrom(responseBytes);
        return true;
    }

    private static void ThrowIfFingerprintMismatch(PersistedIdempotencyRecord stored, string fingerprint)
    {
        if (stored.Fingerprint == null || string.Equals(stored.Fingerprint, fingerprint, StringComparison.Ordinal))
            return;

        throw new ServerOpIdMismatchException();
    }

    private static PersistedIdempotencyRecord CreateRecord(string operationId, string fingerprint, byte[] responseBytes, DateTime createdUtc) =>
        new(operationId, fingerprint, responseBytes, createdUtc);

    private static PersistedIdempotencyRecord CreateRestoredRecord(string operationId, string fingerprint, ReadOnlyMemory<byte> responseBytes, DateTime createdUtc)
    {
        var copy = BufferEx.CopyToOwned(responseBytes.Span);
        return new PersistedIdempotencyRecord(operationId, fingerprint, copy, createdUtc);
    }

    /// <summary>Reads the server clock once for one operation: the wall time dates records, the timestamp measures their age.</summary>
    /// <returns>The wall time and the monotonic timestamp.</returns>
    private (DateTime UtcNow, long Timestamp) ReadClock() => (_timeProvider.GetUtcNow().UtcDateTime, _timeProvider.GetTimestamp());

    /// <summary>Stores a record that must not be refused; the caller swept expired records first.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="stored">The record.</param>
    /// <remarks>A new record evicts completed outcomes of any age; when every record is in flight it exceeds the capacity.</remarks>
    private void AdmitLocked(string operationId, in StoredRecord stored)
    {
        if (!_records.ContainsKey(operationId))
        {
            while (_records.Count >= _options.MaxInFlightRecords && TryEvictOldestCompletedLocked(null, default, 0))
                _metrics.RecordEviction(_nodeId);
        }

        _records[operationId] = stored;
        _order.Add(operationId, stored.Sequence, stored.CreatedTimestamp, stored.Record.CreatedUtc, stored.Record.State == IdempotencyRecordState.Completed);
    }

    /// <summary>Makes room for a new reservation, evicting only completed outcomes older than the minimum retention.</summary>
    /// <param name="utcNow">The server clock wall time.</param>
    /// <param name="timestamp">The server clock monotonic timestamp.</param>
    /// <exception cref="SquirixException">Nothing can be evicted: a retryable refusal.</exception>
    private void MakeRoomForNewOperationLocked(DateTime utcNow, long timestamp)
    {
        // One eviction per new operation: a store left over capacity by a restore keeps its size and drains as records expire, and a
        // refusal never follows evictions that admitted nothing.
        if (_records.Count < _options.MaxInFlightRecords)
            return;

        if (!TryEvictOldestCompletedLocked(_options.MinRetention, utcNow, timestamp))
        {
            _metrics.RecordRejection(_nodeId);
            throw ServerOpContract.TooManyRequests("idempotency_store_capacity");
        }

        _metrics.RecordEviction(_nodeId);
    }

    private bool TryGetLive(string key, long sequence, out DateTime createdUtc)
    {
        if (_records.TryGetValue(key, out var live) && live.Sequence == sequence)
        {
            createdUtc = live.Record.CreatedUtc;
            return true;
        }

        createdUtc = default;
        return false;
    }

    private void SweepExpiredLocked(DateTime utcNow, long timestamp)
    {
        while (_order.TryTakeExpired(_options.Retention, utcNow, timestamp, out var key))
        {
            // An expired reservation can no longer be joined; its owner still signals the retries already joined to it.
            _ = _records.Remove(key);
            _ = _executions.Remove(key);
        }

        _order.Compact(_records.Count);
    }

    /// <summary>Evicts the completed outcome inserted first among those older than <paramref name="minAge" />.</summary>
    /// <param name="minAge">The age an evicted outcome must pass; <see langword="null" /> evicts one of any age.</param>
    /// <param name="utcNow">The server clock wall time.</param>
    /// <param name="timestamp">The server clock monotonic timestamp.</param>
    /// <returns><see langword="true" /> when an outcome was evicted.</returns>
    private bool TryEvictOldestCompletedLocked(TimeSpan? minAge, DateTime utcNow, long timestamp)
    {
        // Insertion order, not wall time: after a backward clock step the newest records carry the earliest wall times. Only completed
        // outcomes are ordered for eviction: a reservation in flight is never evicted, a retry would run alongside its first attempt.
        if (!_order.TryFindOldestCompleted(minAge, utcNow, timestamp, out var oldestKey) || !_records.Remove(oldestKey))
            return false;

        _ = _executions.Remove(oldestKey);
        return true;
    }

    /// <summary>A record with the monotonic timestamp it was created at in this process, its insertion sequence and the reservation holding it.</summary>
    /// <param name="Record">The persisted record.</param>
    /// <param name="CreatedTimestamp">The monotonic creation timestamp; <see langword="null" /> for a record restored from disk.</param>
    /// <param name="Sequence">The insertion sequence capacity eviction orders by.</param>
    /// <param name="Reservation">The completion of the attempt that acquired a Started record; only that attempt settles it.</param>
    /// <param name="Appended">The outcome of a Started record whose outcome frame is enqueued but not yet durable; a snapshot exports it.</param>
    /// <param name="Stamped">Whether a mutation frame stamped with the operation id was enqueued for this live reservation.</param>
    [Immutable]
    private readonly record struct StoredRecord(
        PersistedIdempotencyRecord Record,
        long? CreatedTimestamp,
        long Sequence,
        TaskCompletionSource? Reservation,
        PersistedIdempotencyRecord? Appended,
        bool Stamped = false);
}
