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
/// restored record has no monotonic origin and ages by wall time. Capacity eviction drops the record inserted first, whatever its wall
/// time, and snapshots keep that insertion order.
/// </remarks>
[Mutable]
internal sealed class RpcMutationIdempotencyStore : IIdempotencySnapshotExporter
{
    private readonly Lock _capacityGate = new();

    /// <summary>
    /// Executions in flight for Started reservations acquired in this process, keyed like the records. A retry joins the execution
    /// instead of reporting an unknown outcome; intents rebuilt from the journal never have an entry here.
    /// </summary>
    private readonly Dictionary<string, TaskCompletionSource> _executions = [with(StringComparer.Ordinal)];
    private readonly IdempotencyMetrics _metrics;
    private readonly string _nodeId;
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
    }

    internal int ExecutionCount
    {
        get
        {
            lock (_capacityGate)
                return _executions.Count;
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

    void IIdempotencySnapshotExporter.ExportSnapshot(List<PersistedIdempotencyRecord> destination, DateTime utcNow) => ExportSnapshotCore(destination, utcNow);

    internal void RecordSuccess(string operationId, string fingerprint, byte[] responseBytes)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
        ArgumentNullException.ThrowIfNull(responseBytes);

        lock (_capacityGate)
        {
            var (utcNow, timestamp) = ReadClock();
            SweepExpiredLocked(utcNow, timestamp);
            UpsertLocked(operationId, CreateRecord(operationId, fingerprint, responseBytes, utcNow), timestamp);
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
            UpsertLocked(operationId, CreateRestoredRecord(operationId, fingerprint, responseBytes, createdUtc), null);
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

            UpsertLocked(operationId, new PersistedIdempotencyRecord(operationId, fingerprint, utcNow), timestamp);
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

    internal void RestoreStarted(string operationId, DateTime createdUtc) => RestoreStarted(operationId, null, createdUtc);

    /// <summary>Releases a write-ahead reservation when its execution failed without producing a durable outcome.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="fingerprint">The mutation fingerprint the reservation was acquired with.</param>
    internal void ReleaseIntent(string operationId, string fingerprint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);

        lock (_capacityGate)
        {
            if (!_records.TryGetValue(operationId, out var existing))
                return;

            if (existing.Record.State != IdempotencyRecordState.Started)
                return;

            // Reservations reconstructed from journal mutation frames carry no fingerprint and must outlive the
            // failed attempt: their mutation may already be durably committed.
            if (existing.Record.Fingerprint == null)
                return;

            if (!string.Equals(existing.Record.Fingerprint, fingerprint, StringComparison.Ordinal))
                return;

            _ = _records.Remove(operationId);
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

            UpsertLocked(operationId, new PersistedIdempotencyRecord(operationId, fingerprint, createdUtc), null);
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
                UpsertLocked(record.OperationId, record, null);
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

    /// <summary>Stores a record under the next insertion sequence; the caller swept expired records first with the same clock reading.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="record">The record.</param>
    /// <param name="createdTimestamp">The monotonic creation timestamp of a record created in this process; <see langword="null" /> for a restored one.</param>
    private void UpsertLocked(string operationId, PersistedIdempotencyRecord record, long? createdTimestamp)
    {
        var stored = new StoredRecord(record, createdTimestamp, ++_nextSequence);
#pragma warning disable MA0160 // Intentional single-lookup TryGetValue: ContainsKey plus indexer would hash twice (see ZA0105).
        if (_records.TryGetValue(operationId, out _))
#pragma warning restore MA0160
        {
            _records[operationId] = stored;
            return;
        }

        EnsureCapacityForNewRecordLocked();
        _records[operationId] = stored;
    }

    private void EnsureCapacityForNewRecordLocked()
    {
        while (_records.Count >= _options.MaxInFlightRecords)
        {
            if (!TryEvictOldestLocked())
            {
                _metrics.RecordRejection(_nodeId);
                throw ServerOpContract.TooManyRequests("idempotency_store_capacity");
            }

            _metrics.RecordEviction(_nodeId);
        }
    }

    private void ExportSnapshotCore(List<PersistedIdempotencyRecord> destination, DateTime utcNow)
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
            foreach (var stored in CollectionsMarshal.AsSpan(ordered))
                destination.Add(stored.Record);
        }
    }

    /// <summary>Decides whether a record is past its retention at <paramref name="utcNow" /> and <paramref name="timestamp" />.</summary>
    /// <param name="stored">The record.</param>
    /// <param name="utcNow">The server clock wall time.</param>
    /// <param name="timestamp">The server clock monotonic timestamp.</param>
    /// <returns><see langword="true" /> when the record expired.</returns>
    private bool IsExpired(in StoredRecord stored, DateTime utcNow, long timestamp)
    {
        // A record created in this process must also be past the retention on monotonic time, so a forward wall step cannot purge it.
        return utcNow - stored.Record.CreatedUtc > _options.Retention
            && (stored.CreatedTimestamp is not { } created || _timeProvider.GetElapsedTime(created, timestamp) > _options.Retention);
    }

    private void SweepExpiredLocked(DateTime utcNow, long timestamp)
    {
        // Collect first: Dictionary forbids removal during enumeration.
        List<string>? expired = null;
        foreach (var (key, value) in _records)
        {
            if (!IsExpired(in value, utcNow, timestamp))
                continue;
            expired ??= [];
            expired.Add(key);
        }

        if (expired == null)
            return;

        // An expired reservation can no longer be joined; its owner still signals the retries already joined to it.
        foreach (var key in CollectionsMarshal.AsSpan(expired))
        {
            _ = _records.Remove(key);
            _ = _executions.Remove(key);
        }
    }

    private bool TryEvictOldestLocked()
    {
        // Insertion order, not wall time: after a backward clock step the newest records carry the earliest wall times.
        string? oldestKey = null;
        var oldestSequence = long.MaxValue;
        foreach (var pair in _records)
        {
            if (pair.Value.Sequence >= oldestSequence)
                continue;

            oldestSequence = pair.Value.Sequence;
            oldestKey = pair.Key;
        }

        if (oldestKey == null || !_records.Remove(oldestKey))
            return false;

        _ = _executions.Remove(oldestKey);
        return true;
    }

    /// <summary>A record with the monotonic timestamp it was created at in this process and its insertion sequence.</summary>
    /// <param name="Record">The persisted record.</param>
    /// <param name="CreatedTimestamp">The monotonic creation timestamp; <see langword="null" /> for a record restored from disk.</param>
    /// <param name="Sequence">The insertion sequence capacity eviction orders by.</param>
    private readonly record struct StoredRecord(PersistedIdempotencyRecord Record, long? CreatedTimestamp, long Sequence);
}
