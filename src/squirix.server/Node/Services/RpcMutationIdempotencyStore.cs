using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using Google.Protobuf;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Unified in-memory and durable idempotency store for mutating cache RPC outcomes.</summary>
[Mutable]
internal sealed class RpcMutationIdempotencyStore : IIdempotencySnapshotExporter
{
    private readonly Lock _capacityGate = new();
    private readonly IdempotencyMetrics _metrics;
    private readonly string _nodeId;
    private readonly IdempotencyOptions _options;
    private readonly Dictionary<string, PersistedIdempotencyRecord> _records = new(StringComparer.Ordinal);

    internal RpcMutationIdempotencyStore(IdempotencyOptions options, string nodeId, IdempotencyMetrics metrics)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentNullException.ThrowIfNull(metrics);
        options.Validate();
        _options = options;
        _nodeId = nodeId;
        _metrics = metrics;
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

        var utcNow = DateTime.UtcNow;
        lock (_capacityGate)
        {
            SweepExpiredLocked(utcNow);
            UpsertLocked(operationId, CreateRecord(operationId, fingerprint, responseBytes, utcNow), utcNow);
        }
    }

    internal void RestoreRecord(string operationId, string fingerprint, ReadOnlyMemory<byte> responseBytes, DateTime createdUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);

        // Sweep and capacity-check against wall clock: the frame timestamp only dates the restored
        // record. Sweeping with a stale frame time would retain entries that are expired relative to now
        // and admit capacity pressure incorrectly.
        var utcNow = DateTime.UtcNow;
        lock (_capacityGate)
        {
            SweepExpiredLocked(utcNow);
            UpsertLocked(operationId, CreateRestoredRecord(operationId, fingerprint, responseBytes, createdUtc), utcNow);
        }
    }

    /// <summary>Records a write-ahead intent for an operation about to execute; the caller replays a completed outcome or surfaces the ambiguous outcome to the RPC layer.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="fingerprint">The deterministic mutation fingerprint.</param>
    /// <returns>The reservation outcome for this caller.</returns>
    /// <exception cref="ServerOpIdMismatchException">When the stored fingerprint is non-null and differs.</exception>
    internal IdempotencyReserveResult ReserveIntent(string operationId, string fingerprint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);

        var utcNow = DateTime.UtcNow;
        lock (_capacityGate)
        {
            SweepExpiredLocked(utcNow);

            if (_records.TryGetValue(operationId, out var existing))
            {
                ThrowIfFingerprintMismatch(existing, fingerprint);
                return existing.State == IdempotencyRecordState.Completed ? IdempotencyReserveResult.AlreadyCompleted : IdempotencyReserveResult.AlreadyStarted;
            }

            UpsertLocked(operationId, new PersistedIdempotencyRecord(operationId, fingerprint, utcNow), utcNow);
            return IdempotencyReserveResult.Acquired;
        }
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

            if (existing.State != IdempotencyRecordState.Started)
                return;

            // Reservations reconstructed from journal mutation frames carry no fingerprint and must outlive the
            // failed attempt: their mutation may already be durably committed.
            if (existing.Fingerprint == null)
                return;

            if (!string.Equals(existing.Fingerprint, fingerprint, StringComparison.Ordinal))
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

        // Sweep and capacity-check against wall clock; the frame timestamp only dates the restored record.
        var utcNow = DateTime.UtcNow;
        lock (_capacityGate)
        {
            SweepExpiredLocked(utcNow);

            if (_records.ContainsKey(operationId))
                return;

            UpsertLocked(operationId, new PersistedIdempotencyRecord(operationId, fingerprint, createdUtc), utcNow);
        }
    }

    internal void RestoreSnapshotRecords(IReadOnlyList<PersistedIdempotencyRecord?> records)
    {
        ArgumentNullException.ThrowIfNull(records);

        lock (_capacityGate)
        {
            var utcNow = DateTime.UtcNow;
            SweepExpiredLocked(utcNow);
            for (var i = 0; i < records.Count; i++)
            {
                var record = records[i] ?? ThrowHelper.Throw<PersistedIdempotencyRecord>(new ArgumentException("Idempotency record must not be null.", nameof(records)));
                UpsertLocked(record.OperationId, record, utcNow);
            }
        }
    }

    internal void SweepExpired(DateTime utcNow)
    {
        lock (_capacityGate)
            SweepExpiredLocked(utcNow);
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
            SweepExpiredLocked(DateTime.UtcNow);

            if (!_records.TryGetValue(operationId, out var stored) || stored.State == IdempotencyRecordState.Started)
            {
                response = default;
                return false;
            }

            ThrowIfFingerprintMismatch(stored, fingerprint);
            responseBytes = stored.ResponseBytes;
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

    private void UpsertLocked(string operationId, PersistedIdempotencyRecord record, DateTime utcNow)
    {
#pragma warning disable MA0160 // Intentional single-lookup TryGetValue: ContainsKey plus indexer would hash twice (see ZA0105).
        if (_records.TryGetValue(operationId, out _))
#pragma warning restore MA0160
        {
            _records[operationId] = record;
            return;
        }

        EnsureCapacityForNewRecordLocked(utcNow);
        _records[operationId] = record;
    }

    private void EnsureCapacityForNewRecordLocked(DateTime utcNow)
    {
        SweepExpiredLocked(utcNow);
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
            SweepExpiredLocked(utcNow);

            foreach (var pair in _records)
                destination.Add(pair.Value);
        }
    }

    private void SweepExpiredLocked(DateTime utcNow)
    {
        // Collect first: Dictionary forbids removal during enumeration.
        List<string>? expired = null;
        foreach (var (key, value) in _records)
        {
            if (utcNow - value.CreatedUtc <= _options.Retention)
                continue;
            expired ??= [];
            expired.Add(key);
        }

        if (expired == null)
            return;

        foreach (var key in CollectionsMarshal.AsSpan(expired))
            _ = _records.Remove(key);
    }

    private bool TryEvictOldestLocked()
    {
        string? oldestKey = null;
        var oldestCreatedUtc = DateTime.MaxValue;
        foreach (var pair in _records)
        {
            if (pair.Value.CreatedUtc >= oldestCreatedUtc)
                continue;

            oldestCreatedUtc = pair.Value.CreatedUtc;
            oldestKey = pair.Key;
        }

        return oldestKey != null && _records.Remove(oldestKey);
    }
}
