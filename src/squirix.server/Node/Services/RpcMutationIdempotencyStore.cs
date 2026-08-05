using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Google.Protobuf;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Unified in-memory and durable idempotency store for mutating cache RPC outcomes.</summary>
internal sealed class RpcMutationIdempotencyStore : IIdempotencySnapshotExporter
{
    private readonly Lock _capacityGate = new();
    private readonly string _nodeId;
    private readonly IdempotencyOptions _options;
    private readonly ConcurrentDictionary<string, PersistedIdempotencyRecord> _records = new(StringComparer.Ordinal);

    internal RpcMutationIdempotencyStore()
        : this(new IdempotencyOptions(), "local")
    {
    }

    internal RpcMutationIdempotencyStore(IdempotencyOptions options, string nodeId)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options;
        _nodeId = string.IsNullOrWhiteSpace(nodeId) ? "local" : nodeId;
    }

    internal RpcMutationIdempotencyStore(TimeSpan retention)
        : this(new IdempotencyOptions { Retention = retention }, "local")
    {
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

    internal static byte[] SerializeResponseBytes(IMessage response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var size = response.CalculateSize();
        var bytes = BufferEx.Owned(size);
        response.WriteTo(bytes);
        return bytes;
    }

    internal void RecordSuccess(string operationId, string fingerprint, byte[] responseBytes)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
        ArgumentNullException.ThrowIfNull(responseBytes);

        var utcNow = DateTime.UtcNow;
        lock (_capacityGate)
        {
            SweepExpiredLocked(utcNow);
            if (_records.TryGetValue(operationId, out var existing))
            {
                _ = existing.OperationId;
                _records[operationId] = CreateRecord(operationId, fingerprint, responseBytes, utcNow);
                return;
            }

            EnsureCapacityForNewRecordLocked(utcNow);
            _records[operationId] = CreateRecord(operationId, fingerprint, responseBytes, utcNow);
        }
    }

    internal void RestoreRecord(string operationId, string fingerprint, ReadOnlyMemory<byte> responseBytes, DateTime createdUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);

        lock (_capacityGate)
        {
            SweepExpiredLocked(createdUtc);
            if (_records.TryGetValue(operationId, out var existing))
            {
                _ = existing.OperationId;
                _records[operationId] = CreateRestoredRecord(operationId, fingerprint, responseBytes, createdUtc);
                return;
            }

            EnsureCapacityForNewRecordLocked(createdUtc);
            _records[operationId] = CreateRestoredRecord(operationId, fingerprint, responseBytes, createdUtc);
        }
    }

    internal void RestoreSnapshotRecords(IReadOnlyList<PersistedIdempotencyRecord> records)
    {
        ArgumentNullException.ThrowIfNull(records);

        lock (_capacityGate)
        {
            var utcNow = DateTime.UtcNow;
            SweepExpiredLocked(utcNow);
            for (var i = 0; i < records.Count; i++)
            {
                var record = records[i] ?? throw new ArgumentException("Idempotency record must not be null.", nameof(records));
                if (_records.TryGetValue(record.OperationId, out var existing))
                {
                    _ = existing.OperationId;
                    _records[record.OperationId] = record;
                    continue;
                }

                EnsureCapacityForNewRecordLocked(utcNow);
                _records[record.OperationId] = record;
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

            if (!_records.TryGetValue(operationId, out var stored))
            {
                response = default;
                return false;
            }

            if (!string.Equals(stored.Fingerprint, fingerprint, StringComparison.Ordinal))
                throw new ServerOpIdMismatchException();

            responseBytes = stored.ResponseBytes;
        }

        response = parser.ParseFrom(responseBytes);
        return true;
    }

    private static PersistedIdempotencyRecord CreateRecord(string operationId, string fingerprint, byte[] responseBytes, DateTime createdUtc) =>
        new(operationId, fingerprint, responseBytes, createdUtc);

    private static PersistedIdempotencyRecord CreateRestoredRecord(string operationId, string fingerprint, ReadOnlyMemory<byte> responseBytes, DateTime createdUtc)
    {
        var copy = BufferEx.CopyToOwned(responseBytes.Span);
        return new PersistedIdempotencyRecord(operationId, fingerprint, copy, createdUtc);
    }

    private void EnsureCapacityForNewRecordLocked(DateTime utcNow)
    {
        SweepExpiredLocked(utcNow);
        while (_records.Count >= _options.MaxInFlightRecords)
        {
            if (!TryEvictOldestLocked())
            {
                IdempotencyMetrics.RecordRejection(_nodeId);
                throw ServerOpContract.TooManyRequests("idempotency_store_capacity");
            }

            IdempotencyMetrics.RecordEviction(_nodeId);
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
        foreach (var (key, value) in _records)
        {
            if (utcNow - value.CreatedUtc > _options.Retention)
                _ = _records.TryRemove(key, out _);
        }
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

        if (oldestKey is null)
            return false;

        return _records.TryRemove(oldestKey, out _);
    }
}
