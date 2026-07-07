using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Google.Protobuf;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Node.Services;

/// <summary>Unified in-memory and durable idempotency store for mutating cache RPC outcomes.</summary>
internal sealed class RpcMutationIdempotencyStore
{
    private readonly IdempotencyOptions _options;
    private readonly ConcurrentDictionary<string, PersistedIdempotencyRecord> _records = new(StringComparer.Ordinal);
    private readonly Lock _capacityGate = new();
    private readonly string _nodeId;

    public RpcMutationIdempotencyStore()
        : this(new IdempotencyOptions(), "local")
    {
    }

    public RpcMutationIdempotencyStore(TimeSpan retention)
        : this(new IdempotencyOptions { Retention = retention }, "local")
    {
    }

    public RpcMutationIdempotencyStore(IdempotencyOptions options, string nodeId)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options;
        _nodeId = string.IsNullOrWhiteSpace(nodeId) ? "local" : nodeId;
    }

    public int RecordCount
    {
        get
        {
            lock (_capacityGate)
                return _records.Count;
        }
    }

    public bool TryReplay<TResponse>(string operationId, string fingerprint, MessageParser<TResponse> parser, out TResponse? response)
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
                throw new OperationIdReuseMismatchException();

            responseBytes = stored.ResponseBytes;
        }

        response = parser.ParseFrom(responseBytes);
        return true;
    }

    public void RecordSuccess(string operationId, string fingerprint, byte[] responseBytes)
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

    public void RestoreRecord(string operationId, string fingerprint, ReadOnlyMemory<byte> responseBytes, DateTime createdUtc)
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

    public void ExportSnapshot(List<PersistedIdempotencyRecord> destination, DateTime utcNow)
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

    public IReadOnlyList<PersistedIdempotencyRecord> ExportSnapshot(DateTime utcNow)
    {
        var snapshot = new List<PersistedIdempotencyRecord>();
        ExportSnapshot(snapshot, utcNow);
        return snapshot;
    }

    public void RestoreSnapshotRecords(IReadOnlyList<PersistedIdempotencyRecord> records)
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

    public void SweepExpired(DateTime utcNow)
    {
        lock (_capacityGate)
            SweepExpiredLocked(utcNow);
    }

    internal static byte[] SerializeResponseBytes(IMessage response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var size = response.CalculateSize();

        // ZA0302: exact-size owned buffer; the store must retain serialized response bytes.
#pragma warning disable ZA0302
        var bytes = new byte[size];
#pragma warning restore ZA0302
        response.WriteTo(bytes);
        return bytes;
    }

    private static PersistedIdempotencyRecord CreateRecord(string operationId, string fingerprint, byte[] responseBytes, DateTime createdUtc) => new()
    {
        OperationId = operationId,
        Fingerprint = fingerprint,
        ResponseBytes = responseBytes,
        CreatedUtc = createdUtc,
    };

    private static PersistedIdempotencyRecord CreateRestoredRecord(string operationId, string fingerprint, ReadOnlyMemory<byte> responseBytes, DateTime createdUtc)
    {
        // ZA0302: exact-size owned buffer escape; the store must outlive the borrowed frame buffer.
#pragma warning disable ZA0302
        var copy = new byte[responseBytes.Length];
#pragma warning restore ZA0302
        responseBytes.Span.CopyTo(copy);
        return new PersistedIdempotencyRecord
        {
            OperationId = operationId,
            Fingerprint = fingerprint,
            ResponseBytes = copy,
            CreatedUtc = createdUtc,
        };
    }

    private void EnsureCapacityForNewRecordLocked(DateTime utcNow)
    {
        SweepExpiredLocked(utcNow);
        while (_records.Count >= _options.MaxInFlightRecords)
        {
            if (!TryEvictOldestLocked())
            {
                IdempotencyMetrics.RecordRejection(_nodeId);
                throw CacheOperationContract.TooManyRequests("idempotency_store_capacity");
            }

            IdempotencyMetrics.RecordEviction(_nodeId);
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
