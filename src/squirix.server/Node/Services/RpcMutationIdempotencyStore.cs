using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Google.Protobuf;
using Squirix.Server.Errors;

namespace Squirix.Server.Node.Services;

/// <summary>Unified in-memory and durable idempotency store for mutating cache RPC outcomes.</summary>
internal sealed class RpcMutationIdempotencyStore
{
    private readonly ConcurrentDictionary<string, StoredOutcome> _records = new(StringComparer.Ordinal);
    private readonly TimeSpan _retention;

    public RpcMutationIdempotencyStore(TimeSpan? retention = null)
    {
        _retention = retention ?? TimeSpan.FromMinutes(15);
    }

    public bool TryReplay<TResponse>(string operationId, string fingerprint, MessageParser<TResponse> parser, out TResponse? response)
        where TResponse : IMessage<TResponse>
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
        ArgumentNullException.ThrowIfNull(parser);

        SweepExpired(DateTime.UtcNow);

        if (!_records.TryGetValue(operationId, out var stored))
        {
            response = default;
            return false;
        }

        if (!string.Equals(stored.Fingerprint, fingerprint, StringComparison.Ordinal))
            throw new OperationIdReuseMismatchException();

        response = parser.ParseFrom(stored.ResponseBytes);
        return true;
    }

    public void RecordSuccess(string operationId, string fingerprint, byte[] responseBytes)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
        ArgumentNullException.ThrowIfNull(responseBytes);

        _records[operationId] = new StoredOutcome(fingerprint, responseBytes, DateTime.UtcNow);
    }

    public void RestoreRecord(string operationId, string fingerprint, ReadOnlyMemory<byte> responseBytes, DateTime createdUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);

        // ZA0302: exact-size owned buffer escape; the store must outlive the borrowed frame buffer.
#pragma warning disable ZA0302
        var copy = new byte[responseBytes.Length];
#pragma warning restore ZA0302
        responseBytes.Span.CopyTo(copy);
        _records[operationId] = new StoredOutcome(fingerprint, copy, createdUtc);
    }

    public IReadOnlyList<PersistedIdempotencyRecord> ExportSnapshot(DateTime utcNow)
    {
        SweepExpired(utcNow);

        var snapshot = new List<PersistedIdempotencyRecord>(_records.Count);
        foreach (var pair in _records)
        {
            var persistedIdempotencyRecord = new PersistedIdempotencyRecord
            {
                OperationId = pair.Key,
                Fingerprint = pair.Value.Fingerprint,
                CreatedUtc = pair.Value.CreatedUtc,
                ResponseBytes = pair.Value.ResponseBytes,
            };
            snapshot.Add(persistedIdempotencyRecord);
        }

        return snapshot;
    }

    public void RestoreSnapshotRecords(IReadOnlyList<PersistedIdempotencyRecord> records)
    {
        ArgumentNullException.ThrowIfNull(records);

        for (var i = 0; i < records.Count; i++)
        {
            var record = records[i] ?? throw new ArgumentException("Idempotency record must not be null.", nameof(records));
            _records[record.OperationId] = new StoredOutcome(record.Fingerprint, record.ResponseBytes, record.CreatedUtc);
        }
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

    private void SweepExpired(DateTime utcNow)
    {
        foreach (var (key, value) in _records)
        {
            if (utcNow - value.CreatedUtc > _retention)
                _ = _records.TryRemove(key, out _);
        }
    }

    private sealed record StoredOutcome(string Fingerprint, byte[] ResponseBytes, DateTime CreatedUtc);
}
