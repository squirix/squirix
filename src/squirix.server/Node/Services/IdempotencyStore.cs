using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

internal sealed class IdempotencyStore : IDisposable
{
    private readonly SemaphoreSlim _mutex = new(1, 1);
    private readonly ConcurrentDictionary<string, StoredOperation> _records = new(StringComparer.Ordinal);
    private readonly TimeSpan _retention;

    public IdempotencyStore(TimeSpan? retention = null)
    {
        _retention = retention ?? TimeSpan.FromMinutes(15);
    }

    public static string BuildInsertFingerprint(string key, ReadOnlySpan<byte> payload) => string.Concat("insert|", key, "|", HashPayload(payload));

    public IReadOnlyCollection<PersistedIdempotencyRecord> ExportSnapshot(DateTime utcNow)
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
                Outcome = ToPersistedOutcome(pair.Value.Outcome),
            };
            snapshot.Add(persistedIdempotencyRecord);
        }

        return snapshot;
    }

    public void RestoreInsert(string operationId, string fingerprint) => Restore(operationId, fingerprint, InsertOutcome.Instance);

    public void RestoreSnapshotRecords(IEnumerable<PersistedIdempotencyRecord> records)
    {
        ArgumentNullException.ThrowIfNull(records);

        var restored = new List<KeyValuePair<string, StoredOperation>>();
        foreach (var record in records)
        {
            ArgumentNullException.ThrowIfNull(record);
            restored.Add(
                new KeyValuePair<string, StoredOperation>(record.OperationId, new StoredOperation(record.Fingerprint, FromPersistedOutcome(record.Outcome), record.CreatedUtc)));
        }

        foreach (var (operationId, operation) in restored)
            _records[operationId] = operation;
    }

    public void Dispose() => _mutex.Dispose();

    private static InsertOutcome FromPersistedOutcome(PersistedIdempotencyOutcome outcome)
    {
        return outcome.Kind switch
        {
            "insert" => InsertOutcome.Instance,
            _ => throw new NotSupportedException($"Unsupported persisted outcome kind: {outcome.Kind}"),
        };
    }

    private static string HashPayload(ReadOnlySpan<byte> payload)
    {
        Span<byte> digest = stackalloc byte[32];
        _ = SHA256.HashData(payload, digest);
        return HexFormat.FormatSha256HexUpper(digest);
    }

    private static PersistedIdempotencyOutcome ToPersistedOutcome(IdempotencyOutcome outcome)
    {
        return outcome switch
        {
            InsertOutcome => new PersistedIdempotencyOutcome { Kind = "insert" },
            _ => throw new NotSupportedException($"Unsupported idempotency outcome type: {outcome.GetType().Name}"),
        };
    }

    private void Restore(string operationId, string fingerprint, IdempotencyOutcome outcome)
    {
        if (string.IsNullOrWhiteSpace(operationId))
            return;

        _records[operationId] = new StoredOperation(fingerprint, outcome, DateTime.UtcNow);
    }

    private void SweepExpired(DateTime utcNow)
    {
        foreach (var (key, value) in _records)
        {
            if (utcNow - value.CreatedUtc > _retention)
                _ = _records.TryRemove(key, out _);
        }
    }

    private sealed record StoredOperation(string Fingerprint, IdempotencyOutcome Outcome, DateTime CreatedUtc);
}
