using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.UnitTests.Support;

/// <summary>
/// Reference model of <see cref="RpcMutationIdempotencyStore" /> that scans every record on every call, the way the store did before its
/// expiry order existed. A model test compares the store against it step by step.
/// </summary>
[Mutable]
internal sealed class IdempotencyStoreOracle
{
    private readonly int _capacity;
    private readonly TimeProvider _clock;
    private readonly Dictionary<string, TaskCompletionSource> _executions = [with(StringComparer.Ordinal)];
    private readonly TimeSpan _minRetention;
    private readonly Dictionary<string, Rec> _records = [with(StringComparer.Ordinal)];
    private readonly TimeSpan _retention;
    private long _nextSequence;

    internal IdempotencyStoreOracle(TimeProvider clock, int capacity, TimeSpan retention, TimeSpan minRetention)
    {
        _clock = clock;
        _capacity = capacity;
        _retention = retention;
        _minRetention = minRetention;
    }

    internal int ExecutionCount => _executions.Count;

    internal int RecordCount => _records.Count;

    /// <summary>Mirrors the reservation call.</summary>
    /// <param name="id">The operation identifier.</param>
    /// <param name="execution">The execution registered with the reservation.</param>
    /// <returns>The result, <see langword="null" /> for a capacity refusal, and the execution a retry joins.</returns>
    internal (IdempotencyReserveResult? Result, Task? InFlight) Reserve(string id, TaskCompletionSource? execution)
    {
        var (utcNow, timestamp) = Sweep();
        if (_records.TryGetValue(id, out var existing))
            return existing.Completed ? (IdempotencyReserveResult.AlreadyCompleted, null) : (IdempotencyReserveResult.AlreadyStarted, _executions.GetValueOrDefault(id)?.Task);

        if (_records.Count >= _capacity && !TryEvict(_minRetention, utcNow, timestamp))
            return (null, null);

        _records[id] = new Rec(id, utcNow, timestamp, ++_nextSequence, false, execution);
        if (execution != null)
            _executions[id] = execution;

        return (IdempotencyReserveResult.Acquired, null);
    }

    internal void Release(string id, TaskCompletionSource? reservation)
    {
        if (_records.TryGetValue(id, out var existing) && !existing.Completed && ReferenceEquals(existing.Reservation, reservation) && existing.Stamp != null)
            _ = _records.Remove(id);
    }

    internal void Mark(string id, TaskCompletionSource? reservation)
    {
        if (_records.TryGetValue(id, out var existing) && !existing.Completed && ReferenceEquals(existing.Reservation, reservation))
            existing.Stamped = true;
    }

    internal void Hold(string id, TaskCompletionSource? reservation)
    {
        if (_records.TryGetValue(id, out var existing) && !existing.Completed && ReferenceEquals(existing.Reservation, reservation))
            existing.AppendedUtc = _clock.GetUtcNow().UtcDateTime;
    }

    internal void Success(string id, TaskCompletionSource? reservation)
    {
        var (utcNow, timestamp) = Sweep();
        _ = _records.TryGetValue(id, out var existing);
        if (existing != null && (existing.Completed || !ReferenceEquals(existing.Reservation, reservation)))
            return;

        Admit(new Rec(id, existing?.AppendedUtc ?? utcNow, timestamp, ++_nextSequence, true, null));
    }

    internal void Restore(string id, bool completed, DateTime createdUtc)
    {
        _ = Sweep();
        if (!completed && _records.ContainsKey(id))
            return;

        Admit(new Rec(id, createdUtc, null, ++_nextSequence, completed, null));
    }

    internal void RestoreSnapshot(IReadOnlyList<PersistedIdempotencyRecord> records)
    {
        _ = Sweep();
        for (var i = 0; i < records.Count; i++)
            Admit(new Rec(records[i].OperationId, records[i].CreatedUtc, null, ++_nextSequence, records[i].State == IdempotencyRecordState.Completed, null));
    }

    internal void CompleteExecution(string id, TaskCompletionSource execution)
    {
        if (_executions.TryGetValue(id, out var registered) && ReferenceEquals(registered, execution))
            _ = _executions.Remove(id);
    }

    internal bool Replay(string id)
    {
        _ = Sweep();
        return _records.TryGetValue(id, out var stored) && stored.Completed;
    }

    internal void SweepExpired() => _ = Sweep();

    internal List<(string Id, bool Completed, DateTime CreatedUtc)> Export()
    {
        _ = Sweep();
        var ordered = new List<Rec>(_records.Values);
        ordered.Sort(static (left, right) => left.Sequence.CompareTo(right.Sequence));
        var result = new List<(string, bool, DateTime)>();
        foreach (var stored in CollectionsMarshal.AsSpan(ordered))
        {
            if (stored.AppendedUtc == null && !stored.Completed && stored.Stamp != null && !stored.Stamped)
                continue;

            result.Add(stored.AppendedUtc is { } appended ? (stored.Id, true, appended) : (stored.Id, stored.Completed, stored.CreatedUtc));
        }

        return result;
    }

    private (DateTime UtcNow, long Timestamp) Sweep()
    {
        var utcNow = _clock.GetUtcNow().UtcDateTime;
        var timestamp = _clock.GetTimestamp();
        var expired = new List<string>();
        foreach (var (key, value) in _records)
        {
            if (utcNow - value.CreatedUtc > _retention && (value.Stamp is not { } created || _clock.GetElapsedTime(created, timestamp) > _retention))
                expired.Add(key);
        }

        foreach (var key in CollectionsMarshal.AsSpan(expired))
        {
            _ = _records.Remove(key);
            _ = _executions.Remove(key);
        }

        return (utcNow, timestamp);
    }

    private void Admit(Rec stored)
    {
        if (!_records.ContainsKey(stored.Id))
        {
            var evicted = true;
            while (_records.Count >= _capacity && evicted)
                evicted = TryEvict(null, default, 0);
        }

        _records[stored.Id] = stored;
    }

    private bool TryEvict(TimeSpan? minAge, DateTime utcNow, long timestamp)
    {
        string? oldestKey = null;
        var oldestSequence = long.MaxValue;
        foreach (var pair in _records)
        {
            if (pair.Value.Sequence >= oldestSequence || !pair.Value.Completed)
                continue;

            var lived = pair.Value.Stamp is { } created ? _clock.GetElapsedTime(created, timestamp) : utcNow - pair.Value.CreatedUtc;
            if (minAge is { } age && lived <= age)
                continue;

            oldestSequence = pair.Value.Sequence;
            oldestKey = pair.Key;
        }

        if (oldestKey == null)
            return false;

        _ = _records.Remove(oldestKey);
        _ = _executions.Remove(oldestKey);
        return true;
    }

    private sealed class Rec
    {
        internal Rec(string id, DateTime createdUtc, long? stamp, long sequence, bool completed, TaskCompletionSource? reservation)
        {
            Id = id;
            CreatedUtc = createdUtc;
            Stamp = stamp;
            Sequence = sequence;
            Completed = completed;
            Reservation = reservation;
        }

        internal DateTime? AppendedUtc { get; set; }

        internal bool Completed { get; }

        internal DateTime CreatedUtc { get; }

        internal string Id { get; }

        internal TaskCompletionSource? Reservation { get; }

        internal long Sequence { get; }

        internal long? Stamp { get; }

        internal bool Stamped { get; set; }
    }
}
