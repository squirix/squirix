using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>The group idempotency store expires exactly the records a full scan of the table would.</summary>
public sealed class GroupIdempotencyModelTests : ServerUnitTestBase
{
    private const string Scope = "client";

    private const int Capacity = 8;

    private const int Keys = 12;

    private const int Steps = 500;

    private static readonly TimeSpan Retention = TimeSpan.FromSeconds(600);

    private static readonly DateTimeOffset Epoch = new(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>A seeded sequence of operations leaves the store and a scanning reference model in the same observable state at every step.</summary>
    /// <param name="seed">The seed of the operation sequence.</param>
    [Test]
    [Arguments(1UL)]
    [Arguments(2UL)]
    [Arguments(3UL)]
    [Arguments(4UL)]
    [Arguments(5UL)]
    [Arguments(6UL)]
    [Arguments(7UL)]
    [Arguments(8UL)]
    public async Task StoreMatchesScanningModel(ulong seed)
    {
        var run = new Run(seed);
        var divergence = string.Empty;
        for (var step = 0; step < Steps && divergence.Length == 0; step++)
        {
            divergence = run.Step(step);
            if (divergence.Length == 0)
                divergence = run.Compare(step);
        }

        _ = await Assert.That(divergence).IsEqualTo(string.Empty);
    }

    private sealed record Entry(string Fingerprint, ulong Index, ulong Term, bool IsResolved, string Payload, TimeSpan Birth);

    private sealed class Splitmix
    {
        private ulong _state;

        internal Splitmix(ulong seed)
        {
            _state = seed;
        }

        internal int Next(int bound)
        {
            _state = unchecked(_state + 0x9E3779B97F4A7C15UL);
            var z = _state;
            z = unchecked((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL);
            z = unchecked((z ^ (z >> 27)) * 0x94D049BB133111EBUL);
            z ^= z >> 31;
            return int.CreateChecked(z % ulong.CreateChecked(bound));
        }
    }

    private sealed class Run
    {
        private readonly FakeTimeProvider _clock = new(Epoch);
        private readonly Model _model;
        private readonly Splitmix _random;
        private readonly GroupIdempotencyState _state;

        internal Run(ulong seed)
        {
            _random = new Splitmix(seed);
            _state = new GroupIdempotencyState(Capacity, Retention, _clock);
            _state.MarkOutcomesRebuilt();
            _model = new Model(_clock);
        }

        internal string Step(int step)
        {
            var id = $"k{_random.Next(Keys)}";
            var index = Convert.ToUInt64(_random.Next(40) + 1);
            return _random.Next(9) switch
            {
                0 or 1 => Reserve(id, index),
                2 or 3 => Resolve(id, index, $"p{step}"),
                4 or 5 => Commit(id, index, $"c{step}"),
                6 => _state.ReleaseFromIndex(index) == _model.Release(index) ? string.Empty : "release",
                7 => _model.Advance(TimeSpan.FromSeconds(_random.Next(301))),
                _ => _random.Next(3) == 0 ? Restore() : Sweep(),
            };
        }

        internal string Compare(int step)
        {
            for (var i = 0; i < Keys; i++)
            {
                var id = $"k{i}";
                var actual = _state.Lookup(Scope, id, Encoding.UTF8.GetBytes(id), out var record);
                var (expected, expectedPayload) = _model.Lookup(id);
                var payload = actual == GroupIdempotencyLookup.Found ? Encoding.UTF8.GetString(record.OutcomePayload.Span) : string.Empty;
                if (actual != expected || !string.Equals(payload, expectedPayload, StringComparison.Ordinal))
                    return $"step {step}: lookup of {id} gave {actual}/{payload}, expected {expected}/{expectedPayload}";
            }

            var exported = new List<string>();
            foreach (var record in _state.ExportResolved(out _))
                exported.Add($"{record.OperationId}|{record.LogIndex}|{Encoding.UTF8.GetString(record.OutcomePayload.Span)}");

            exported.Sort(StringComparer.Ordinal);
            var exportedSet = string.Join(';', exported);
            var expectedSet = _model.ResolvedSet();
            return string.Equals(exportedSet, expectedSet, StringComparison.Ordinal) ? string.Empty : $"step {step}: exported {exportedSet}, expected {expectedSet}";
        }

        private string Reserve(string id, ulong index)
        {
            var fingerprint = _random.Next(6) == 0 ? id + "x" : id;
            var reserved = _state.Reserve(Scope, id, Encoding.UTF8.GetBytes(fingerprint), GroupRecordKind.UserMutation, index, 1UL);
            return reserved == _model.Reserve(id, fingerprint, index) ? string.Empty : "reserve";
        }

        private string Resolve(string id, ulong index, string payload)
        {
            var known = _model.Find(id);
            var (resolveIndex, resolveTerm) = known != null && _random.Next(4) != 0 ? (known.Index, known.Term) : (index, 1UL);
            var resolved = _state.TryResolve(Scope, id, Encoding.UTF8.GetBytes(payload), resolveIndex, resolveTerm);
            return resolved == _model.Resolve(id, payload, resolveIndex, resolveTerm) ? string.Empty : "resolve";
        }

        private string Commit(string id, ulong index, string payload)
        {
            var decided = TimeSpan.FromSeconds(_random.Next(841) - 60);
            var resolvedUtc = _clock.GetUtcNow().UtcDateTime - decided;
            var record = new GroupIdempotencyRecord(
                Scope,
                id,
                Encoding.UTF8.GetBytes(id),
                Encoding.UTF8.GetBytes(payload),
                GroupRecordKind.UserMutation,
                resolvedUtc,
                resolvedUtc,
                index,
                1UL);
            _state.RecordCommittedOutcome(in record);
            _model.Commit(id, payload, index, decided);
            return string.Empty;
        }

        private string Sweep()
        {
            _state.Expire();
            _model.Sweep();
            return string.Empty;
        }

        private string Restore()
        {
            var captured = _clock.GetUtcNow().UtcDateTime;
            var snapshot = _model.Snapshot(_random, captured);
            var retained = new List<ulong>();
            for (var index = 1UL; index <= 40UL; index++)
            {
                if (_random.Next(4) == 0)
                    retained.Add(index);
            }

            _state.RestoreFromSnapshot(snapshot, captured, retained);
            _model.Restore(snapshot, captured, retained);
            return string.Empty;
        }
    }

    private sealed class Model
    {
        private readonly FakeTimeProvider _clock;
        private readonly Dictionary<string, Entry> _table;
        private TimeSpan _elapsed;

        internal Model(FakeTimeProvider clock)
        {
            _clock = clock;
            _table = [with(StringComparer.Ordinal)];
        }

        internal string Advance(TimeSpan by)
        {
            _clock.Advance(by);
            _elapsed += by;
            return string.Empty;
        }

        internal Entry? Find(string id) => _table.GetValueOrDefault(id);

        internal void Sweep()
        {
            var expired = new List<string>();
            foreach (var (id, entry) in _table)
            {
                if (entry.IsResolved && _elapsed - entry.Birth >= Retention)
                    expired.Add(id);
            }

            foreach (var id in CollectionsMarshal.AsSpan(expired))
                _ = _table.Remove(id);
        }

        internal GroupIdempotencyReserveResult Reserve(string id, string fingerprint, ulong index)
        {
            Sweep();
            if (_table.TryGetValue(id, out var existing))
            {
                if (!string.Equals(existing.Fingerprint, fingerprint, StringComparison.Ordinal))
                    return GroupIdempotencyReserveResult.FingerprintMismatch;

                if (!existing.IsResolved && (existing.Index != index || existing.Term != 1UL))
                    _table[id] = existing with { Index = index, Term = 1UL };

                return GroupIdempotencyReserveResult.Success;
            }

            if (_table.Count >= Capacity)
                return GroupIdempotencyReserveResult.CapacityExceeded;

            _table[id] = new Entry(fingerprint, index, 1UL, false, string.Empty, TimeSpan.Zero);
            return GroupIdempotencyReserveResult.Success;
        }

        internal bool Resolve(string id, string payload, ulong index, ulong term)
        {
            Sweep();
            if (!_table.TryGetValue(id, out var existing) || existing.Index != index || existing.Term != term || existing.IsResolved)
                return false;

            _table[id] = existing with { IsResolved = true, Payload = payload, Birth = _elapsed };
            return true;
        }

        internal void Commit(string id, string payload, ulong index, TimeSpan decided)
        {
            Sweep();
            if (_table.TryGetValue(id, out var existing) && existing.IsResolved && existing.Index >= index)
                return;

            var age = decided > TimeSpan.Zero ? decided : TimeSpan.Zero;
            _table[id] = new Entry(id, index, 1UL, true, payload, _elapsed - age);
        }

        internal int Release(ulong fromIndex)
        {
            var released = new List<string>();
            foreach (var (id, entry) in _table)
            {
                if (entry.Index >= fromIndex)
                    released.Add(id);
            }

            foreach (var id in CollectionsMarshal.AsSpan(released))
                _ = _table.Remove(id);

            return released.Count;
        }

        internal (GroupIdempotencyLookup Result, string Payload) Lookup(string id)
        {
            Sweep();
            if (!_table.TryGetValue(id, out var entry))
                return (GroupIdempotencyLookup.Miss, string.Empty);

            if (!string.Equals(entry.Fingerprint, id, StringComparison.Ordinal))
                return (GroupIdempotencyLookup.Mismatch, string.Empty);

            return entry.IsResolved ? (GroupIdempotencyLookup.Found, entry.Payload) : (GroupIdempotencyLookup.Unresolved, string.Empty);
        }

        internal string ResolvedSet()
        {
            var lines = new List<string>();
            foreach (var (id, entry) in _table)
            {
                if (entry.IsResolved)
                    lines.Add($"{id}|{entry.Index}|{entry.Payload}");
            }

            lines.Sort(StringComparer.Ordinal);
            return string.Join(';', lines);
        }

        internal List<GroupIdempotencyRecord> Snapshot(Splitmix random, DateTime capturedUtc)
        {
            Sweep();
            var records = new List<GroupIdempotencyRecord>();
            foreach (var (id, entry) in _table)
            {
                if (!entry.IsResolved || random.Next(3) == 0)
                    continue;

                var resolvedUtc = capturedUtc - (_elapsed - entry.Birth);
                records.Add(
                    new GroupIdempotencyRecord(
                        Scope,
                        id,
                        Encoding.UTF8.GetBytes(entry.Fingerprint),
                        Encoding.UTF8.GetBytes(entry.Payload),
                        GroupRecordKind.UserMutation,
                        resolvedUtc,
                        resolvedUtc,
                        entry.Index,
                        entry.Term));
            }

            return records;
        }

        internal void Restore(List<GroupIdempotencyRecord> snapshot, DateTime capturedUtc, List<ulong> retained)
        {
            Sweep();
            var previous = new Dictionary<string, Entry>(_table, StringComparer.Ordinal);
            _table.Clear();
            foreach (var record in CollectionsMarshal.AsSpan(snapshot))
            {
                var age = capturedUtc - record.ResolvedUtc!.Value;
                if (age >= Retention)
                    continue;

                var payload = Encoding.UTF8.GetString(record.OutcomePayload.Span);
                _table[record.OperationId] = new Entry(Encoding.UTF8.GetString(record.OperationFingerprint.Span), record.LogIndex, record.Term, true, payload, _elapsed - age);
            }

            foreach (var (id, entry) in previous)
            {
                if (retained.Contains(entry.Index))
                    _table[id] = entry;
            }
        }
    }
}
