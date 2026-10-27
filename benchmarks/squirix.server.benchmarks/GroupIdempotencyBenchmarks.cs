using System;
using System.Globalization;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Cost of the group idempotency state for one committed client write on a table that already holds resolved records: the three calls a
/// leader makes (lookup, reserve, resolve) and the single call a follower makes when it applies the entry. Every call first sweeps the table
/// for expired records, so the cost is expected to grow with the number of records held.
/// </summary>
/// <remarks>
/// Resolved records cannot be released one by one, so each iteration rebuilds the state in <see cref="IterationSetup" /> and runs one batch of
/// new operations. The table grows by one batch (under one percent of the largest prefill) over the measured invocation.
/// </remarks>
[MemoryDiagnoser]
[Config(typeof(ContentionBenchmarkConfig))]
public class GroupIdempotencyBenchmarks
{
    private const string Scope = "bench-cache";
    private const int OperationsPerBatch = 512;
    private const int Capacity = 1_000_000;
    private const ulong Term = 1;
    private static readonly TimeSpan Retention = TimeSpan.FromDays(30);
    private static readonly byte[] Fingerprint = new byte[32];
    private static readonly byte[] Outcome = new byte[16];
    private string[] _batchIds = [];
    private ulong[] _batchIndexes = [];
    private GroupIdempotencyRecord[] _batchRecords = [];
    private GroupIdempotencyState _state = new(Capacity, Retention, TimeProvider.System);

    /// <summary>Gets or sets how many resolved, unexpired records the state already holds when the measured operations start.</summary>
    [Params(0, 60000)]
    public int Prefill { get; set; }

    /// <summary>Applies one committed entry the way a follower does: records its resolved outcome.</summary>
    [Benchmark(OperationsPerInvoke = OperationsPerBatch)]
    public void FollowerApply()
    {
        var state = _state;
        for (var i = 0; i < _batchRecords.Length; i++)
            state.RecordCommittedOutcome(in _batchRecords[i]);
    }

    /// <summary>Builds a fresh state holding the prefill and the operation ids and records of the batch.</summary>
    /// <exception cref="InvalidOperationException">Thrown when a prefill outcome is not restored.</exception>
    [IterationSetup]
    public void IterationSetup()
    {
        var now = DateTime.UtcNow;

        // The restore path does not sweep, so the prefill costs one pass instead of one sweep per record.
        _state = new GroupIdempotencyState(Capacity, Retention, TimeProvider.System);
        _state.BeginOutcomeRebuild();
        var logIndex = 0UL;
        for (var i = 0; i < Prefill; i++)
        {
            logIndex++;
            var record = CreateRecord(string.Create(CultureInfo.InvariantCulture, $"prefill-{i}"), logIndex, now);
            if (_state.RestoreOutcome(in record, TimeSpan.Zero) != GroupOutcomeRestoreResult.Restored)
                throw new InvalidOperationException("Prefill outcome was not restored.");
        }

        _state.MarkOutcomesRebuilt();

        _batchIds = new string[OperationsPerBatch];
        _batchRecords = new GroupIdempotencyRecord[OperationsPerBatch];
        _batchIndexes = new ulong[OperationsPerBatch];
        for (var i = 0; i < OperationsPerBatch; i++)
        {
            logIndex++;
            var id = string.Create(CultureInfo.InvariantCulture, $"batch-{i}");
            _batchIds[i] = id;
            _batchIndexes[i] = logIndex;
            _batchRecords[i] = CreateRecord(id, logIndex, now);
        }
    }

    /// <summary>Runs the calls a leader makes for one committed client write: lookup, reserve, resolve.</summary>
    /// <exception cref="InvalidOperationException">Thrown when a call does not return what a new operation gets.</exception>
    [Benchmark(Baseline = true, OperationsPerInvoke = OperationsPerBatch)]
    public void LeaderSequence()
    {
        var state = _state;
        for (var i = 0; i < _batchIds.Length; i++)
        {
            var id = _batchIds[i];
            var logIndex = _batchIndexes[i];
            if (state.Lookup(Scope, id, Fingerprint, out _) != GroupIdempotencyLookup.Miss)
                throw new InvalidOperationException("Operation was already known.");

            if (state.Reserve(Scope, id, Fingerprint, GroupRecordKind.UserMutation, logIndex, Term, new GroupReserveOptions(false, Outcome.Length)) != GroupIdempotencyReserveResult.Success)
                throw new InvalidOperationException("Reservation was not made.");

            if (!state.TryResolve(Scope, id, Outcome, logIndex, Term))
                throw new InvalidOperationException("Reservation was not resolved.");
        }
    }

    private static GroupIdempotencyRecord CreateRecord(string id, ulong logIndex, DateTime now) =>
        new(Scope, id, Fingerprint, Outcome, GroupRecordKind.UserMutation, now, now, logIndex, Term);
}
