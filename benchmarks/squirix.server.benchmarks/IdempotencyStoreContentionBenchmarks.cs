using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Scaling of <see cref="RpcMutationIdempotencyStore" /> with concurrent workers that use distinct operation ids. A mutation runs the call
/// sequence of a durable idempotent mutation (replay probe, reserve, stamp mark, held outcome, record, complete execution); a replay
/// looks up outcomes that are already recorded. The same worker harness over a <see cref="ConcurrentDictionary{TKey, TValue}" /> shows the
/// ceiling without the store, so the cost of its single gate can be told from the cost of the harness. Each invocation runs the operations
/// of every worker, so the mean per operation is the wall time of one worker's operation: flat across worker counts means perfect scaling.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(IdempotencyStoreContentionConfig))]
public class IdempotencyStoreContentionBenchmarks
{
    private const string Fingerprint = "fingerprint";
    private const int OperationsPerWorker = 512;
    private static readonly byte[] ResponseBytes = new Int32Value { Value = 42 }.ToByteArray();
    private readonly Meter _meter = new("idempotency-store-contention-bench");
    private ConcurrentDictionary<string, object> _baselineExecutions = new(StringComparer.Ordinal);
    private ConcurrentDictionary<string, object> _baselineRecords = new(StringComparer.Ordinal);
    private string[][] _ids = [];
    private string[][] _mutationIds = [];
    private RpcMutationIdempotencyStore? _store;

    private enum WorkerMode
    {
        BaselineMutation = 0,
        BaselineReplay = 1,
        StoreMutation = 2,
        StoreReplay = 3,
    }

    /// <summary>Gets the worker counts: 1, 4, the processor count and 16 when the machine has fewer processors.</summary>
    public static IEnumerable<int> WorkerCounts
    {
        get
        {
            var counts = new List<int> { 1, 4 };
            if (Environment.ProcessorCount > 4)
                counts.Add(Environment.ProcessorCount);
            if (Environment.ProcessorCount < 16)
                counts.Add(16);
            return counts;
        }
    }

    /// <summary>Gets or sets how many completed records the store already holds when the measured operations start.</summary>
    [Params(0, 60000)]
    public int Prefill { get; set; }

    /// <summary>Gets or sets the number of concurrent workers.</summary>
    [ParamsSource(nameof(WorkerCounts))]
    public int Workers { get; set; }

    /// <summary>Runs the mutation sequence on a concurrent dictionary in place of the store.</summary>
    /// <returns>Operations completed, as a sink.</returns>
    [Benchmark(Baseline = true, OperationsPerInvoke = OperationsPerWorker)]
    public int BaselineMutation() => RunWorkers(WorkerMode.BaselineMutation);

    /// <summary>Runs the replay lookup on a concurrent dictionary in place of the store.</summary>
    /// <returns>Operations completed, as a sink.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerWorker)]
    public int BaselineReplay() => RunWorkers(WorkerMode.BaselineReplay);

    /// <summary>Releases the meter.</summary>
    [GlobalCleanup]
    public void Cleanup() => _meter.Dispose();

    /// <summary>Creates the store, the baseline dictionaries and the per-worker operation ids; records everything a replay reads.</summary>
    [IterationSetup]
    public void IterationSetup()
    {
        _ids = new string[Workers][];
        _mutationIds = new string[Workers][];
        for (var worker = 0; worker < Workers; worker++)
        {
            var ids = new string[OperationsPerWorker];
            var mutationIds = new string[OperationsPerWorker];
            for (var i = 0; i < ids.Length; i++)
            {
                ids[i] = string.Create(CultureInfo.InvariantCulture, $"op-{worker}-{i}");
                mutationIds[i] = string.Create(CultureInfo.InvariantCulture, $"mutation-{worker}-{i}");
            }

            _ids[worker] = ids;
            _mutationIds[worker] = mutationIds;
        }

        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions { MaxInFlightRecords = 1_000_000 }, "bench-node", new IdempotencyMetrics(_meter));
        _store = store;
        _baselineRecords = new ConcurrentDictionary<string, object>(StringComparer.Ordinal);
        _baselineExecutions = new ConcurrentDictionary<string, object>(StringComparer.Ordinal);

        var createdUtc = DateTime.UtcNow;
        var restored = new List<PersistedIdempotencyRecord?>(Prefill + (Workers * OperationsPerWorker));
        for (var i = 0; i < Prefill; i++)
        {
            var id = string.Create(CultureInfo.InvariantCulture, $"prefill-{i}");
            restored.Add(new PersistedIdempotencyRecord(id, Fingerprint, ResponseBytes, createdUtc));
            _baselineRecords[id] = id;
        }

        // Replay reads the ids of every worker; the mutations use their own distinct ids.
        foreach (var ids in _ids)
            foreach (var id in ids)
            {
                restored.Add(new PersistedIdempotencyRecord(id, Fingerprint, ResponseBytes, createdUtc));
                _baselineRecords[id] = id;
            }

        store.RestoreSnapshotRecords(restored);
    }

    /// <summary>Runs the mutation sequence on the store.</summary>
    /// <returns>Operations completed, as a sink.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerWorker)]
    public int StoreMutation() => RunWorkers(WorkerMode.StoreMutation);

    /// <summary>Replays outcomes already recorded in the store.</summary>
    /// <returns>Operations completed, as a sink.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerWorker)]
    public int StoreReplay() => RunWorkers(WorkerMode.StoreReplay);

    private int BaselineMutationWorker(int worker)
    {
        var records = _baselineRecords;
        var executions = _baselineExecutions;
        var done = 0;
        foreach (var id in _mutationIds[worker])
        {
            // The same steps as the store sequence, each against a shared map: probe, reserve, mark, hold, record, release the execution.
            if (records.ContainsKey(id))
                done++;
            var execution = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            records[id] = execution;
            executions[id] = execution;
            _ = records.TryUpdate(id, id, execution);
            _ = records.TryUpdate(id, Fingerprint, id);
            _ = records.TryUpdate(id, ResponseBytes, Fingerprint);
            _ = executions.TryRemove(id, out _);
            _ = execution.TrySetResult();
        }

        return done;
    }

    private int BaselineReplayWorker(int worker)
    {
        var records = _baselineRecords;
        var done = 0;
        foreach (var id in _ids[worker])
        {
            if (records.ContainsKey(id))
                done++;
        }

        return done;
    }

    private int RunWorkers(WorkerMode mode)
    {
        var workers = Workers;
        var total = 0;
        using var ready = new CountdownEvent(workers);
        using var start = new ManualResetEventSlim(false);
        var threads = new Thread[workers];
        var runs = new WorkerRun[workers];
        for (var w = 0; w < workers; w++)
        {
            var run = new WorkerRun(this, mode, w, ready, start);
            runs[w] = run;
            threads[w] = new Thread(static state => Unsafe.As<WorkerRun>(state)!.Execute()) { IsBackground = true, Name = string.Create(CultureInfo.InvariantCulture, $"idempotency-bench-{w}") };
            threads[w].Start(run);
        }

        ready.Wait(CancellationToken.None);
        start.Set();
        foreach (var thread in threads)
            thread.Join();

        foreach (var run in runs)
            total += run.Done;

        return total;
    }

    private int StoreMutationWorker(int worker)
    {
        var store = ThrowHelper.Required(_store, "Benchmark store was not initialized.");
        var done = 0;
        foreach (var operationId in _mutationIds[worker])
        {
            _ = store.TryReplay(operationId, Fingerprint, Int32Value.Parser, out _);
            var execution = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            if (store.ReserveIntent(operationId, Fingerprint, execution, out _) != IdempotencyReserveResult.Acquired)
                throw new InvalidOperationException("Reservation was not acquired.");

            store.MarkStamped(operationId, execution);
            store.HoldAppendedOutcome(operationId, Fingerprint, ResponseBytes, execution);
            store.RecordSuccess(operationId, Fingerprint, ResponseBytes, execution);
            store.CompleteExecution(operationId, execution);
            done++;
        }

        return done;
    }

    private int StoreReplayWorker(int worker)
    {
        var store = ThrowHelper.Required(_store, "Benchmark store was not initialized.");
        var done = 0;
        foreach (var id in _ids[worker])
        {
            if (store.TryReplay(id, Fingerprint, Int32Value.Parser, out _))
                done++;
        }

        return done;
    }

    private sealed class WorkerRun
    {
        private readonly WorkerMode _mode;
        private readonly IdempotencyStoreContentionBenchmarks _owner;
        private readonly CountdownEvent _ready;
        private readonly ManualResetEventSlim _start;
        private readonly int _worker;

        internal WorkerRun(IdempotencyStoreContentionBenchmarks owner, WorkerMode mode, int worker, CountdownEvent ready, ManualResetEventSlim start)
        {
            _owner = owner;
            _mode = mode;
            _worker = worker;
            _ready = ready;
            _start = start;
        }

        internal int Done { get; private set; }

        internal void Execute()
        {
            _ = _ready.Signal();
            _start.Wait(CancellationToken.None);
            Done = _mode switch
            {
                WorkerMode.BaselineMutation => _owner.BaselineMutationWorker(_worker),
                WorkerMode.BaselineReplay => _owner.BaselineReplayWorker(_worker),
                WorkerMode.StoreMutation => _owner.StoreMutationWorker(_worker),
                WorkerMode.StoreReplay => _owner.StoreReplayWorker(_worker),
                _ => throw new InvalidOperationException("Unsupported worker mode."),
            };
        }
    }
}
