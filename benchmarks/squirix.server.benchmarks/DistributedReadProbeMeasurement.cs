using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Measures two retained-outcome probes around a live local entry read without changing the distributed algorithm.</summary>
internal static class DistributedReadProbeMeasurement
{
    private const int IdentityCount = 1024;

    /// <summary>Runs paired samples with persistent workers; the baseline is a synthetic lower bound, not a valid distributed read.</summary>
    /// <param name="args">Options: --workers, --operations, --samples and --prefill.</param>
    /// <exception cref="ArgumentOutOfRangeException">An operation count, sample count, worker count or prefill count is invalid.</exception>
    internal static void Run(string[] args)
    {
        var options = MeasurementArgs.Parse(args);
        var workers = options.GetInts("--workers", [1, 8, 32]);
        var operations = options.GetInt("--operations", 65536);
        var samples = options.GetInt("--samples", 5);
        var prefill = options.GetInt("--prefill", 60000);
        if (operations <= 0 || samples <= 0 || prefill < 0)
            throw new ArgumentOutOfRangeException(nameof(args), "Operations and samples must be positive; prefill must not be negative.");
        MeasurementOutput.Line($"Runtime: {RuntimeInformation.FrameworkDescription}; OS: {RuntimeInformation.OSDescription}; CPUs: {Environment.ProcessorCount}");
        MeasurementOutput.Line("No transport, payload response copies, background sweeper, concurrent mutations or replica read fence. IDs are preallocated and reused, but never retained.");
        MeasurementOutput.Line("| workers | state | sample | probes | operations | ns/op | CPU ns/op | B/op | ops/s |");
        MeasurementOutput.Line("|---:|---|---:|---|---:|---:|---:|---:|---:|");
        foreach (var count in workers)
        {
            if (count <= 0)
                throw new ArgumentOutOfRangeException(nameof(args), "Worker counts must be positive.");
            using var run = new WorkerGroup(count, operations);
            foreach (var state in new[] { "empty", "retained", "expired" })
            {
                run.Prepare(state, prefill);
                _ = run.Measure(false);
                run.Prepare(state, prefill);
                _ = run.Measure(true);

                // Alternate order and rebuild both fixtures outside the timing, including the expired burst every time.
                for (var sample = 0; sample < samples; sample++)
                    for (var pair = 0; pair < 2; pair++)
                    {
                        var probes = (sample + pair) % 2 == 0;
                        run.Prepare(state, prefill);
                        var (elapsed, cpu, bytes) = run.Measure(probes);
                        run.Verify(state, prefill, probes);
                        var total = long.CreateChecked(count) * operations;
                        MeasurementOutput.Line(string.Create(CultureInfo.InvariantCulture, $"| {count} | {state} | {sample + 1} | {probes} | {total} | {elapsed.TotalNanoseconds / total:F2} | {cpu.TotalNanoseconds / total:F2} | {double.CreateChecked(bytes) / total:F2} | {total / elapsed.TotalSeconds:F0} |"));
                    }
            }
        }
    }

    private sealed record WorkerStart(WorkerGroup Owner, int Worker);

    private sealed class WorkerGroup : IDisposable
    {
        private readonly Barrier _barrier;
        private readonly PhysicalCache<object?> _cache = new(TimeProvider.System, new CacheEntrySizeEstimator<object?>());
        private readonly string[][] _identities;
        private readonly CacheKey[] _keys;
        private readonly Meter _meter = new("distributed-read-probes");
        private readonly int _operations;
        private readonly Process _process = Process.GetCurrentProcess();
        private readonly Thread[] _threads;
        private readonly byte[] _value = new DistributedCachePolicy(null, null).Encode(new byte[1024]);
        private Exception? _failure;
        private bool _probes;
        private bool _stopping;
        private RpcMutationIdempotencyStore? _store;

        internal WorkerGroup(int workers, int operations)
        {
            _operations = operations;
            _barrier = new Barrier(workers + 1);
            _identities = new string[workers][];
            _keys = new CacheKey[workers];
            _threads = new Thread[workers];
            for (var worker = 0; worker < workers; worker++)
            {
                _keys[worker] = CacheKey.Default(worker.ToString(CultureInfo.InvariantCulture));
                _cache.Set(_keys[worker], new NodeCacheEntry<object?>(_value, expiresUtc: TimeProvider.System.GetUtcNow().AddDays(1).UtcDateTime), CancellationToken.None);
                var ids = new string[IdentityCount];
                for (var index = 0; index < ids.Length; index++)
                    ids[index] = string.Create(CultureInfo.InvariantCulture, $"read-{worker:D8}-{index:D8}-identity");
                _identities[worker] = ids;
                _threads[worker] = new Thread(
                    static state =>
                    {
                        var start = Unsafe.As<WorkerStart>(state)!;
                        start.Owner.Work(start.Worker);
                    })
                {
                    IsBackground = true,
                };
                _threads[worker].Start(new WorkerStart(this, worker));
            }
        }

        public void Dispose()
        {
            _stopping = true;
            _barrier.SignalAndWait(CancellationToken.None);
            foreach (var thread in _threads)
                thread.Join();
            _barrier.Dispose();
            _process.Dispose();
            _meter.Dispose();
        }

        internal void Prepare(string state, int prefill)
        {
            _store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "probe-node", new IdempotencyMetrics(_meter), TimeProvider.System);
            if (string.Equals(state, "empty", StringComparison.Ordinal))
                return;
            var created = TimeProvider.System.GetUtcNow().UtcDateTime;
            if (string.Equals(state, "expired", StringComparison.Ordinal))
                created -= TimeSpan.FromHours(1);
            var records = new List<PersistedIdempotencyRecord?>(prefill);
            for (var index = 0; index < prefill; index++)
                records.Add(new PersistedIdempotencyRecord(string.Create(CultureInfo.InvariantCulture, $"retained-{index:D8}"), "fingerprint", new byte[1], created));
            _store.RestoreSnapshotRecords(records);
        }

        internal (TimeSpan Elapsed, TimeSpan Cpu, long Bytes) Measure(bool probes)
        {
            _probes = probes;
#pragma warning disable S1215 // Remove setup garbage before the timed batch so fixture construction does not decide which sample collects it.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
#pragma warning restore S1215
            var bytes = GC.GetTotalAllocatedBytes(true);
            var cpu = _process.TotalProcessorTime;
            var start = Stopwatch.GetTimestamp();
            _barrier.SignalAndWait(CancellationToken.None);
            _barrier.SignalAndWait(CancellationToken.None);
            var elapsed = Stopwatch.GetElapsedTime(start);
            var usedCpu = _process.TotalProcessorTime - cpu;
            var allocated = GC.GetTotalAllocatedBytes(true) - bytes;
            return _failure is { } failure
                ? ThrowHelper.Throw<(TimeSpan, TimeSpan, long)>(new InvalidOperationException("Read probe worker failed.", failure))
                : (elapsed, usedCpu, allocated);
        }

        internal void Verify(string state, int prefill, bool probes)
        {
            var expected = string.Equals(state, "empty", StringComparison.Ordinal) || (probes && string.Equals(state, "expired", StringComparison.Ordinal)) ? 0 : prefill;
            if (_store?.RecordCount != expected)
                throw new InvalidOperationException("The measured outcome fixture has an unexpected record count.");
        }

        private void Work(int worker)
        {
            while (true)
            {
                _barrier.SignalAndWait(CancellationToken.None);
                if (_stopping)
                    return;
                try
                {
                    Read(worker);
                }
#pragma warning disable CA1031 // Hand the worker failure to the caller after the completion barrier, rather than stranding its wait.
                catch (Exception error)
                {
                    Volatile.Write(ref _failure, error);
                }
#pragma warning restore CA1031
                _barrier.SignalAndWait(CancellationToken.None);
            }
        }

        private void Read(int worker)
        {
            var store = ThrowHelper.Required(_store, "Prepare must run first.");
            var ids = _identities[worker];
            var key = _keys[worker];
            for (var index = 0; index < _operations; index++)
            {
                var id = ids[index % IdentityCount];
                if (_probes && store.HasRetainedOperation(id))
                    throw new InvalidOperationException("The read identity must not be retained.");
                var entry = _cache.GetEntry(key, CancellationToken.None);
                if (_probes && store.HasRetainedOperation(id))
                    throw new InvalidOperationException("The read identity must not be retained.");
                if (entry == null || !ReferenceEquals(entry.Value, _value))
                    throw new InvalidOperationException("The seeded entry must stay live and unchanged.");
            }
        }
    }
}
