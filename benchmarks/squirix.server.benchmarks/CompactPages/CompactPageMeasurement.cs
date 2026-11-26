using System;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Threading;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;

namespace Squirix.Server.Benchmarks;

internal static class CompactPageMeasurement
{
    private const int Keys = 2_000_000;
    private const int Removed = Keys * 9 / 10;

    internal static void Run(bool compact, bool binary)
    {
        var order = BuildOrder();
        Validate(compact, binary);
        var empty = Live();
        var initialRss = Environment.WorkingSet;
        var store = new Subject(compact, binary);
        long peakLive = 0;
        for (var i = 0; i < Keys; i++)
        {
            Insert(store, i);

            // Sample before, during and after the last index doubling as well as at regular intervals.
            if ((i + 1) % 125000 == 0 || i + 1 is 1560000 or 1572864 or 1585000 or 1600000)
                peakLive = Math.Max(peakLive, Live() - empty);
        }

        using (var process = Process.GetCurrentProcess())
            Print("growth", $"peakLiveSample={peakLive} peakProcessRss={process.PeakWorkingSet64} initialRss={initialRss}");

        Report("filled", empty, Keys);
        for (var i = 0; i < Removed; i++)
        {
            if (!store.Remove(Key(order[i])))
                throw new InvalidOperationException("Missing deletion target.");
        }

        Report("removed", empty, Keys - Removed);
        for (var i = 0; i < order.Length; i++)
            Verify(store, Key(order[i]), order[i], i >= Removed);

        for (var i = 0; i < Removed; i++)
            Insert(store, Keys + i);
        Report("refilled", empty, Keys);
        for (var i = 0; i < Removed; i++)
            Verify(store, Key(Keys + i), Keys + i, true);

        // Independent lookup strings; generating and formatting inputs is outside the timed region.
        var probes = new string[Keys];
        for (var i = 0; i < Keys; i++)
            probes[i] = Key(i < Removed ? Keys + i : order[i]);
        foreach (var workers in new[] { 1, 8 })
        {
            var pass = new ReadMeasurement(store, probes, order, workers);
            pass.MeasureReads();
        }

        GC.KeepAlive(order);
        GC.KeepAlive(store);
    }

    internal static void MeasureOverwrites(bool binary)
    {
        var order = BuildOrder();
        var empty = Live();
        var store = new Subject(false, binary);
        for (var i = 0; i < Keys; i++)
            Insert(store, i);
        Report("filled", empty, Keys);

        for (var pass = 1; pass <= 2; pass++)
        {
            var start = Stopwatch.GetTimestamp();
            var allocated = GC.GetAllocatedBytesForCurrentThread();
            foreach (var id in order)
            {
                var value = Key(id ^ pass);
                store.Set(Key(id), binary ? StoredValue.FromBinary(Encoding.ASCII.GetBytes(value)) : StoredValue.FromText(value));
            }

            allocated = GC.GetAllocatedBytesForCurrentThread() - allocated;
            Print("overwrite", $"pass={pass} ops={Keys} seconds={Stopwatch.GetElapsedTime(start).TotalSeconds:F6} allocatedPerSet={double.CreateChecked(allocated) / Keys:F2}");
            Report("updated", empty, Keys);
            foreach (var id in order)
                Verify(store, Key(id), id ^ pass, true);
        }

        GC.KeepAlive(order);
        GC.KeepAlive(store);
    }

    private static int[] BuildOrder()
    {
        var order = new int[Keys];
        for (var i = 0; i < Keys; i++)
            order[i] = i;

        // Deterministic Fisher-Yates workload, independent of each process's hash seed.
#pragma warning disable S2245, CA5394
        new Random(12194).Shuffle(order);
#pragma warning restore S2245, CA5394
        return order;
    }

    private static string Key(int id) => id.ToString("X8", CultureInfo.InvariantCulture);

    private static void Insert(Subject store, int id)
    {
        var key = Key(id);
        store.Set(key, store.Kind == StoredValueKind.Binary ? StoredValue.FromBinary(Encoding.ASCII.GetBytes(key)) : StoredValue.FromText(key));
    }

#pragma warning disable S1215 // Measurement checkpoints must exclude temporary caller objects.
    private static long Live() => GC.GetTotalMemory(true);
#pragma warning restore S1215

    private static void Report(string phase, long empty, int count)
    {
        var live = Live() - empty;
        Print(phase, $"entries={count} live={live} bytesPerEntry={double.CreateChecked(live) / count:F3}");
    }

    private static void Print(string phase, FormattableString values) =>
        Console.WriteLine(phase + " " + values.ToString(CultureInfo.InvariantCulture));

    private static void Verify(Subject store, string key, int expected, bool present)
    {
        var result = store.Get(key);
        if (result.Found != present)
            throw new InvalidOperationException("Unexpected key presence.");
        if (!present)
            return;
        if (result.Value.Kind != store.Kind || result.Value.Bytes.Length != 8)
            throw new InvalidOperationException("Unexpected value kind or length.");
        var bytes = result.Value.Bytes.AsSpan();
        for (var i = 7; i >= 0; i--)
        {
            if (bytes[i] != "0123456789ABCDEF"[expected & 15])
                throw new InvalidOperationException("Read a different key's value.");
            expected >>= 4;
        }
    }

    private static void Validate(bool compact, bool binary)
    {
        var store = new Subject(compact, binary);
        CheckSizesAndOwnership(store);
        Race(store);
    }

    private static void CheckSizesAndOwnership(Subject store)
    {
        var names = new[] { "ключ", "é", "e\u0301", "🦊", new string('k', 300) };
        foreach (var name in names)
            store.Set(name, StoredValue.FromText(name));
        foreach (var name in names)
        {
            if (!store.Get(name).Value.Bytes.AsSpan().SequenceEqual(Encoding.UTF8.GetBytes(name)))
                throw new InvalidOperationException("Unicode mismatch.");
        }

        // Keep the result across moves, growth, replacement and removal.
        var held = store.Get(names[0]).Value;
        foreach (var length in new[] { 0, 1, 8, 17, 96, 257, 1025, 33_000, 23, 0 })
        {
            var bytes = new byte[length];
            Array.Fill(bytes, byte.CreateTruncating(length));
            store.Set(names[0], StoredValue.FromBinary(bytes));
            var read = store.Get(names[0]).Value;
            if (read.Kind != StoredValueKind.Binary || !read.Bytes.AsSpan().SequenceEqual(bytes))
                throw new InvalidOperationException("Replacement mismatch.");
        }

        foreach (var name in names)
        {
            if (!store.Remove(name) || store.Remove(name))
                throw new InvalidOperationException("Removal mismatch.");
        }

        if (!held.Bytes.AsSpan().SequenceEqual(Encoding.UTF8.GetBytes(names[0])))
            throw new InvalidOperationException("Previously returned bytes changed.");
    }

    private static void Race(Subject store)
    {
        using var gate = new Barrier(5);
        var counts = new int[5];
        var threads = new Thread[counts.Length];
        for (var worker = 0; worker < threads.Length; worker++)
        {
            threads[worker] = CreateRaceThread(store, gate, worker, counts);
            threads[worker].Start();
        }

        foreach (var thread in threads)
            thread.Join();
        var reads = 0;
        foreach (var count in counts)
            reads += count;
        if (reads == 0)
            throw new InvalidOperationException("No successful contended reads.");
        Print("race", $"verifiedHits={reads}");
    }

    private static Thread CreateRaceThread(Subject store, Barrier gate, int worker, int[] counts) => new(() => counts[worker] = RaceWorker(store, gate, worker));

    private static int RaceWorker(Subject store, Barrier gate, int worker)
    {
        gate.SignalAndWait(CancellationToken.None);
        var hits = 0;
        for (var i = 0; i < 120000; i++)
        {
            var id = (i + worker) % 97;
            var key = Key(id);
            if (worker == 0)
            {
                store.Remove(key);
                Insert(store, id);
                continue;
            }

            var result = store.Get(key);
            if (!result.Found)
                continue;
            if (result.Value.Kind != store.Kind || !result.Value.Bytes.AsSpan().SequenceEqual(Encoding.ASCII.GetBytes(key)))
                throw new InvalidOperationException("Reader crossed a slot lifetime.");
            hits++;
        }

        return hits;
    }

    private sealed class ReadMeasurement
    {
        private readonly Subject _store;
        private readonly string[] _probes;
        private readonly int[] _order;
        private readonly int _workers;
        private readonly long[] _starts;
        private readonly long[] _ends;
        private readonly long[] _allocated;

        internal ReadMeasurement(Subject store, string[] probes, int[] order, int workers)
        {
            _store = store;
            _probes = probes;
            _order = order;
            _workers = workers;
            _starts = new long[workers];
            _ends = new long[workers];
            _allocated = new long[workers];
        }

        internal void MeasureReads()
        {
            using var gate = new Barrier(_workers);
            var threads = new Thread[_workers];
            for (var i = 0; i < threads.Length; i++)
            {
                threads[i] = NewReader(i, gate);
                threads[i].Start();
            }

            foreach (var thread in threads)
                thread.Join();
            var start = long.MaxValue;
            long end = 0;
            long allocated = 0;
            for (var i = 0; i < _workers; i++)
            {
                start = Math.Min(start, _starts[i]);
                end = Math.Max(end, _ends[i]);
                allocated += _allocated[i];
            }

            const int operations = 3 * Keys;
            Print("read", $"workers={_workers} ops={operations} seconds={Stopwatch.GetElapsedTime(start, end).TotalSeconds:F6} allocatedPerGet={double.CreateChecked(allocated) / operations:F2}");
        }

        private Thread NewReader(int worker, Barrier gate) => new(() => RunReader(worker, gate));

        private void RunReader(int worker, Barrier gate)
        {
            ReadPartition(worker, 131072);
            gate.SignalAndWait(CancellationToken.None);
            var before = GC.GetAllocatedBytesForCurrentThread();
            _starts[worker] = Stopwatch.GetTimestamp();
            for (var pass = 0; pass < 3; pass++)
                ReadPartition(worker, Keys);
            _ends[worker] = Stopwatch.GetTimestamp();
            _allocated[worker] = GC.GetAllocatedBytesForCurrentThread() - before;
        }

        private void ReadPartition(int worker, int limit)
        {
            for (var i = worker; i < limit; i += _workers)
            {
                var pos = _order[i];
                Verify(_store, _probes[pos], pos < Removed ? Keys + pos : _order[pos], true);
            }
        }
    }

    private sealed class Subject
    {
        private readonly PhysicalCache<StoredValue>? _current;
        private readonly CompactPageCache? _compact;

        internal Subject(bool compact, bool binary)
        {
            Kind = binary ? StoredValueKind.Binary : StoredValueKind.Text;
            if (compact)
                _compact = new CompactPageCache();
            else
                _current = new PhysicalCache<StoredValue>(TimeProvider.System, new CacheEntrySizeEstimator<StoredValue>());
        }

        internal StoredValueKind Kind { get; }

        internal void Set(string key, StoredValue value)
        {
            if (_compact != null)
                _compact.Access(key, CompactPageCache.Operation.Set, ref value);
            else
                _current!.Set(CacheKey.Default(key), new NodeCacheEntry<StoredValue>(value), CancellationToken.None);
        }

        internal bool Remove(string key)
        {
            StoredValue unused = default;
            return _compact != null ? _compact.Access(key, CompactPageCache.Operation.Remove, ref unused) : _current!.Remove(CacheKey.Default(key), CancellationToken.None);
        }

        internal NodeCacheValueResult<StoredValue> Get(string key)
        {
            if (_compact == null)
                return _current!.GetValue(CacheKey.Default(key), CancellationToken.None);
            StoredValue value = default;
            var found = _compact.Access(key, CompactPageCache.Operation.Get, ref value);
            return new NodeCacheValueResult<StoredValue>(found, value);
        }
    }
}
