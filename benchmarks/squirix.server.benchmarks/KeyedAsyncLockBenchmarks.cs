using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Benchmarks;

/// <summary>Measures keyed lock churn and table contention with synchronized dedicated workers and preallocated integer keys.</summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class KeyedAsyncLockBenchmarks
{
    private const int Operations = 65536;
    private readonly KeyedAsyncLock<int> _locks = new();

    /// <summary>Gets or sets the number of workers using separate keys in one table.</summary>
    [Params(1, 8, 24)]
    public int Workers { get; set; }

    /// <summary>Measures acquire and return pairs including worker startup; every worker advances through distinct keys.</summary>
    /// <returns>The completion of all workers.</returns>
    [Benchmark(OperationsPerInvoke = Operations)]
    public async Task DistinctKeysAsync()
    {
        using var ready = new CountdownEvent(Workers);
        using var start = new ManualResetEventSlim(false);
        var tasks = new Task[Workers];
        for (var i = 0; i < Workers; i++)
        {
            var count = (Operations / Workers) + (i < Operations % Workers ? 1 : 0);
            var run = new WorkerRun(_locks, i, Workers, count, ready, start);
            tasks[i] = Task.Factory.StartNew(
                               static state => Unsafe.As<WorkerRun>(state)!.ExecuteAsync(),
                               run,
                               CancellationToken.None,
                               TaskCreationOptions.LongRunning,
                               TaskScheduler.Default)
                           .Unwrap();
        }

        ready.Wait(CancellationToken.None);
        start.Set();
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    private sealed class WorkerRun
    {
        private readonly int _count;
        private readonly int _firstKey;
        private readonly KeyedAsyncLock<int> _locks;
        private readonly CountdownEvent _ready;
        private readonly ManualResetEventSlim _start;
        private readonly int _stride;

        internal WorkerRun(KeyedAsyncLock<int> locks, int firstKey, int stride, int count, CountdownEvent ready, ManualResetEventSlim start)
        {
            _locks = locks;
            _firstKey = firstKey;
            _stride = stride;
            _count = count;
            _ready = ready;
            _start = start;
        }

        internal async Task ExecuteAsync()
        {
            _ = _ready.Signal();
            _start.Wait(CancellationToken.None);
            var key = _firstKey;
            for (var i = 0; i < _count; i++, key += _stride)
            {
                using var lease = await _locks.LockAsync(key, CancellationToken.None).ConfigureAwait(false);
            }
        }
    }
}
