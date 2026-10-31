using System;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Measures shared semaphore and admission-gate pairs from synchronized dedicated workers, including worker startup.</summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class AdmissionSemaphoreContentionBenchmarks
{
    private const int Operations = 262144;
    private AdmissionGate? _gate;
    private Meter? _meter;
    private AsyncSemaphore? _semaphore;

    private enum Operation
    {
        TryAcquire = 0,
        Wait = 1,
        Admission = 2,
    }

    /// <summary>Gets or sets the number of dedicated workers sharing one semaphore or gate.</summary>
    [Params(1, 8, 24)]
    public int Workers { get; set; }

    /// <summary>Measures admission and lease release without queueing.</summary>
    /// <returns>The completion of every worker.</returns>
    [Benchmark(OperationsPerInvoke = Operations)]
    public Task AdmissionAsync() => RunAsync(Operation.Admission);

    /// <summary>Releases resources after every worker has finished.</summary>
    [GlobalCleanup]
    public void Cleanup()
    {
        _gate?.Dispose();
        _meter?.Dispose();
        _semaphore?.Dispose();
    }

    /// <summary>Creates a shared primitive with enough permits for every worker.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _meter = new Meter("admission-semaphore-contention-benchmark");
        _gate = new AdmissionGate(new AdmissionOptions(), new BackpressureMetrics(_meter), TimeProvider.System);
        _semaphore = new AsyncSemaphore(Workers);
    }

    /// <summary>Measures a non-queuing acquire and release pair.</summary>
    /// <returns>The completion of every worker.</returns>
    [Benchmark(OperationsPerInvoke = Operations)]
    public Task AcquireReleaseAsync() => RunAsync(Operation.TryAcquire);

    /// <summary>Measures the synchronous completion of an asynchronous wait and its release.</summary>
    /// <returns>The completion of every worker.</returns>
    [Benchmark(OperationsPerInvoke = Operations)]
    public Task WaitAsync() => RunAsync(Operation.Wait);

    private async Task RunAsync(Operation operation)
    {
        using var ready = new CountdownEvent(Workers);
        using var start = new ManualResetEventSlim(false);
        var tasks = new Task[Workers];
        for (var i = 0; i < Workers; i++)
        {
            var count = (Operations / Workers) + (i < Operations % Workers ? 1 : 0);
            var worker = new WorkerRun(this, operation, count, ready, start);
            tasks[i] = Task.Factory.StartNew(
                               static state => Unsafe.As<WorkerRun>(state)!.ExecuteAsync(),
                               worker,
                               CancellationToken.None,
                               TaskCreationOptions.LongRunning,
                               TaskScheduler.Default)
                           .Unwrap();
        }

        ready.Wait(CancellationToken.None);
        start.Set();
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    private async Task RunWorkerAsync(Operation operation, int count)
    {
        var semaphore = ThrowHelper.Required(_semaphore, "Benchmark semaphore was not initialized.");
        var gate = ThrowHelper.Required(_gate, "Benchmark gate was not initialized.");
        for (var i = 0; i < count; i++)
        {
            switch (operation)
            {
                case Operation.TryAcquire:
                    if (!semaphore.TryAcquire())
                        throw new InvalidOperationException("A worker lost a free permit.");
                    semaphore.Release();
                    break;
                case Operation.Wait:
                    await semaphore.WaitAsync(CancellationToken.None).ConfigureAwait(false);
                    semaphore.Release();
                    break;
                case Operation.Admission:
                    var (decision, lease) = await gate.AcquireAsync("grpc", "get", "client", CancellationToken.None).ConfigureAwait(false);
                    if (!decision.IsAccepted)
                        throw new InvalidOperationException("Uncontended admission was refused.");
                    lease.Dispose();
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(operation));
            }
        }
    }

    private sealed class WorkerRun
    {
        private readonly int _count;
        private readonly Operation _operation;
        private readonly AdmissionSemaphoreContentionBenchmarks _owner;
        private readonly CountdownEvent _ready;
        private readonly ManualResetEventSlim _start;

        internal WorkerRun(AdmissionSemaphoreContentionBenchmarks owner, Operation operation, int count, CountdownEvent ready, ManualResetEventSlim start)
        {
            _owner = owner;
            _operation = operation;
            _count = count;
            _ready = ready;
            _start = start;
        }

        internal Task ExecuteAsync()
        {
            _ = _ready.Signal();
            _start.Wait(CancellationToken.None);
            return _owner.RunWorkerAsync(_operation, _count);
        }
    }
}
