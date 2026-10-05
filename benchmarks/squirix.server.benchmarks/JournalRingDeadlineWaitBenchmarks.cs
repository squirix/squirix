using System.Threading;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Journal ring idle wait: how long <see cref="BoundedJournalRing.WaitForWork" /> actually parks for a short deadline with no work queued, with and
/// without the high-resolution timer. Mean per wait minus the requested milliseconds is the overshoot; allocation per wait should stay zero.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class JournalRingDeadlineWaitBenchmarks
{
    private const int WaitsPerInvoke = 100;
    private BoundedJournalRing? _ring;

    /// <summary>Gets or sets a value indicating whether the high-resolution timer is allowed; false measures the work-signal timeout fallback.</summary>
    [Params(false, true)]
    public bool UseHighResolutionTimer { get; set; }

    /// <summary>Disposes the ring.</summary>
    [GlobalCleanup]
    public void Cleanup()
    {
        _ring?.Dispose();
        _ring = null;
    }

    /// <summary>Creates the ring.</summary>
    [GlobalSetup]
    public void Setup() => _ring = new BoundedJournalRing(4, UseHighResolutionTimer);

    /// <summary>Waits one millisecond with no work queued.</summary>
    /// <exception cref="System.InvalidOperationException">Thrown when the ring was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = WaitsPerInvoke)]
    public void WaitOneMillisecond()
    {
        var ring = ThrowHelper.Required(_ring, "Ring was not initialized.");
        for (var i = 0; i < WaitsPerInvoke; i++)
            ring.WaitForWork(1, CancellationToken.None);
    }

    /// <summary>Waits five milliseconds with no work queued.</summary>
    /// <exception cref="System.InvalidOperationException">Thrown when the ring was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = WaitsPerInvoke)]
    public void WaitFiveMilliseconds()
    {
        var ring = ThrowHelper.Required(_ring, "Ring was not initialized.");
        for (var i = 0; i < WaitsPerInvoke; i++)
            ring.WaitForWork(5, CancellationToken.None);
    }
}
