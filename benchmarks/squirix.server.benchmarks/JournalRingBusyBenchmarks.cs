using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Isolates enqueue notification overhead while a consumer drains bounded bursts without parking.</summary>
[MemoryDiagnoser]
public class JournalRingBusyBenchmarks
{
    private const int Operations = 65536;
    private readonly JournalWorkItem _item = JournalWorkItem.Shutdown();
    private BoundedJournalRing? _ring;

    /// <summary>Gets or sets the number of enqueues followed by a complete drain.</summary>
    [Params(1, 32)]
    public int Burst { get; set; }

    /// <summary>Releases the ring after all invocations complete.</summary>
    [GlobalCleanup]
    public void Cleanup() => _ring?.Dispose();

    /// <summary>Creates a ring large enough for every burst without waiting for capacity.</summary>
    [GlobalSetup]
    public void Setup() => _ring = new BoundedJournalRing(Burst, false);

    /// <summary>Measures enqueue and dequeue pairs without a parked consumer or blocking capacity wait.</summary>
    /// <returns>The completion of every pair.</returns>
    /// <exception cref="InvalidOperationException">The ring lost an enqueued item.</exception>
    [Benchmark(OperationsPerInvoke = Operations)]
    public async Task BusyBurstAsync()
    {
        var ring = ThrowHelper.Required(_ring, "Benchmark ring was not initialized.");
        for (var round = 0; round < Operations / Burst; round++)
        {
            for (var i = 0; i < Burst; i++)
                await ring.EnqueueAsync(_item, CancellationToken.None).ConfigureAwait(false);
            for (var i = 0; i < Burst; i++)
            {
                if (!ring.TryDequeue(out _))
                    throw new InvalidOperationException("The ring lost an enqueued item.");
            }
        }
    }
}
