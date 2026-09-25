using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Allocation cost of the journal tracing decorator when no trace listener is attached.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class JournalTracingBenchmarks
{
    private const int OperationsPerInvoke = 100_000;
    private readonly CacheKey _key = new("bench", "key");
    private TracingJournalCoordinatorDecorator? _decorated;
    private JournalBenchmarkHost? _host;
    private byte[] _putPayload = [];

    /// <summary>Appends PUT operations through the tracing decorator and awaits durability after each append.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark host was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task DecoratedAppendPutAsync()
    {
        var coordinator = ThrowHelper.Required(_decorated, "Benchmark host was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
        {
            await coordinator.AppendPutUnderGateAsync(_key, _putPayload, CancellationToken.None).ConfigureAwait(false);
            await coordinator.AwaitDurabilityCommitAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    /// <summary>Disposes the journal coordinator created during setup.</summary>
    /// <returns>A task that completes when cleanup finishes.</returns>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_host != null)
            await _host.DisposeAsync().ConfigureAwait(false);
        _host = null;
        _decorated = null;
    }

    /// <summary>Creates the journal coordinator and its tracing decorator.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        var options = new PersistenceOptions
        {
            JournalPlatformBackend = JournalPlatformBackend.RandomAccess,
            JournalMaxSegmentMb = 64,
        };
        _host = await JournalBenchmarkHost.CreateAsync("journal-tracing-bench", options, CancellationToken.None).ConfigureAwait(false);
        _decorated = new TracingJournalCoordinatorDecorator(_host.Coordinator, new NoListenerTracer());
        _putPayload = new byte[256];
        Array.Fill(_putPayload, Convert.ToByte('x'));
    }

    /// <summary>Tracer that reports tracing as disabled, like a production node without a listener.</summary>
    private sealed class NoListenerTracer : IJournalOperationTracer
    {
        IJournalOperationTraceScope? IJournalOperationTracer.Begin(JournalOperationKind kind, in JournalOperationTraceContext? context) => null;
    }
}
