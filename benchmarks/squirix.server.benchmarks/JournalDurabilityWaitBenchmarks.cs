using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;

namespace Squirix.Server.Benchmarks;

/// <summary>Allocation profile of the journal durability wait paths: durably append, flush checkpoint, maintenance gates.</summary>
[SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global", Justification = "BenchmarkDotNet [Params] properties require public setters.")]
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class JournalDurabilityWaitBenchmarks
{
    private const int OperationsPerInvoke = 2_000;
    private const int PutPayloadBytes = 256;
    private readonly CacheKey _key = new("bench", "durability-key");
    private JournalBenchmarkHost? _host;
    private byte[] _payload = [];

    /// <summary>Gets or sets a value indicating whether group-commit batching is enabled.</summary>
    [Params(true, false)]
    public bool GroupCommitEnabled { get; set; }

    /// <summary>Disposes the journal coordinator created during setup.</summary>
    /// <returns>A task that completes when cleanup finishes.</returns>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_host != null)
            await _host.DisposeAsync().ConfigureAwait(false);
        _host = null;
    }

    /// <summary>Durable PUT appends: every operation resolves through the durable appended wait.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark host was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task DurablePutAsync()
    {
        var host = _host ?? throw new InvalidOperationException("Benchmark host was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            await host.Coordinator.AppendPutAndAwaitDurabilityAsync(_key, _payload, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Maintenance begin/end gate pair around an empty exclusive action.</summary>
    /// <returns>A task that completes when the maintenance run finishes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark host was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task MaintenanceBeginEndAsync()
    {
        var host = _host ?? throw new InvalidOperationException("Benchmark host was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            await host.Coordinator.ExecuteMaintenanceExclusiveAsync(static _ => default(ValueTask), CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Pipelined appends resolving durability per operation: through a flush checkpoint wait when
    /// group commit is disabled, or through the group-commit batch wait when enabled.
    /// </summary>
    /// <returns>A task that completes when all operations finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark host was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task PipelinedPutWithFlushCheckpointAsync()
    {
        var host = _host ?? throw new InvalidOperationException("Benchmark host was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
        {
            await host.Coordinator.AppendPutAsync(_key, _payload, CancellationToken.None).ConfigureAwait(false);
            await host.Coordinator.AwaitDurabilityCommitAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    /// <summary>Creates the journal coordinator and payload for the current parameter set.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        var options = new PersistenceOptions
        {
            JournalPlatformBackend = JournalPlatformBackend.RandomAccess,
            JournalGroupCommitMaxWait = GroupCommitEnabled ? TimeSpan.FromMilliseconds(1) : TimeSpan.Zero,
            JournalGroupCommitMaxBatch = 32,
            JournalMaxSegmentMb = 64,
        };
        _host = await JournalBenchmarkHost.CreateAsync("journal-durability-bench", options, CancellationToken.None).ConfigureAwait(false);
        _payload = new byte[PutPayloadBytes];
        Array.Fill(_payload, Convert.ToByte('x'));
    }
}
