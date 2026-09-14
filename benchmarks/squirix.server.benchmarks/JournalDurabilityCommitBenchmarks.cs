using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Durability-commit allocation and throughput benchmarks for the plain completion-source model.</summary>
[SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global", Justification = "BenchmarkDotNet [Params] properties require public setters.")]
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class JournalDurabilityCommitBenchmarks
{
    private const int ConcurrentWriters = 8;
    private const int OperationsPerSequentialInvoke = 1_000;
    private const int OperationsPerWriter = 250;
    private JournalBenchmarkHost? _host;
    private CacheKey _key = new("bench", "key");
    private byte[] _putPayload = new byte[256];

    /// <summary>Gets group-commit max wait values (zero selects the strict fsync path).</summary>
    public static IEnumerable<TimeSpan> GroupCommitMaxWaitValues
    {
        get
        {
            yield return TimeSpan.Zero;
            yield return TimeSpan.FromMilliseconds(1);
        }
    }

    /// <summary>Gets or sets the group commit wait values.</summary>
    [ParamsSource(nameof(GroupCommitMaxWaitValues))]
    public TimeSpan GroupCommitMaxWait { get; set; }

    /// <summary>Awaits durability commits sequentially (one flush or batch wait per operation).</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark host was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerSequentialInvoke)]
    public async Task AwaitDurabilityCommitSequentialAsync()
    {
        var coordinator = ThrowHelper.Required(_host, "Benchmark host was not initialized.").Coordinator;
        for (var i = 0; i < OperationsPerSequentialInvoke; i++)
            await coordinator.AwaitDurabilityCommitAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Awaits durability commits from concurrent writers (shared group-commit batches).</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark host was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = ConcurrentWriters * OperationsPerWriter)]
    public Task AwaitDurabilityCommitConcurrentAsync()
    {
        var coordinator = ThrowHelper.Required(_host, "Benchmark host was not initialized.").Coordinator;
        return Parallel.ForEachAsync(
            new int[ConcurrentWriters],
            new ParallelOptions { MaxDegreeOfParallelism = ConcurrentWriters },
            async (_, cancellationToken) =>
            {
                for (var i = 0; i < OperationsPerWriter; i++)
                    await coordinator.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
            });
    }

    /// <summary>Appends PUT operations, each awaited for durability (durable-append path).</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark host was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerSequentialInvoke)]
    public async Task AppendPutAndAwaitDurabilityAsync()
    {
        var coordinator = ThrowHelper.Required(_host, "Benchmark host was not initialized.").Coordinator;
        for (var i = 0; i < OperationsPerSequentialInvoke; i++)
            await coordinator.AppendPutAndAwaitDurabilityAsync(_key, _putPayload, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Disposes the journal coordinator created during setup.</summary>
    /// <returns>A task that completes when cleanup finishes.</returns>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_host != null)
            await _host.DisposeAsync().ConfigureAwait(false);
        _host = null;
    }

    /// <summary>Creates the journal coordinator and payload for the current parameter set.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        var options = new PersistenceOptions
        {
            JournalPlatformBackend = JournalPlatformBackend.RandomAccess,
            JournalGroupCommitMaxWait = GroupCommitMaxWait,
            JournalGroupCommitMaxBatch = 32,
            JournalMaxSegmentMb = 64,
        };
        _host = await JournalBenchmarkHost.CreateAsync("durability-commit-bench", options, CancellationToken.None).ConfigureAwait(false);
        _putPayload = new byte[256];
        Array.Fill(_putPayload, Convert.ToByte('x'));
        _key = new CacheKey("bench", "durability");
    }
}
