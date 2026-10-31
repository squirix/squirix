using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Cost of preparing a replicated mutation of a live key against the local cache: the prepare reads the stored entry, which completes
/// synchronously, so it is expected to allocate only the prepared record and not a task per call.
/// </summary>
[MemoryDiagnoser]
public class ReplicaMutationPrepareBenchmarks
{
    private const string CacheName = "cache";
    private const string Key = "k";
    private const int OperationsPerInvoke = 10_000;
    private ReplicaMutationFactory? _factory;

    /// <summary>Prepares the expiry of a live key, which finds no tombstone to build.</summary>
    /// <returns>A task that completes when all prepares finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task PrepareExpireAsync()
    {
        var factory = ThrowHelper.Required(_factory, "Benchmark factory was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = await factory.PrepareExpireAsync(CacheName, Key, 2UL, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Prepares the value replacement of a live key.</summary>
    /// <returns>A task that completes when all prepares finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task PrepareUpdateAsync()
    {
        var factory = ThrowHelper.Required(_factory, "Benchmark factory was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = await factory.PrepareUpdateAsync("op-2", CacheName, Key, "v2", 2UL, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Seeds the live key the prepares read.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        var physical = new PhysicalCache<object?>(TimeProvider.System, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        var local = new ClientCache<object?>(physical, physical);
        await local.SetEntryAsync("op-1", CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, TimeSpan.FromDays(1)), CancellationToken.None).ConfigureAwait(false);
        _factory = new ReplicaMutationFactory(local, "n1", 1UL, TimeProvider.System, NullLogger.Instance);
    }
}
