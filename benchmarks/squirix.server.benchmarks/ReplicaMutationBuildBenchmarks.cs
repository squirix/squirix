using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of building the canonical mutation of a value replacement and of mapping a replication request around one entry.</summary>
[MemoryDiagnoser]
public class ReplicaMutationBuildBenchmarks
{
    private const string CacheName = "cache";
    private const string Key = "k";
    private const int OperationsPerInvoke = 1_000;
    private const int FingerprintLength = 32;
    private FollowerBatch _batch;
    private ReplicaMutationFactory? _factory;
    private ReplicaRpcHeader _header;
    private string _value = string.Empty;

    /// <summary>Gets or sets the length of the replaced value in characters.</summary>
    [Params(64, 102_400)]
    public int ValueLength { get; set; }

    /// <summary>Builds the mutation that replaces the value of a live key.</summary>
    /// <returns>A task that completes when all builds finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task BuildUpdateAsync()
    {
        var factory = ThrowHelper.Required(_factory, "Benchmark factory was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = await factory.PrepareUpdateAsync("op-2", CacheName, Key, _value, 2UL, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Maps an append batch of one entry to its wire request.</summary>
    /// <returns>The entry count of the last mapped request.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public int MapRequest()
    {
        var entries = 0;
        for (var i = 0; i < OperationsPerInvoke; i++)
            entries = ReplicaRpcGateway.MapRequest(in _header, in _batch).Entries.Count;

        return entries;
    }

    /// <summary>Seeds the live key the builds read and the batch the mapping sends.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _value = new string('x', ValueLength);
        var physical = new PhysicalCache<object?>(TimeProvider.System, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        var local = new ClientCache<object?>(physical, physical);
        await local.SetEntryAsync("op-1", CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, TimeSpan.FromDays(1)), CancellationToken.None).ConfigureAwait(false);
        _factory = new ReplicaMutationFactory(local, "n1", 1UL, TimeProvider.System, NullLogger.Instance);
        _header = new ReplicaRpcHeader("n1", new byte[FingerprintLength], 1, 1, "n1", "n1");
        _batch = new FollowerBatch([new FollowerLogEntry(2, 1, new byte[ValueLength])], "n1", 1, 1, 1, 1);
    }
}
