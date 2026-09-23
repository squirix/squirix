using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.E2EBenchmarks.Support.Client;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2EBenchmarks.Cache;

/// <summary>End-to-end quorum commit benchmarks over a two-node RF=2 cluster.</summary>
[BenchmarkCategory("e2e", "replication", "write")]
public class ReplicaCommitBenchmarks : IAsyncDisposable
{
    private const int Batch = 8;

    private ICache<string>? _cache;
    private E2EBenchmarkClientLease? _client;
    private TestCluster<ClusterStartOptions>? _cluster;
    private TempDirectory? _dir;
    private int _offset;

    /// <summary>Stops the benchmark cluster.</summary>
    /// <returns>A task that completes after cleanup.</returns>
    [GlobalCleanup]
    public Task CleanupAsync() => DisposeClusterAsync();

    /// <summary>Measures replicated SetAsync through owner quorum commit.</summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = Batch)]
    [BenchmarkCategory("write")]
    public async Task ReplicatedSetAsync()
    {
        var cache = _cache!;
        var offset = Interlocked.Add(ref _offset, Batch);
        for (var i = 0; i < Batch; i++)
        {
            await cache.SetAsync(NodeInvariantIndexStrings.FormatPrefixedPadded("commit", offset + i, "D10", 10), "v", cancellationToken: CancellationToken.None)
                       .ConfigureAwait(false);
        }
    }

    /// <summary>Starts a two-node RF=2 cluster and opens the public client.</summary>
    /// <returns>A task that completes when the cluster is ready.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        using var heldA = ListenPortPool.EndToEndBenchmarks.HoldPort();
        using var heldB = ListenPortPool.EndToEndBenchmarks.HoldPort();
        _dir = new TempDirectory("squirix-e2e-replica-commit");
        ClusterNode[] topology = [new("nodeA", heldA.HttpUri), new("nodeB", heldB.HttpUri)];
        try
        {
            _cluster = TestCluster<ClusterStartOptions>.Create(topology);
            _ = await _cluster.StartAllAsync(StartOptions, cancellationToken: CancellationToken.None).ConfigureAwait(false);

            _client = await E2EBenchmarkClientLease.ConnectAsync(heldA.HttpUri, CancellationToken.None).ConfigureAwait(false);
            _cache = await _client.Client.GetCacheAsync<string>("replica-commit", CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            await DisposeClusterAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await DisposeClusterAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    private async Task DisposeClusterAsync()
    {
        if (_client != null)
            await _client.DisposeAsync().ConfigureAwait(false);
        _client = null;
        _cache = null;

        if (_cluster != null)
            await _cluster.DisposeAsync().ConfigureAwait(false);
        _cluster = null;

        _dir?.Dispose();
        _dir = null;
    }

    private ClusterStartOptions StartOptions(string node) => new()
    {
        ReplicaCount = 2,
        DataDir = NodePathKit.Combine(_dir!, node),
    };
}
