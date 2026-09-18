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

/// <summary>End-to-end failover benchmarks over a three-node RF=3 cluster.</summary>
[BenchmarkCategory("e2e", "failover", "write")]
public class FailoverBenchmarks : IAsyncDisposable
{
    private const int Batch = 8;

    private ICache<string>? _cache;
    private E2EBenchmarkClientLease? _client;
    private TestCluster<ClusterStartOptions>? _cluster;
    private TempDirectory? _dataDir;
    private int _offset;

    /// <summary>Stops the benchmark cluster.</summary>
    /// <returns>A task that completes after cleanup.</returns>
    [GlobalCleanup]
    public Task CleanupAsync() => DisposeClusterAsync();

    /// <summary>Measures replicated SetAsync on the failover-capable client path.</summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = Batch)]
    [BenchmarkCategory("write")]
    public async Task FailoverSetAsync()
    {
        var cache = _cache!;
        var offset = Interlocked.Add(ref _offset, Batch);
        for (var i = 0; i < Batch; i++)
        {
            await cache.SetAsync(NodeInvariantIndexStrings.FormatPrefixedPadded("failover", offset + i, "D10", 10), "v", cancellationToken: CancellationToken.None)
                       .ConfigureAwait(false);
        }
    }

    /// <summary>Starts a three-node RF=3 cluster and opens the public client.</summary>
    /// <returns>A task that completes when the cluster is ready.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        using var heldA = ListenPortPool.EndToEndBenchmarks.HoldPort();
        using var heldB = ListenPortPool.EndToEndBenchmarks.HoldPort();
        using var heldC = ListenPortPool.EndToEndBenchmarks.HoldPort();
        _dataDir = new TempDirectory("squirix-e2e-failover");
        ClusterNode[] topology = [new("nodeA", heldA.HttpUri), new("nodeB", heldB.HttpUri), new("nodeC", heldC.HttpUri)];
        try
        {
            _cluster = TestCluster<ClusterStartOptions>.Create(topology);
            _ = await _cluster.StartNodeAsync("nodeA", StartOptions("nodeA"), CancellationToken.None).ConfigureAwait(false);
            _ = await _cluster.StartNodeAsync("nodeB", StartOptions("nodeB"), CancellationToken.None).ConfigureAwait(false);
            _ = await _cluster.StartNodeAsync("nodeC", StartOptions("nodeC"), CancellationToken.None).ConfigureAwait(false);

            _client = await E2EBenchmarkClientLease.ConnectAsync(heldA.HttpUri, CancellationToken.None).ConfigureAwait(false);
            _cache = await _client.Client.GetCacheAsync<string>("failover", CancellationToken.None).ConfigureAwait(false);
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

        _dataDir?.Dispose();
        _dataDir = null;
    }

    private ClusterStartOptions StartOptions(string node) => new()
    {
        ReplicaCount = 3,
        DataDir = NodePathKit.Combine(_dataDir!.Path, node),
    };
}
