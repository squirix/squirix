using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Squirix.E2EBenchmarks.Support.Client;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2EBenchmarks.Cache;

/// <summary>End-to-end quorum commit benchmarks over a two-node RF=2 cluster.</summary>
[BenchmarkCategory("e2e", "replication", "write")]
public class ReplicaCommitBenchmarks : IAsyncDisposable
{
    private const int Batch = 8;

    private ICache<string>? _cache;
    private E2EBenchmarkClientLease? _client;
    private TempDirectory? _dataDir;
    private TestNodeHost? _nodeA;
    private TestNodeHost? _nodeB;
    private ClusterTls? _mtls;
    private int _offset;

    /// <summary>Gets the consumer used to prevent dead-code elimination.</summary>
    protected Consumer Consumer { get; } = new();

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
        var uriA = ListenPortPool.EndToEndBenchmarks.NextHttpUri();
        var uriB = ListenPortPool.EndToEndBenchmarks.NextHttpUri();
        _mtls = new ClusterTls();
        _dataDir = new TempDirectory("squirix-e2e-replica-commit");
        var topology = new[] { ("nodeA", uriA), ("nodeB", uriB) };
        try
        {
            _nodeA = await TestNodeHostFactory.StartNodeAsync(
                "nodeA",
                uriA,
                topology,
                new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = NodePathKit.Combine(_dataDir.Path, "nodeA") },
                _mtls,
                CancellationToken.None).ConfigureAwait(false);
            _nodeB = await TestNodeHostFactory.StartNodeAsync(
                "nodeB",
                uriB,
                topology,
                new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = NodePathKit.Combine(_dataDir.Path, "nodeB") },
                _mtls,
                CancellationToken.None).ConfigureAwait(false);

            _client = await E2EBenchmarkClientLease.ConnectAsync(uriA, CancellationToken.None).ConfigureAwait(false);
            _cache = await _client.Client.GetCacheAsync<string>("replica-commit", CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            await DisposeClusterAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Stops the benchmark cluster.</summary>
    /// <returns>A task that completes after cleanup.</returns>
    [GlobalCleanup]
    public Task CleanupAsync() => DisposeClusterAsync();

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

        if (_nodeA != null)
            await _nodeA.DisposeAsync().ConfigureAwait(false);
        _nodeA = null;

        if (_nodeB != null)
            await _nodeB.DisposeAsync().ConfigureAwait(false);
        _nodeB = null;

        _mtls?.Dispose();
        _mtls = null;
        _dataDir?.Dispose();
        _dataDir = null;
    }
}
