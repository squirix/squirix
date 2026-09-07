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

/// <summary>End-to-end leader-authority benchmarks over a single node.</summary>
[BenchmarkCategory("e2e", "authority", "read")]
public class LeaderAuthorityBenchmarks : IAsyncDisposable
{
    private const int Batch = 8;
    private const int Seeded = 64;

    private ICache<string>? _cache;
    private E2EBenchmarkClientLease? _client;
    private TempDirectory? _dataDir;
    private TestNodeHost? _node;
    private int _offset;

    /// <summary>Stops the benchmark node.</summary>
    /// <returns>A task that completes after cleanup.</returns>
    [GlobalCleanup]
    public Task CleanupAsync() => DisposeClusterAsync();

    /// <summary>Measures local reads on the leader-authority fast path.</summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = Batch)]
    [BenchmarkCategory("read")]
    public async Task AuthorityReadAsync()
    {
        var cache = _cache!;
        var offset = Interlocked.Add(ref _offset, Batch);
        for (var i = 0; i < Batch; i++)
        {
            _ = await cache.GetValueAsync(NodeInvariantIndexStrings.FormatPrefixedPadded("authority", (offset + i) % Seeded, "D10", 10), CancellationToken.None)
                           .ConfigureAwait(false);
        }
    }

    /// <summary>Measures local writes on the leader-authority fast path.</summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = Batch)]
    [BenchmarkCategory("write")]
    public async Task AuthorityWriteAsync()
    {
        var cache = _cache!;
        var offset = Interlocked.Add(ref _offset, Batch);
        for (var i = 0; i < Batch; i++)
        {
            await cache.SetAsync(NodeInvariantIndexStrings.FormatPrefixedPadded("authority", Seeded + offset + i, "D10", 10), "v", cancellationToken: CancellationToken.None)
                       .ConfigureAwait(false);
        }
    }

    /// <summary>Starts a single node, opens the public client, and seeds read keys.</summary>
    /// <returns>A task that completes when the node is ready.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        var uri = ListenPortPool.EndToEndBenchmarks.NextHttpUri();
        _dataDir = new TempDirectory("squirix-e2e-authority");
        try
        {
            _node = await TestNodeHostFactory.StartNodeAsync("nodeA", uri, _dataDir.Path, CancellationToken.None).ConfigureAwait(false);
            _client = await E2EBenchmarkClientLease.ConnectAsync(uri, CancellationToken.None).ConfigureAwait(false);
            _cache = await _client.Client.GetCacheAsync<string>("authority", CancellationToken.None).ConfigureAwait(false);
            for (var i = 0; i < Seeded; i++)
            {
                await _cache.SetAsync(NodeInvariantIndexStrings.FormatPrefixedPadded("authority", i, "D10", 10), "v", cancellationToken: CancellationToken.None)
                             .ConfigureAwait(false);
            }
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

        if (_node != null)
            await _node.DisposeAsync().ConfigureAwait(false);
        _node = null;

        _dataDir?.Dispose();
        _dataDir = null;
    }
}
