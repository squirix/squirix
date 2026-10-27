using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.E2EBenchmarks.Support.Client;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using Squirix.Testing;

namespace Squirix.E2EBenchmarks.Cache;

/// <summary>
///     End-to-end benchmarks for many keys expiring at once on one replicated group over a two-node RF=2 cluster. Reads of the expired keys and
///     writes to the same group run concurrently; the wall time to clear the burst shows whether the group serializes one commit per key.
///     SetAsync latency during the burst is printed when the run ends.
/// </summary>
[BenchmarkCategory("e2e", "replication", "expiration")]
public class ReplicaExpiryBurstBenchmarks : IAsyncDisposable
{
    private const int ConcurrentWrites = 64;
    private const int SeedParallelism = 32;
    private const int P99Percent = 99;
    private const int PercentBase = 100;
    private static readonly TimeSpan KeyTimeToLive = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan PastKeyTimeToLive = TimeSpan.FromMilliseconds(750);

    private readonly double[] _iterationLatenciesMs = new double[ConcurrentWrites];
    private readonly List<double> _setLatenciesMs = [];
    private ICache<string>? _cache;
    private E2EBenchmarkClientLease? _client;
    private TestCluster<ClusterStartOptions>? _cluster;
    private TempDirectory? _dir;
    private int _iteration;
    private string[] _keys = [];
    private long _writeCounter;

    /// <summary>Gets or sets the number of keys that expire at the same moment.</summary>
    [Params(256, 1024)]
    public int ExpiringKeys { get; set; }

    /// <summary>Stops the benchmark cluster and prints the SetAsync latency seen during the burst.</summary>
    /// <returns>A task that completes after cleanup.</returns>
    [GlobalCleanup]
    public Task CleanupAsync()
    {
        PrintSetLatency();
        return DisposeClusterAsync();
    }

    /// <summary>Reads every expired key and writes to the same group concurrently.</summary>
    /// <returns>A task that completes when the whole burst has been served.</returns>
    [Benchmark]
    [BenchmarkCategory("expiration", "read", "write")]
    public async Task ClearExpiredBurstAsync()
    {
        var cache = _cache!;
        var keys = _keys;
        var tasks = new Task[keys.Length + ConcurrentWrites];
        for (var i = 0; i < keys.Length; i++)
            tasks[i] = cache.GetValueAsync(keys[i], CancellationToken.None);

        var writeBase = Interlocked.Add(ref _writeCounter, ConcurrentWrites) - ConcurrentWrites;
        for (var i = 0; i < ConcurrentWrites; i++)
            tasks[keys.Length + i] = TimedSetAsync(cache, "burst-write-" + (writeBase + i).ToString(CultureInfo.InvariantCulture), i);

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    /// <summary>Seeds keys with one short time to live and waits until all of them are expired, outside the measured body.</summary>
    [IterationSetup]
    public void SeedExpiringKeys()
    {
        var cache = _cache!;
        var iteration = Interlocked.Increment(ref _iteration);
        _keys = new string[ExpiringKeys];
        for (var i = 0; i < _keys.Length; i++)
            _keys[i] = "burst-" + iteration.ToString(CultureInfo.InvariantCulture) + "-" + i.ToString(CultureInfo.InvariantCulture);

        var options = new CacheEntryOptions { Expiration = KeyTimeToLive };
        var batch = new Task[SeedParallelism];
        for (var start = 0; start < _keys.Length; start += SeedParallelism)
        {
            var count = Math.Min(SeedParallelism, _keys.Length - start);
            for (var i = 0; i < count; i++)
                batch[i] = cache.SetAsync(_keys[start + i], "v", options, CancellationToken.None);

            Task.WhenAll(batch.AsSpan(0, count)).GetAwaiter().GetResult();
        }

        Thread.Sleep(PastKeyTimeToLive);
    }

    /// <summary>Starts a two-node RF=2 cluster and opens the public client.</summary>
    /// <returns>A task that completes when the cluster is ready.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        using var heldA = ListenPortPool.EndToEndBenchmarks.HoldPort();
        using var heldB = ListenPortPool.EndToEndBenchmarks.HoldPort();
        _dir = new TempDirectory("squirix-e2e-replica-expiry-burst");
        ClusterNode[] topology = [new("nodeA", heldA.HttpUri), new("nodeB", heldB.HttpUri)];
        try
        {
            _cluster = TestCluster<ClusterStartOptions>.Create(topology);
            _ = await _cluster.StartAllAsync(StartOptions, cancellationToken: CancellationToken.None).ConfigureAwait(false);

            _client = await E2EBenchmarkClientLease.ConnectAsync(heldA.HttpUri, CancellationToken.None).ConfigureAwait(false);
            _cache = await _client.Client.GetCacheAsync<string>("replica-expiry-burst", CancellationToken.None).ConfigureAwait(false);
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

    /// <summary>Moves the latencies of the finished iteration into the run-wide sample list.</summary>
    [IterationCleanup]
    public void CollectSetLatencies() => _setLatenciesMs.AddRange(_iterationLatenciesMs);

    private async Task TimedSetAsync(ICache<string> cache, string key, int slot)
    {
        var start = Stopwatch.GetTimestamp();
        await cache.SetAsync(key, "v", cancellationToken: CancellationToken.None).ConfigureAwait(false);
        _iterationLatenciesMs[slot] = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
    }

    private void PrintSetLatency()
    {
        var samples = CollectionsMarshal.AsSpan(_setLatenciesMs);
        if (samples.IsEmpty)
            return;

        samples.Sort();
        var sum = 0.0;
        foreach (var sample in samples)
            sum += sample;

        var p99Index = (((samples.Length * P99Percent) + PercentBase - 1) / PercentBase) - 1;
        Console.WriteLine(
            string.Create(
                CultureInfo.InvariantCulture,
                $"// SetAsync during burst (ExpiringKeys={ExpiringKeys}): samples={samples.Length} mean={sum / samples.Length:F2} ms p99={samples[p99Index]:F2} ms max={samples[^1]:F2} ms"));
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
        DataDir = PathKit.Combine(_dir!, node),
    };
}
