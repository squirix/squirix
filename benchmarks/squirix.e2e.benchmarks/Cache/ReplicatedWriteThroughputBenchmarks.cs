using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.E2EBenchmarks.Fixtures;
using Squirix.E2EBenchmarks.Support.Client;
using Squirix.E2EBenchmarks.Support.Harness;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using Squirix.Testing;

namespace Squirix.E2EBenchmarks.Cache;

/// <summary>
///     End-to-end replicated write throughput over a three-node RF=3 cluster with many concurrent clients. The mix is 70 percent string sets,
///     15 percent JSON sets, and 15 percent gets. Operations per second come from the benchmark mean; the latency percentiles and the flushes of the
///     group logs per write, and the average commit batch size are printed when the run ends.
/// </summary>
/// <remarks>
///     Every write uses a key of its own, so no two concurrent writes meet on a key: this is the best case for batching, with no key contention that would end
///     a batch early. The latency percentiles cover the last <see cref="LatencyIterations" /> iterations, which are workload iterations whatever the
///     pilot and warmup stages ran. The batch size sums the commit rounds of the committers of the groups the nodes lead when the run starts and when it
///     ends. A group that changes its leader during the run loses the rounds of its old leader, so the figure is exact only for a run without a leader change;
///     a drop of the total is printed as n/a, and a change that keeps the total growing cannot be told apart.
/// </remarks>
[BenchmarkCategory("e2e", "replication", "write")]
public class ReplicatedWriteThroughputBenchmarks : IAsyncDisposable
{
    private const int MixPeriod = 20;
    private const int StringSetShare = 14;
    private const int JsonSetShare = 17;
    private const int SeededKeys = 256;
    private const int TotalOperations = 2048;
    private const int LatencyIterations = 3;
    private const int SeedParallelism = 32;
    private const int P50Percent = 50;
    private const int P99Percent = 99;
    private const int PercentBase = 100;

    private readonly Queue<double[]> _latenciesMs = new();
    private E2EBenchmarkClientLease? _client;
    private TestCluster<ClusterStartOptions>? _cluster;
    private long _counter;
    private TempDirectory? _dir;
    private long _flushesAtStart;
    private (long Rounds, long Entries) _roundsAtStart;
    private double[][] _iterationLatenciesMs = [];
    private ICache<BenchmarkUserProfile>? _profiles;
    private ICache<string>? _strings;
    private long _writes;

    /// <summary>Gets or sets the number of concurrent clients; each runs its share of the operations one after another.</summary>
    [Params(1, 48, 256)]
    public int Clients { get; set; }

    /// <summary>Stops the benchmark cluster and prints the latency percentiles and the flushes per write of the whole run.</summary>
    /// <returns>A task that completes after cleanup.</returns>
    [GlobalCleanup]
    public Task CleanupAsync()
    {
        PrintReport();
        return DisposeClusterAsync();
    }

    /// <summary>Keeps the latencies of the finished iteration; only the last few iterations, which the workload stage ends with, are reported.</summary>
    [IterationCleanup]
    public void CollectLatencies()
    {
        var all = new double[TotalOperations];
        var offset = 0;
        foreach (var samples in _iterationLatenciesMs)
        {
            samples.CopyTo(all, offset);
            offset += samples.Length;
        }

        _latenciesMs.Enqueue(all);
        while (_latenciesMs.Count > LatencyIterations)
            _ = _latenciesMs.Dequeue();
    }

    /// <summary>Runs the mixed workload from all clients and waits until every operation has completed.</summary>
    /// <returns>A task that completes when the workload has finished.</returns>
    [Benchmark(OperationsPerInvoke = TotalOperations)]
    [BenchmarkCategory("write")]
    public async Task MixedReplicatedWorkloadAsync()
    {
        var workers = new Task[Clients];
        for (var client = 0; client < workers.Length; client++)
            workers[client] = RunClientAsync(client, OperationsOf(client));

        await Task.WhenAll(workers).ConfigureAwait(false);
    }

    /// <summary>Allocates the latency slots of the next iteration.</summary>
    [IterationSetup]
    public void PrepareIteration()
    {
        _iterationLatenciesMs = new double[Clients][];
        for (var i = 0; i < _iterationLatenciesMs.Length; i++)
            _iterationLatenciesMs[i] = new double[OperationsOf(i)];
    }

    /// <summary>Starts a three-node RF=3 cluster, opens the public client, and seeds the keys the gets read.</summary>
    /// <returns>A task that completes when the cluster is ready.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        using var heldA = ListenPortPool.EndToEndBenchmarks.HoldPort();
        using var heldB = ListenPortPool.EndToEndBenchmarks.HoldPort();
        using var heldC = ListenPortPool.EndToEndBenchmarks.HoldPort();
        _dir = new TempDirectory("squirix-e2e-replica-write-throughput");
        ClusterNode[] topology = [new("nodeA", heldA.HttpUri), new("nodeB", heldB.HttpUri), new("nodeC", heldC.HttpUri)];
        try
        {
            _cluster = TestCluster<ClusterStartOptions>.Create(topology);
            _ = await _cluster.StartAllAsync(StartOptions, cancellationToken: CancellationToken.None).ConfigureAwait(false);

            _client = await E2EBenchmarkClientLease.ConnectAsync(heldA.HttpUri, CancellationToken.None).ConfigureAwait(false);
            _strings = await _client.Client.GetCacheAsync<string>("write-throughput-strings", CancellationToken.None).ConfigureAwait(false);
            _profiles = await _client.Client.GetCacheAsync<BenchmarkUserProfile>("write-throughput-profiles", CancellationToken.None).ConfigureAwait(false);
            await SeedAsync().ConfigureAwait(false);
            _flushesAtStart = GroupLogFlushReads.Total(_cluster);
            _roundsAtStart = ReplicaCommitBatchReads.Totals(_cluster);
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

    private static double Percentile(ReadOnlySpan<double> sorted, int percent) => sorted[(((sorted.Length * percent) + PercentBase - 1) / PercentBase) - 1];

    private static string SeededKey(long index) => "seed-" + (index % SeededKeys).ToString(CultureInfo.InvariantCulture);

    /// <summary>Splits the operations of an iteration over the clients so that all of them run: the first clients take one more when the split is uneven.</summary>
    /// <param name="client">The client index.</param>
    /// <returns>The number of operations the client runs.</returns>
    private int OperationsOf(int client) => (TotalOperations / Clients) + (client < TotalOperations % Clients ? 1 : 0);

    private async Task DisposeClusterAsync()
    {
        if (_client != null)
            await _client.DisposeAsync().ConfigureAwait(false);
        _client = null;
        _strings = null;
        _profiles = null;

        if (_cluster != null)
            await _cluster.DisposeAsync().ConfigureAwait(false);
        _cluster = null;

        _dir?.Dispose();
        _dir = null;
    }

    private async Task ExecuteAsync(long sequence)
    {
        var slot = int.CreateTruncating(sequence % MixPeriod);
        var key = "wt-" + sequence.ToString(CultureInfo.InvariantCulture);
        if (slot < StringSetShare)
        {
            _ = Interlocked.Increment(ref _writes);
            await _strings!.SetAsync(key, "v", cancellationToken: CancellationToken.None).ConfigureAwait(false);
        }
        else if (slot < JsonSetShare)
        {
            _ = Interlocked.Increment(ref _writes);
            await _profiles!.SetAsync(key, E2EBenchmarkDataFactory.CreateUserProfile(int.CreateTruncating(sequence % int.MaxValue)), cancellationToken: CancellationToken.None).ConfigureAwait(false);
        }
        else
        {
            _ = await _strings!.GetValueAsync(SeededKey(sequence), CancellationToken.None).ConfigureAwait(false);
        }
    }

    private void PrintReport()
    {
        if (_latenciesMs.Count == 0 || _cluster == null)
            return;

        var samples = new double[_latenciesMs.Count * TotalOperations];
        var offset = 0;
        foreach (var iteration in _latenciesMs)
        {
            iteration.CopyTo(samples, offset);
            offset += iteration.Length;
        }

        Array.Sort(samples);
        var flushes = GroupLogFlushReads.Total(_cluster) - _flushesAtStart;
        var writes = Interlocked.Read(ref _writes);
        var (rounds, entries) = ReplicaCommitBatchReads.Totals(_cluster);
        var batchSize = rounds > _roundsAtStart.Rounds && entries >= _roundsAtStart.Entries
            ? (double.CreateChecked(entries - _roundsAtStart.Entries) / double.CreateChecked(rounds - _roundsAtStart.Rounds)).ToString("F2", CultureInfo.InvariantCulture)
            : "n/a";
        Console.WriteLine(
            string.Create(
                CultureInfo.InvariantCulture,
                $"// Replicated write throughput (Clients={Clients}): latencySamples={samples.Length} writes={writes} p50={Percentile(samples, P50Percent):F2} ms p99={Percentile(samples, P99Percent):F2} ms flushes={flushes} flushesPerWrite={double.CreateChecked(flushes) / double.CreateChecked(Math.Max(1L, writes)):F2} averageBatchSize={batchSize}"));
    }

    private async Task RunClientAsync(int client, int operations)
    {
        await Task.Yield();
        var samples = _iterationLatenciesMs[client];
        for (var i = 0; i < operations; i++)
        {
            var sequence = Interlocked.Increment(ref _counter);
            var start = Stopwatch.GetTimestamp();
            await ExecuteAsync(sequence).ConfigureAwait(false);
            samples[i] = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
        }
    }

    private async Task SeedAsync()
    {
        var batch = new Task[SeedParallelism];
        for (var start = 0; start < SeededKeys; start += SeedParallelism)
        {
            for (var i = 0; i < batch.Length; i++)
                batch[i] = _strings!.SetAsync(SeededKey(start + i), "v", cancellationToken: CancellationToken.None);

            await Task.WhenAll(batch).ConfigureAwait(false);
        }
    }

    private ClusterStartOptions StartOptions(string node) => new()
    {
        ReplicaCount = 3,
        DataDir = PathKit.Combine(_dir!, node),
    };
}
