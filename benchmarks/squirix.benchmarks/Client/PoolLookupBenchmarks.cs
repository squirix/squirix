using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;

namespace Squirix.Benchmarks.Client;

/// <summary>Measures resolving a transport client and its policy from a configured pool.</summary>
[MemoryDiagnoser]
[InProcess]
[WarmupCount(2)]
[IterationCount(5)]
public class PoolLookupBenchmarks
{
    private const int Batch = 16_384;
    private readonly Consumer _consumer = new();
    private ClientPool? _pool;

    /// <summary>Creates a pool without connecting its transport channels.</summary>
    [GlobalSetup]
    public void Setup()
    {
        Peer[] peers =
        [
            new() { NodeId = "node-a", Uri = new UriBuilder("https", "127.0.0.1", 6510).Uri },
            new() { NodeId = "node-b", Uri = new UriBuilder("https", "127.0.0.1", 6511).Uri },
        ];
        _pool = new ClientPool(peers, static _ => new CallPolicy(TimeProvider.System), TimeProvider.System);
    }

    /// <summary>Resolves both objects needed for a request.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public void LookupBatched()
    {
        var pool = _pool!;
        for (var i = 0; i < Batch; i++)
        {
            var node = pool.ForNode("node-a");
            _consumer.Consume(node.Client);
            _consumer.Consume(node.Policy);
        }
    }

    /// <summary>Releases the pool and its policies.</summary>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_pool != null)
            await _pool.DisposeAsync().ConfigureAwait(false);
    }
}
