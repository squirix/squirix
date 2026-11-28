using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.E2EBenchmarks.Support.Client;
using Squirix.E2EBenchmarks.Support.Cluster;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.E2EBenchmarks.Cache;

/// <summary>Measures acknowledged inserts through one SDK connection to an ephemeral server.</summary>
[MemoryDiagnoser]
public class PipelinedCacheFillBenchmarks
{
    private const int KeyCount = 32_768;
    private const int PipelineDepth = 128;
    private const string Value = "12345678";

    private readonly Task[] _writes = new Task[PipelineDepth];
    private readonly Task<bool>[] _removals = new Task<bool>[PipelineDepth];
    private TestCluster<ClusterStartOptions>? _cluster;
    private E2EBenchmarkClientLease? _client;
    private ICache<string>? _cache;
    private int _nextKey;

    /// <summary>Opens the SDK connection once for the entire run.</summary>
    [GlobalSetup]
    public async Task OpenAsync()
    {
        _cluster = await E2EBenchmarkNodeCluster.StartAsync(cancellationToken: CancellationToken.None).ConfigureAwait(false);
        _client = await _cluster.OpenClientAsync(CancellationToken.None).ConfigureAwait(false);
        _cache = await _client.Client.GetCacheAsync<string>("fill", CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Writes eight-byte values to distinct eight-character keys, awaiting each window of acknowledgements.</summary>
    /// <exception cref="InvalidOperationException">The cache has not been opened.</exception>
    [Benchmark(OperationsPerInvoke = KeyCount)]
    public async Task FillAsync()
    {
        var cache = _cache ?? ThrowCacheUnavailable();
        var end = checked(_nextKey + KeyCount);
        while (_nextKey < end)
        {
            for (var i = 0; i < PipelineDepth; i++)
                _writes[i] = cache.SetAsync(Key(_nextKey++), Value, cancellationToken: CancellationToken.None);

            await Task.WhenAll(_writes).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies the writes and makes every key available for a fresh insert in the next iteration.</summary>
    /// <exception cref="InvalidOperationException">The cache is unavailable or an acknowledged key is missing.</exception>
    [IterationCleanup]
    public void Clear()
    {
        var cache = _cache ?? ThrowCacheUnavailable();
        for (var start = 0; start < _nextKey; start += PipelineDepth)
        {
            for (var i = 0; i < PipelineDepth; i++)
                _removals[i] = cache.RemoveAsync(Key(start + i), CancellationToken.None);

            var removed = Task.WhenAll(_removals).GetAwaiter().GetResult();
            if (Array.IndexOf(removed, false) >= 0)
                throw new InvalidOperationException("An acknowledged key was missing.");
        }

        _nextKey = 0;
    }

    /// <summary>Closes the client and stops the server.</summary>
    [GlobalCleanup]
    public async Task CloseAsync()
    {
        if (_client != null)
            await _client.DisposeAsync().ConfigureAwait(false);
        if (_cluster != null)
            await _cluster.DisposeAsync().ConfigureAwait(false);
    }

    private static ICache<string> ThrowCacheUnavailable() => throw new InvalidOperationException("The benchmark cache is unavailable.");

    private static string Key(int index) => index.ToString("X8", CultureInfo.InvariantCulture);
}
