using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using Squirix.E2EBenchmarks.Support.Cluster;
using Squirix.Extensions.Caching;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.E2EBenchmarks.Cache;

/// <summary>Measures acknowledged binary overwrites through the public adapter on a single TLS node.</summary>
[MemoryDiagnoser]
public class DistributedCacheWriteBenchmarks
{
    private readonly DistributedCacheEntryOptions _entryOptions = new();
    private TestCluster<ClusterStartOptions>? _cluster;
    private SquirixDistributedCache? _cache;
    private byte[] _value = [];

    /// <summary>Gets or sets the number of value bytes per request.</summary>
    [Params(825, 64 * 1024, 1024 * 1024)]
    public int ValueBytes { get; set; }

    /// <summary>Starts one node and seeds the measured key.</summary>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _cluster = await E2EBenchmarkNodeCluster.StartAsync(cancellationToken: CancellationToken.None).ConfigureAwait(false);
        var options = new SquirixCacheOptions();
        options.ClientOptions.Endpoints.Add(_cluster.Topology[0].Uri);
        _cache = new SquirixDistributedCache(Options.Create(options));
        _value = new byte[ValueBytes];
        await _cache.SetAsync("binary", _value, _entryOptions, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Overwrites an existing key through the blocking SDK transport.</summary>
    [Benchmark]
    public void Set() => _cache!.Set("binary", _value, _entryOptions);

    /// <summary>Overwrites an existing key through the asynchronous SDK transport.</summary>
    [Benchmark]
    public Task SetAsync() => _cache!.SetAsync("binary", _value, _entryOptions, CancellationToken.None);

    /// <summary>Closes the client and stops the node.</summary>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_cache != null)
            await _cache.DisposeAsync().ConfigureAwait(false);
        if (_cluster != null)
            await _cluster.DisposeAsync().ConfigureAwait(false);
    }
}
