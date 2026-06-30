using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Squirix.E2EBenchmarks.Fixtures;
using Squirix.E2EBenchmarks.Support.Client;
using Squirix.E2EBenchmarks.Support.Cluster;
using Squirix.E2EBenchmarks.Support.Harness;

namespace Squirix.E2EBenchmarks.Cache;

/// <summary>
/// End-to-end allocation baselines for structured cache values on the gRPC wire path.
/// Re-run with the same filter after changing wire encoding to compare allocated bytes.
/// </summary>
[MemoryDiagnoser]
[MinIterationTime(150)]
public class CacheWireStructuredAllocBenchmarks
{
    private const int Batch = 512;
    private const string CacheName = "bench-wire-structured-alloc";
    private const int KeyCount = 512;

    private readonly Consumer _consumer = new();
    private readonly string[] _keys = new string[KeyCount];
    private ICache<BenchmarkUserProfile>? _writeCache;
    private ICache<BenchmarkUserProfile>? _readCache;

    private BenchmarkClientLease? _client;
    private BenchmarkNodeScope? _node;
    private BenchmarkUserProfile? _profile;

    /// <summary>Stops benchmark dependencies.</summary>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_client is not null)
            await _client.DisposeAsync().ConfigureAwait(false);

        if (_node is not null)
            await _node.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Structured value read via public <c>GetValueAsync</c>.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task ReadStructuredValueBatchedAsync()
    {
        var cache = _readCache!;
        for (var i = 0; i < Batch; i++)
        {
            var result = await cache.GetValueAsync(_keys[i], CancellationToken.None).ConfigureAwait(false);
            _consumer.Consume(result.Value?.Id ?? 0);
        }
    }

    /// <summary>Starts a single-node server, opens a typed cache session, and seeds read keys.</summary>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _profile = E2EBenchmarkDataFactory.CreateUserProfile(0);

        for (var i = 0; i < KeyCount; i++)
            _keys[i] = $"profile:{i.ToString("D5", CultureInfo.InvariantCulture)}";

        _node = await BenchmarkNodeScope.StartAsync(CancellationToken.None).ConfigureAwait(false);
        _client = await _node.OpenClientAsync(CancellationToken.None).ConfigureAwait(false);
        _writeCache = await _client.Client.GetCacheAsync<BenchmarkUserProfile>(CacheName, CancellationToken.None).ConfigureAwait(false);
        _readCache = await _client.Client.GetCacheAsync<BenchmarkUserProfile>(CacheName, CancellationToken.None).ConfigureAwait(false);

        var profile = _profile;
        for (var i = 0; i < KeyCount; i++)
            await _readCache.SetAsync(_keys[i], profile, cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Structured value write via public <c>SetAsync</c>.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task WriteStructuredValueBatchedAsync()
    {
        var profile = _profile!;
        for (var i = 0; i < Batch; i++)
            await _writeCache!.SetAsync(_keys[i], profile, cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }
}
