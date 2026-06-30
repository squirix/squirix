using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Squirix.E2EBenchmarks.Support.Client;
using Squirix.E2EBenchmarks.Support.Cluster;

namespace Squirix.E2EBenchmarks.Cache;

/// <summary>
/// End-to-end allocation baselines for scalar cache values on the gRPC wire path.
/// Re-run with the same filter after changing wire encoding to compare allocated bytes.
/// </summary>
[MemoryDiagnoser]
[MinIterationTime(150)]
public class CacheWireScalarAllocBenchmarks
{
    private const int Batch = 512;
    private const int KeyCount = 512;
    private const string CachePrefix = "bench-wire-scalar-alloc";

    private readonly Consumer _consumer = new();
    private readonly string[] _keys = new string[KeyCount];

    private ICache<string>? _stringReadCache;
    private ICache<string>? _stringWriteCache;
    private ICache<int>? _intReadCache;
    private ICache<int>? _intWriteCache;
    private ICache<long>? _longReadCache;
    private ICache<long>? _longWriteCache;
    private ICache<double>? _doubleReadCache;
    private ICache<double>? _doubleWriteCache;
    private ICache<bool>? _boolReadCache;
    private ICache<bool>? _boolWriteCache;

    private BenchmarkClientLease? _client;
    private BenchmarkNodeScope? _node;

    /// <summary>Stops benchmark dependencies.</summary>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_client is not null)
            await _client.DisposeAsync().ConfigureAwait(false);

        if (_node is not null)
            await _node.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Bool scalar read via <c>GetValueAsync</c>.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task ReadBoolBatchedAsync()
    {
        var cache = _boolReadCache!;
        for (var i = 0; i < Batch; i++)
        {
            var result = await cache.GetValueAsync(_keys[i], CancellationToken.None).ConfigureAwait(false);
            _consumer.Consume(result.Value);
        }
    }

    /// <summary>Double scalar read via <c>GetValueAsync</c>.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task ReadDoubleBatchedAsync()
    {
        var cache = _doubleReadCache!;
        for (var i = 0; i < Batch; i++)
        {
            var result = await cache.GetValueAsync(_keys[i], CancellationToken.None).ConfigureAwait(false);
            _consumer.Consume(result.Value);
        }
    }

    /// <summary>Int32 scalar read via <c>GetValueAsync</c>.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task ReadIntBatchedAsync()
    {
        var cache = _intReadCache!;
        for (var i = 0; i < Batch; i++)
        {
            var result = await cache.GetValueAsync(_keys[i], CancellationToken.None).ConfigureAwait(false);
            _consumer.Consume(result.Value);
        }
    }

    /// <summary>Int64 scalar read via <c>GetValueAsync</c>.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task ReadLongBatchedAsync()
    {
        var cache = _longReadCache!;
        for (var i = 0; i < Batch; i++)
        {
            var result = await cache.GetValueAsync(_keys[i], CancellationToken.None).ConfigureAwait(false);
            _consumer.Consume(result.Value);
        }
    }

    /// <summary>String scalar read via <c>GetValueAsync</c>.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task ReadStringBatchedAsync()
    {
        var cache = _stringReadCache!;
        for (var i = 0; i < Batch; i++)
        {
            var result = await cache.GetValueAsync(_keys[i], CancellationToken.None).ConfigureAwait(false);
            _consumer.Consume(result.Value ?? string.Empty);
        }
    }

    /// <summary>Starts a single-node server and seeds scalar read caches.</summary>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        for (var i = 0; i < KeyCount; i++)
            _keys[i] = $"scalar:{i.ToString("D5", CultureInfo.InvariantCulture)}";

        _node = await BenchmarkNodeScope.StartAsync(CancellationToken.None).ConfigureAwait(false);
        _client = await _node.OpenClientAsync(CancellationToken.None).ConfigureAwait(false);

        _stringReadCache = await _client.Client.GetCacheAsync<string>($"{CachePrefix}-string", CancellationToken.None).ConfigureAwait(false);
        _stringWriteCache = await _client.Client.GetCacheAsync<string>($"{CachePrefix}-string-write", CancellationToken.None).ConfigureAwait(false);
        _intReadCache = await _client.Client.GetCacheAsync<int>($"{CachePrefix}-int", CancellationToken.None).ConfigureAwait(false);
        _intWriteCache = await _client.Client.GetCacheAsync<int>($"{CachePrefix}-int-write", CancellationToken.None).ConfigureAwait(false);
        _longReadCache = await _client.Client.GetCacheAsync<long>($"{CachePrefix}-long", CancellationToken.None).ConfigureAwait(false);
        _longWriteCache = await _client.Client.GetCacheAsync<long>($"{CachePrefix}-long-write", CancellationToken.None).ConfigureAwait(false);
        _doubleReadCache = await _client.Client.GetCacheAsync<double>($"{CachePrefix}-double", CancellationToken.None).ConfigureAwait(false);
        _doubleWriteCache = await _client.Client.GetCacheAsync<double>($"{CachePrefix}-double-write", CancellationToken.None).ConfigureAwait(false);
        _boolReadCache = await _client.Client.GetCacheAsync<bool>($"{CachePrefix}-bool", CancellationToken.None).ConfigureAwait(false);
        _boolWriteCache = await _client.Client.GetCacheAsync<bool>($"{CachePrefix}-bool-write", CancellationToken.None).ConfigureAwait(false);

        for (var i = 0; i < KeyCount; i++)
        {
            var key = _keys[i];
            await _stringReadCache.SetAsync(key, "bench-scalar-string", cancellationToken: CancellationToken.None).ConfigureAwait(false);
            await _intReadCache.SetAsync(key, 42, cancellationToken: CancellationToken.None).ConfigureAwait(false);
            await _longReadCache.SetAsync(key, 9_007_199_254_740_991L, cancellationToken: CancellationToken.None).ConfigureAwait(false);
            await _doubleReadCache.SetAsync(key, 3.141592653589793, cancellationToken: CancellationToken.None).ConfigureAwait(false);
            await _boolReadCache.SetAsync(key, true, cancellationToken: CancellationToken.None).ConfigureAwait(false);
        }
    }

    /// <summary>Bool scalar write via <c>SetAsync</c>.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task WriteBoolBatchedAsync()
    {
        var cache = _boolWriteCache!;
        for (var i = 0; i < Batch; i++)
            await cache.SetAsync(_keys[i], true, cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Double scalar write via <c>SetAsync</c>.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task WriteDoubleBatchedAsync()
    {
        var cache = _doubleWriteCache!;
        for (var i = 0; i < Batch; i++)
            await cache.SetAsync(_keys[i], 3.141592653589793, cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Int32 scalar write via <c>SetAsync</c>.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task WriteIntBatchedAsync()
    {
        var cache = _intWriteCache!;
        for (var i = 0; i < Batch; i++)
            await cache.SetAsync(_keys[i], 42, cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Int64 scalar write via <c>SetAsync</c>.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task WriteLongBatchedAsync()
    {
        var cache = _longWriteCache!;
        for (var i = 0; i < Batch; i++)
            await cache.SetAsync(_keys[i], 9_007_199_254_740_991L, cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>String scalar write via <c>SetAsync</c>.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task WriteStringBatchedAsync()
    {
        var cache = _stringWriteCache!;
        for (var i = 0; i < Batch; i++)
            await cache.SetAsync(_keys[i], "bench-scalar-string", cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }
}
