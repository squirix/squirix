using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Squirix.E2EBenchmarks.Support.Client;
using Squirix.E2EBenchmarks.Support.Cluster;

namespace Squirix.E2EBenchmarks.Cache;

/// <summary>Baseline end-to-end benchmarks for the public Squirix SDK against a real single-node Squirix server.</summary>
[MemoryDiagnoser]
[MinIterationTime(150)]
public sealed class PublicSdkOperationsBenchmarks
{
    private const string CacheName = "bench-public-sdk-operations";
    private const int KeyCount = 8_192;
    private const int MixedBatch = 1_024;
    private const int ReadBatch = 1_024;
    private const int WriteBatch = 256;

    private readonly Consumer _consumer = new();
    private readonly string[] _existingKeys = new string[KeyCount];
    private readonly string[] _expiringKeys = new string[KeyCount];
    private readonly string[] _missingKeys = new string[KeyCount];
    private BenchmarkClientLease? _client;
    private int _getOrAddMissingOffset;
    private int _mixedWriteOffset;
    private BenchmarkNodeScope? _node;
    private ICache<string>? _squirix;
    private int _writeOffset;

    /// <summary>Stops benchmark dependencies.</summary>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_client is not null)
            await _client.DisposeAsync().ConfigureAwait(false);

        if (_node is not null)
            await _node.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Calls <see cref="ICache{T}.GetOrAddAsync" /> on existing keys, so the factory must stay cold.
    /// </summary>
    [Benchmark(OperationsPerInvoke = ReadBatch)]
    public async Task GetOrAddExistingValueBatchedAsync()
    {
        var cache = _squirix!;
        for (var i = 0; i < ReadBatch; i++)
        {
            var result = await cache.GetOrAddAsync(_existingKeys[i], ColdFactoryAsync, cancellationToken: CancellationToken.None).ConfigureAwait(false);
            _consumer.Consume(result.Value ?? string.Empty);
        }
    }

    /// <summary>
    /// Calls <see cref="ICache{T}.GetOrAddAsync" /> on new unique keys, so the factory and insert path are measured.
    /// </summary>
    [Benchmark(OperationsPerInvoke = WriteBatch)]
    public async Task GetOrAddMissingValueBatchedAsync()
    {
        var cache = _squirix!;
        var offset = Interlocked.Add(ref _getOrAddMissingOffset, WriteBatch);
        for (var i = 0; i < WriteBatch; i++)
        {
            var result = await cache.GetOrAddAsync($"get-or-add:{(offset + i).ToString("D10", CultureInfo.InvariantCulture)}", CreateValueAsync, cancellationToken: CancellationToken.None).ConfigureAwait(false);
            _consumer.Consume(result.Value ?? string.Empty);
        }
    }

    /// <summary>Runs a deterministic 90 percent read / 10 percent write public SDK workload.</summary>
    [Benchmark(OperationsPerInvoke = MixedBatch)]
    public async Task MixedReadWriteBatchedAsync()
    {
        var cache = _squirix!;
        var writeOffset = Interlocked.Add(ref _mixedWriteOffset, MixedBatch / 10);
        var writes = 0;
        for (var i = 0; i < MixedBatch; i++)
        {
            if (i % 10 is 0)
            {
                await cache.SetAsync($"mixed-write:{(writeOffset + writes).ToString("D10", CultureInfo.InvariantCulture)}", $"value:{i.ToString("D5", CultureInfo.InvariantCulture)}", cancellationToken: CancellationToken.None).ConfigureAwait(false);
                writes++;
                continue;
            }

            _consumer.Consume((await cache.GetValueAsync(_existingKeys[i], CancellationToken.None).ConfigureAwait(false)).Value ?? string.Empty);
        }
    }

    /// <summary>
    /// Overwrites existing keys through the public <c>SetAsync</c> API.
    /// </summary>
    [Benchmark(OperationsPerInvoke = WriteBatch)]
    public async Task OverwriteExistingValueBatchedAsync()
    {
        var cache = _squirix!;
        for (var i = 0; i < WriteBatch; i++)
            await cache.SetAsync(_existingKeys[i], $"overwrite:{Environment.TickCount64.ToString(CultureInfo.InvariantCulture)}:{i.ToString("D5", CultureInfo.InvariantCulture)}", cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads existing keys through <see cref="ICache{T}.GetValueAsync" />.
    /// </summary>
    [Benchmark(OperationsPerInvoke = ReadBatch)]
    public async Task ReadExistingValueBatchedAsync()
    {
        var cache = _squirix!;
        for (var i = 0; i < ReadBatch; i++)
        {
            var result = await cache.GetValueAsync(_existingKeys[i], CancellationToken.None).ConfigureAwait(false);
            _consumer.Consume(result.Value ?? string.Empty);
        }
    }

    /// <summary>Reads live values that carry expiration metadata.</summary>
    [Benchmark(OperationsPerInvoke = ReadBatch)]
    public async Task ReadLiveExpiringValueBatchedAsync()
    {
        var cache = _squirix!;
        for (var i = 0; i < ReadBatch; i++)
            _consumer.Consume((await cache.GetValueAsync(_expiringKeys[i], CancellationToken.None).ConfigureAwait(false)).Value ?? string.Empty);
    }

    /// <summary>
    /// Reads known-missing keys through <see cref="ICache{T}.GetValueAsync" />.
    /// </summary>
    [Benchmark(OperationsPerInvoke = ReadBatch)]
    public async Task ReadMissingValueBatchedAsync()
    {
        var cache = _squirix!;
        for (var i = 0; i < ReadBatch; i++)
        {
            var result = await cache.GetValueAsync(_missingKeys[i], CancellationToken.None).ConfigureAwait(false);
            _consumer.Consume(result.Found);
        }
    }

    /// <summary>Starts benchmark dependencies and seeds baseline keys.</summary>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        SeedKeys();

        _node = await BenchmarkNodeScope.StartAsync(CancellationToken.None).ConfigureAwait(false);
        _client = await _node.OpenClientAsync(CancellationToken.None).ConfigureAwait(false);
        _squirix = await _client.Client.GetCacheAsync<string>(CacheName, CancellationToken.None).ConfigureAwait(false);

        await SeedBackendsAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Writes new unique keys through the public <c>SetAsync</c> API.
    /// </summary>
    [Benchmark(OperationsPerInvoke = WriteBatch)]
    public async Task WriteNewValueBatchedAsync()
    {
        var cache = _squirix!;
        var offset = Interlocked.Add(ref _writeOffset, WriteBatch);
        for (var i = 0; i < WriteBatch; i++)
            await cache.SetAsync($"write:{(offset + i).ToString("D10", CultureInfo.InvariantCulture)}", $"value:{i.ToString("D5", CultureInfo.InvariantCulture)}", cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    private static Task<string?> ColdFactoryAsync(string key, CancellationToken cancellationToken) =>
        throw new InvalidOperationException($"Factory must not be called for existing key '{key}'.");

    private static Task<string?> CreateValueAsync(string key, CancellationToken cancellationToken) => Task.FromResult<string?>($"created:{key}");

    private async Task SeedBackendsAsync()
    {
        var cache = _squirix!;

        for (var i = 0; i < KeyCount; i++)
        {
            await cache.SetAsync(_existingKeys[i], $"value:{i.ToString("D5", CultureInfo.InvariantCulture)}", cancellationToken: CancellationToken.None).ConfigureAwait(false);
            await cache.SetAsync(_expiringKeys[i], $"expiring:{i.ToString("D5", CultureInfo.InvariantCulture)}", new CacheEntryOptions { Expiration = TimeSpan.FromHours(1) }, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private void SeedKeys()
    {
        for (var i = 0; i < KeyCount; i++)
        {
            _existingKeys[i] = $"existing:{i.ToString("D5", CultureInfo.InvariantCulture)}";
            _missingKeys[i] = $"missing:{i.ToString("D5", CultureInfo.InvariantCulture)}";
            _expiringKeys[i] = $"expiring:{i.ToString("D5", CultureInfo.InvariantCulture)}";
        }
    }
}
