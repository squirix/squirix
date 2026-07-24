using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Squirix.E2EBenchmarks.Scenarios;
using Squirix.E2EBenchmarks.Support.Client;
using Squirix.E2EBenchmarks.Support.Cluster;

namespace Squirix.E2EBenchmarks.Cache;

/// <summary>Shared single-node E2E allocation harness covering every <see cref="ICache{T}" /> API surface.</summary>
/// <typeparam name="T">The cache value type measured by the derived benchmark class.</typeparam>
[MemoryDiagnoser]
[MinIterationTime(150)]
public abstract class WireAllocBenchmarkBase<T>
{
    private const int Batch = 512;
    private const int KeyCount = 512;
    private readonly string[] _expiringKeys = new string[KeyCount];
    private readonly string[] _hitKeys = new string[KeyCount];

    private readonly TimeSpan _longExpiration = TimeSpan.FromHours(1);

    private E2EBenchmarkClientLease? _client;

    private GetOrAddMissFactory? _getOrAddMissFactory;
    private int _getOrAddMissOffset;
    private E2EBenchmarkNodeScope? _node;
    private int _removeExpirationOffset;
    private int _removeOffset;
    private int _uniqueKeyOffset;

    /// <summary>Gets or sets the durability mode measured by the current BenchmarkDotNet case.</summary>
    [Params(E2EBenchmarkDurabilityMode.Ephemeral, E2EBenchmarkDurabilityMode.Persistence)]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global", Justification = "A property annotated with [Params] must have a public setter")]
    [SuppressMessage("ReSharper", "AutoPropertyCanBeMadeGetOnly.Global", Justification = "A property annotated with [Params] must have a public setter")]
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global", Justification = "A property annotated with [Params] must have a public setter")]
    public E2EBenchmarkDurabilityMode DurabilityMode { get; set; }

    /// <summary>Gets the consumer used to prevent dead-code elimination.</summary>
    private protected Consumer Consumer { get; } = new();

    private ICache<T>? Cache { get; set; }

    /// <summary>Stores a new value for a unique key via <see cref="ICache{T}.AddAsync" />.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task AddAsync()
    {
        var cache = Cache!;
        var offset = Interlocked.Add(ref _uniqueKeyOffset, Batch);
        for (var i = 0; i < Batch; i++)
            await cache.AddAsync(Keys.FormatUnique(offset + i), CreateValue(offset + i), cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Stops benchmark dependencies.</summary>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_client is not null)
            await _client.DisposeAsync().ConfigureAwait(false);

        if (_node is not null)
            await _node.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Reads a pre-seeded entry via <see cref="ICache{T}.GetEntryAsync" />.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task GetEntryAsync()
    {
        var cache = Cache!;
        for (var i = 0; i < Batch; i++)
        {
            var result = await cache.GetEntryAsync(_hitKeys[i], CancellationToken.None).ConfigureAwait(false);
            if (result.Found)
                ConsumeValue(result.Value);
        }
    }

    /// <summary>Reads expiration metadata for a pre-seeded expiring entry.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task GetExpirationAsync()
    {
        var cache = Cache!;
        for (var i = 0; i < Batch; i++)
        {
            var result = await cache.GetExpirationAsync(_expiringKeys[i], CancellationToken.None).ConfigureAwait(false);
            Consumer.Consume(result.Found);
        }
    }

    /// <summary>Gets an existing value via <see cref="ICache{T}.GetOrAddAsync" /> without invoking the factory.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task GetOrAddAsyncHitAsync()
    {
        var cache = Cache!;
        for (var i = 0; i < Batch; i++)
        {
            var result = await cache.GetOrAddAsync(_hitKeys[i], Keys.GetOrAddHitFactoryAsync, cancellationToken: CancellationToken.None).ConfigureAwait(false);
            ConsumeValue(result.Value);
        }
    }

    /// <summary>Creates a missing value via <see cref="ICache{T}.GetOrAddAsync" />.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task GetOrAddAsyncMissAsync()
    {
        var cache = Cache!;
        var offset = Interlocked.Add(ref _getOrAddMissOffset, Batch);
        for (var i = 0; i < Batch; i++)
        {
            var factory = _getOrAddMissFactory ??= new GetOrAddMissFactory(CreateValue);
            factory.ValueIndex = offset + i;
            var result = await cache.GetOrAddAsync(Keys.FormatUnique(offset + i), factory.ValueFactory, cancellationToken: CancellationToken.None).ConfigureAwait(false);
            ConsumeValue(result.Value);
        }
    }

    /// <summary>Reads a pre-seeded value via <see cref="ICache{T}.GetValueAsync" />.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task GetValueAsync()
    {
        var cache = Cache!;
        for (var i = 0; i < Batch; i++)
        {
            var result = await cache.GetValueAsync(_hitKeys[i], CancellationToken.None).ConfigureAwait(false);
            ConsumeValue(result.Value);
        }
    }

    /// <summary>Removes a pre-seeded entry via <see cref="ICache{T}.RemoveAsync" />.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task RemoveAsync()
    {
        var cache = Cache!;
        for (var i = 0; i < Batch; i++)
            Consumer.Consume(await cache.RemoveAsync(_hitKeys[i], CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>Removes expiration metadata from pre-seeded expiring entries.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task RemoveExpirationAsync()
    {
        var cache = Cache!;
        for (var i = 0; i < Batch; i++)
            Consumer.Consume(await cache.RemoveExpirationAsync(_expiringKeys[i], CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>Creates or overwrites values via <see cref="ICache{T}.SetAsync" />.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task SetAsync()
    {
        var cache = Cache!;
        var offset = Interlocked.Add(ref _uniqueKeyOffset, Batch);
        for (var i = 0; i < Batch; i++)
            await cache.SetAsync(Keys.FormatUnique(offset + i), CreateValue(offset + i), cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Starts a single-node server, opens a typed cache session, and seeds hit and expiring keys.</summary>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        for (var i = 0; i < KeyCount; i++)
        {
            _hitKeys[i] = $"hit:{i.ToString("D5", CultureInfo.InvariantCulture)}";
            _expiringKeys[i] = $"exp:{i.ToString("D5", CultureInfo.InvariantCulture)}";
        }

        _node = await E2EBenchmarkNodeScope.StartAsync(CancellationToken.None, DurabilityMode).ConfigureAwait(false);
        _client = await _node.OpenClientAsync(CancellationToken.None).ConfigureAwait(false);
        Cache = await _client.Client.GetCacheAsync<T>(GetCacheName(), CancellationToken.None).ConfigureAwait(false);

        var cache = Cache;
        for (var i = 0; i < KeyCount; i++)
        {
            await cache.SetAsync(_hitKeys[i], CreateValue(i), cancellationToken: CancellationToken.None).ConfigureAwait(false);
            await cache.SetAsync(_expiringKeys[i], CreateValue(i), new CacheEntryOptions { Expiration = _longExpiration }, CancellationToken.None).ConfigureAwait(false);
        }
    }

    /// <summary>Updates expiration using an absolute timestamp.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task TouchAbsoluteAsync()
    {
        var cache = Cache!;
        var expiresAt = DateTimeOffset.UtcNow.AddHours(1);
        for (var i = 0; i < Batch; i++)
            Consumer.Consume(await cache.TouchAsync(_hitKeys[i], expiresAt, CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>Updates expiration using a relative duration.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task TouchRelativeAsync()
    {
        var cache = Cache!;
        for (var i = 0; i < Batch; i++)
            Consumer.Consume(await cache.TouchAsync(_hitKeys[i], _longExpiration, CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>Attempts to add a new value for a unique key via <see cref="ICache{T}.TryAddAsync" />.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task TryAddAsync()
    {
        var cache = Cache!;
        var offset = Interlocked.Add(ref _uniqueKeyOffset, Batch);
        for (var i = 0; i < Batch; i++)
            Consumer.Consume(await cache.TryAddAsync(Keys.FormatUnique(offset + i), CreateValue(offset + i), cancellationToken: CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>Updates a pre-seeded value via <see cref="ICache{T}.UpdateAsync" />.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task UpdateAsync()
    {
        var cache = Cache!;
        for (var i = 0; i < Batch; i++)
            Consumer.Consume(await cache.UpdateAsync(_hitKeys[i], CreateValue(i), CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>Consumes a value so BenchmarkDotNet does not eliminate the read path.</summary>
    /// <param name="value">The value returned from the cache.</param>
    protected abstract void ConsumeValue(T? value);

    /// <summary>Creates a deterministic value for the given index.</summary>
    /// <param name="index">The value index.</param>
    /// <returns>A value for the active benchmark shape.</returns>
    protected abstract T CreateValue(int index);

    /// <summary>Gets the cache name used by the derived benchmark class.</summary>
    /// <returns>The cache name opened during global setup.</returns>
    protected abstract string GetCacheName();

    /// <summary>Re-seeds expiring entries outside the measured body for <see cref="RemoveExpirationAsync" />.</summary>
    protected async Task SeedRemoveExpirationIterationCoreAsync()
    {
        var cache = Cache!;
        var offset = Interlocked.Add(ref _removeExpirationOffset, Batch);
        for (var i = 0; i < Batch; i++)
            await cache.SetAsync(_expiringKeys[i], CreateValue(offset + i), new CacheEntryOptions { Expiration = _longExpiration }, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Re-seeds hit keys outside the measured body for <see cref="RemoveAsync" />.</summary>
    protected async Task SeedRemoveIterationCoreAsync()
    {
        var cache = Cache!;
        var offset = Interlocked.Add(ref _removeOffset, Batch);
        for (var i = 0; i < Batch; i++)
            await cache.SetAsync(_hitKeys[i], CreateValue(offset + i), cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    private static class Keys
    {
        internal static string FormatUnique(int index) => $"unique:{index.ToString("D8", CultureInfo.InvariantCulture)}";

        internal static Task<T?> GetOrAddHitFactoryAsync(string key, CancellationToken cancellationToken)
        {
            _ = key;
            _ = cancellationToken;
            return Task.FromResult<T?>(default);
        }
    }

    private sealed class GetOrAddMissFactory
    {
        private readonly Func<int, T> _valueFactory;

        internal GetOrAddMissFactory(Func<int, T> valueFactory)
        {
            _valueFactory = valueFactory;
            ValueFactory = CreateValueAsync;
        }

        internal Func<string, CancellationToken, Task<T?>> ValueFactory { get; }

        internal int ValueIndex { private get; set; }

        private Task<T?> CreateValueAsync(string key, CancellationToken cancellationToken)
        {
            _ = key;
            _ = cancellationToken;
            return Task.FromResult<T?>(_valueFactory(ValueIndex));
        }
    }
}
