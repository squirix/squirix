using System;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Endpoint;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of validating a cache key and of resolving the routed cache API for an inbound request by cache name.</summary>
[MemoryDiagnoser]
[ShortRunJob]
public class KeyValidationBenchmarks
{
    private const string CacheName = "orders-cache";
    private string _key = string.Empty;
    private InboundEndpointCacheOperations<object?>? _operations;

    /// <summary>Gets or sets the key length in characters.</summary>
    [Params(16, 128, 1024)]
    public int KeyLength { get; set; }

    /// <summary>Prepares the key and warms the cache API for <see cref="CacheName" />.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _key = new string('k', KeyLength);
        var physical = new PhysicalCache<object?>(TimeProvider.System, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        _operations = new InboundEndpointCacheOperations<object?>(new ClientCache<object?>(physical, physical));
        _ = _operations.ForCache(CacheName);
    }

    /// <summary>Validates the key.</summary>
    /// <returns>Whether the key is valid.</returns>
    [Benchmark]
    public bool ValidateKey() => CacheKeyValidator.TryValidate(_key, out _);

    /// <summary>Resolves the cache API for a name that was resolved before.</summary>
    /// <returns>The routed cache API.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark]
    public object ForCacheRepeatedName() => ThrowHelper.Required(_operations, "Benchmark was not initialized.").ForCache(CacheName);
}
