using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Xunit;

namespace Squirix.Server.UnitTests.Node.App;

/// <summary>
/// Unit tests for <see cref="MemoryAdmissionCacheDecorator{T}" /> local-owner accounting.
/// </summary>
public sealed class MemoryAdmissionCacheDecoratorTests : ServerUnitTestBase
{
    /// <summary>
    /// Ensures concurrent local-owner GetOrAddAsync misses account memory for one physical entry only.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task GetOrAddAsyncConcurrentMissAccountsSingleEntryForLocalOwnerKey()
    {
        const string self = "node-a";
        const string cacheName = "orders";
        const string key = "race-key";

        await using var physical = new PhysicalCache<string>();
        var (cache, inner, accounting, estimator) = CreateLocalOwnerCache(self, physical);
        var entry = new CacheEntry<string> { Value = "v", Version = 1 };
        var expectedBytes = estimator.EstimateBytes(new CacheKey(cacheName, key), entry, payloadIsCounter: false);

        const int concurrency = 32;
        var results = await Task.WhenAll(
            Enumerable.Range(0, concurrency)
                .Select(_ => cache.GetOrAddAsync(cacheName, key, entry, DefaultCancellationToken).AsTask()));

        Assert.All(results, result =>
        {
            Assert.True(result.Found);
            Assert.Equal("v", result.Value);
        });
        Assert.Equal(1, accounting.EntryCount);
        Assert.Equal(expectedBytes, accounting.EstimatedBytes);
        Assert.True(await inner.ContainsAsync(cacheName, key, DefaultCancellationToken));
    }

    /// <summary>
    /// Ensures concurrent local-owner SetAsync misses account memory for one physical entry only.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task SetAsyncConcurrentMissAccountsSingleEntryForLocalOwnerKey()
    {
        const string self = "node-a";
        const string cacheName = "orders";
        const string key = "set-race-key";

        await using var physical = new PhysicalCache<string>();
        var (cache, inner, accounting, estimator) = CreateLocalOwnerCache(self, physical);
        var entry = new CacheEntry<string> { Value = "v", Version = 1 };
        var expectedBytes = estimator.EstimateBytes(new CacheKey(cacheName, key), entry, payloadIsCounter: false);

        const int concurrency = 32;
        await Task.WhenAll(
            Enumerable.Range(0, concurrency)
                .Select(_ => cache.SetAsync(cacheName, key, entry, DefaultCancellationToken).AsTask()));

        Assert.Equal(1, accounting.EntryCount);
        Assert.Equal(expectedBytes, accounting.EstimatedBytes);
        Assert.True(await inner.ContainsAsync(cacheName, key, DefaultCancellationToken));
        var stored = await inner.GetEntryAsync(cacheName, key, DefaultCancellationToken);
        Assert.NotNull(stored);
        Assert.Equal("v", stored.Value);
    }

    private static (
        MemoryAdmissionCacheDecorator<string> Cache,
        ClientCache<string> Inner,
        MemoryUsageAccounting Accounting,
        CacheEntrySizeEstimator<string> Estimator) CreateLocalOwnerCache(string self, PhysicalCache<string> physical)
    {
        var inner = new ClientCache<string>(physical, physical);
        var accounting = new MemoryUsageAccounting();
        var estimator = new CacheEntrySizeEstimator<string>();
        var gate = CreatePermissiveGate(accounting, self);
        var cache = new MemoryAdmissionCacheDecorator<string>(inner, gate, estimator, accounting, self, new FixedOwnerLocator(self));
        return (cache, inner, accounting, estimator);
    }

    private static MemoryPressureGate CreatePermissiveGate(IMemoryUsageAccounting accounting, string nodeId)
    {
        var options = Options.Create(
            new MemoryPressureOptions
            {
                MaxEstimatedCacheBytes = 10_000_000_000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });
        return new MemoryPressureGate(new MemoryPressureStateEvaluator(options), accounting, nodeId);
    }

    private sealed class FixedOwnerLocator(string owner) : INodeLocator
    {
        public string GetOwner(string cacheName, string key) => owner;
    }
}
