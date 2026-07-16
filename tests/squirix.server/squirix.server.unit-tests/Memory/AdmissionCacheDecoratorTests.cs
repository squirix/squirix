using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>
/// Unit tests for <see cref="MemoryAdmissionCacheDecorator{T}" /> local-owner accounting.
/// </summary>
public sealed class AdmissionCacheDecoratorTests : ServerUnitTestBase
{
    private const string CacheName = "orders";
    private const int ConcurrentRaceWidth = 64;
    private const string Self = "node-a";

    /// <summary>Ensures RemoveAsync accounts for one removed local-owner entry.</summary>
    [Fact]
    public async Task RemoveAsyncConcurrentRemoveAccountsSingleEntryForLocalOwnerKey()
    {
        const string key = "remove-race";
        await using var physical = new PhysicalCache<string>();
        var (cache, inner, accounting, _) = CreateLocalOwnerCache(Self, physical);
        var entry = CreateEntry("v");

        Assert.True(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, entry, DefaultCancellationToken));
        Assert.Equal(1, accounting.ReadEntryCount());

        var results = await RunSynchronizedConcurrentlyAsync(
            ConcurrentRaceWidth,
            _ => cache.RemoveAsync(UnitMutationOpIds.Default, CacheName, key, DefaultCancellationToken).AsTask(),
            DefaultCancellationToken);

        var removedCount = 0;
        foreach (var result in results)
        {
            if (result.Removed)
                removedCount++;
        }

        Assert.Equal(1, removedCount);
        Assert.Equal(0, accounting.ReadEntryCount());
        Assert.Equal(0, accounting.ReadEstimatedBytes());
        Assert.False(await KeyExistsAsync(inner, CacheName, key, DefaultCancellationToken));
    }

    /// <summary>Ensures RemoveExpirationAsync accounts for removed expiration metadata on a local-owner entry.</summary>
    [Fact]
    public async Task RemoveExpirationAsyncAccountsExpirationMetadataShrinkForLocalOwnerKey()
    {
        const string key = "remove-expiration-key";
        var timeProvider = new FakeTimeProvider();
        await using var physical = new PhysicalCache<string>(timeProvider);
        var (cache, _, accounting, estimator) = CreateLocalOwnerCache(Self, physical);
        var keyValue = new CacheKey(CacheName, key);
        var entry = new NodeCacheEntry<string>
        {
            Value = "v",
            Version = 1,
            ExpiresUtc = timeProvider.GetUtcNow().UtcDateTime.AddMinutes(10),
        };
        var expirationGrowth = EstimateExpirationMetadataDelta(estimator, keyValue, CreateEntry("v"));

        Assert.True(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, entry, DefaultCancellationToken));
        var bytesWithExpiration = accounting.ReadEstimatedBytes();

        Assert.True(await cache.RemoveExpirationAsync(UnitMutationOpIds.Default, CacheName, key, DefaultCancellationToken));
        Assert.Equal(bytesWithExpiration - expirationGrowth, accounting.ReadEstimatedBytes());
        Assert.Equal(1, accounting.ReadEntryCount());
    }

    /// <summary>Ensures concurrent local-owner SetAsync misses account memory for one physical entry only.</summary>
    [Fact]
    public async Task SetAsyncConcurrentMissAccountsSingleEntryForLocalOwnerKey()
    {
        const string key = "set-race";
        await using var physical = new PhysicalCache<string>();
        var (cache, inner, accounting, estimator) = CreateLocalOwnerCache(Self, physical);
        var entry = CreateEntry("v");
        var expectedBytes = EstimateEntryBytes(estimator, CacheName, key, entry);

        await RunSynchronizedConcurrentVoidAsync(ConcurrentRaceWidth, _ => cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, key, entry, DefaultCancellationToken).AsTask(), DefaultCancellationToken);

        Assert.Equal(1, accounting.ReadEntryCount());
        Assert.Equal(expectedBytes, accounting.ReadEstimatedBytes());
        Assert.True(await KeyExistsAsync(inner, CacheName, key, DefaultCancellationToken));
    }

    /// <summary>Ensures SetAsync replace accounts for value-size growth on a local-owner entry.</summary>
    [Fact]
    public async Task SetAsyncReplaceAccountsValueSizeDeltaForLocalOwnerKey()
    {
        const string key = "set-replace";
        await using var physical = new PhysicalCache<string>();
        var (cache, _, accounting, estimator) = CreateLocalOwnerCache(Self, physical);
        var initial = CreateEntry("a");
        var replacement = CreateEntry("much-longer-value");

        Assert.True(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, initial, DefaultCancellationToken));
        var bytesBeforeReplace = accounting.ReadEstimatedBytes();
        var expectedDelta = EstimateEntryBytes(estimator, CacheName, key, replacement) - EstimateEntryBytes(estimator, CacheName, key, initial);

        await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, key, replacement, DefaultCancellationToken);

        Assert.Equal(1, accounting.ReadEntryCount());
        Assert.Equal(bytesBeforeReplace + expectedDelta, accounting.ReadEstimatedBytes());
    }

    /// <summary>Ensures TouchAsync accounts for added expiration metadata on a previously non-expiring entry.</summary>
    [Fact]
    public async Task TouchAsyncAccountsExpirationMetadataGrowthForLocalOwnerKey()
    {
        const string key = "touch-key";
        var timeProvider = new FakeTimeProvider();
        await using var physical = new PhysicalCache<string>(timeProvider);
        var (cache, _, accounting, estimator) = CreateLocalOwnerCache(Self, physical);
        var keyValue = new CacheKey(CacheName, key);
        var entry = CreateEntry("v");
        var expirationGrowth = EstimateExpirationMetadataDelta(estimator, keyValue, entry);

        Assert.True(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, entry, DefaultCancellationToken));
        var bytesBeforeTouch = accounting.ReadEstimatedBytes();

        Assert.True(await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, key, TimeSpan.FromMinutes(5), DefaultCancellationToken));
        Assert.Equal(bytesBeforeTouch + expirationGrowth, accounting.ReadEstimatedBytes());
        Assert.Equal(1, accounting.ReadEntryCount());
    }

    /// <summary>Ensures TouchAsync does not change accounting when expiration metadata was already present.</summary>
    [Fact]
    public async Task TouchAsyncDoesNotChangeAccountingWhenExpirationMetadataAlreadyPresent()
    {
        const string key = "retouch-key";
        var timeProvider = new FakeTimeProvider();
        await using var physical = new PhysicalCache<string>(timeProvider);
        var (cache, _, accounting, _) = CreateLocalOwnerCache(Self, physical);
        var entry = new NodeCacheEntry<string>
        {
            Value = "v",
            Version = 1,
            ExpiresUtc = timeProvider.GetUtcNow().UtcDateTime.AddMinutes(10),
        };

        Assert.True(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, entry, DefaultCancellationToken));
        var bytesBeforeTouch = accounting.ReadEstimatedBytes();

        Assert.True(await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, key, TimeSpan.FromMinutes(5), DefaultCancellationToken));
        Assert.Equal(bytesBeforeTouch, accounting.ReadEstimatedBytes());
        Assert.Equal(1, accounting.ReadEntryCount());
    }

    /// <summary>Ensures concurrent local-owner TryAddAsync misses account memory for one physical entry only.</summary>
    [Fact]
    public async Task TryAddAsyncConcurrentMissAccountsSingleEntryForLocalOwnerKey()
    {
        const string key = "try-add-race";
        await using var physical = new PhysicalCache<string>();
        var (cache, inner, accounting, estimator) = CreateLocalOwnerCache(Self, physical);
        var entry = CreateEntry("v");
        var expectedBytes = EstimateEntryBytes(estimator, CacheName, key, entry);

        var results = await RunSynchronizedConcurrentlyAsync(
            ConcurrentRaceWidth,
            _ => cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, entry, DefaultCancellationToken).AsTask(),
            DefaultCancellationToken);

        var addedCount = 0;
        foreach (var added in results)
        {
            if (added)
                addedCount++;
        }

        Assert.Equal(1, addedCount);
        Assert.Equal(1, accounting.ReadEntryCount());
        Assert.Equal(expectedBytes, accounting.ReadEstimatedBytes());
        Assert.True(await KeyExistsAsync(inner, CacheName, key, DefaultCancellationToken));
    }

    /// <summary>Ensures UpdateAsync accounts for value-size growth on a local-owner entry.</summary>
    [Fact]
    public async Task UpdateAsyncAccountsValueSizeDeltaForLocalOwnerKey()
    {
        const string key = "update-replace";
        await using var physical = new PhysicalCache<string>();
        var (cache, _, accounting, estimator) = CreateLocalOwnerCache(Self, physical);
        var initial = CreateEntry("a");

        Assert.True(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, initial, DefaultCancellationToken));
        var bytesBeforeUpdate = accounting.ReadEstimatedBytes();
        const string updatedValue = "much-longer-value";
        var replacement = CreateEntry(updatedValue);
        var expectedDelta = EstimateEntryBytes(estimator, CacheName, key, replacement) - EstimateEntryBytes(estimator, CacheName, key, initial);

        Assert.True(await cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, key, updatedValue, DefaultCancellationToken));

        Assert.Equal(1, accounting.ReadEntryCount());
        Assert.Equal(bytesBeforeUpdate + expectedDelta, accounting.ReadEstimatedBytes());
    }

    /// <summary>Ensures concurrent local-owner UpdateAsync applies to replace accounting once for one physical entry.</summary>
    [Fact]
    public async Task UpdateAsyncConcurrentReplaceAccountsSingleReplaceForLocalOwnerKey()
    {
        const string key = "update-race";
        await using var physical = new PhysicalCache<string>();
        var (cache, _, accounting, estimator) = CreateLocalOwnerCache(Self, physical);
        var initial = CreateEntry("a");
        const string updatedValue = "much-longer-value";
        var replacement = CreateEntry(updatedValue);

        Assert.True(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, initial, DefaultCancellationToken));
        var bytesBeforeUpdate = accounting.ReadEstimatedBytes();
        var expectedDelta = EstimateEntryBytes(estimator, CacheName, key, replacement) - EstimateEntryBytes(estimator, CacheName, key, initial);

        var results = await RunSynchronizedConcurrentlyAsync(
            ConcurrentRaceWidth,
            _ => cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, key, updatedValue, DefaultCancellationToken).AsTask(),
            DefaultCancellationToken);

        Assert.All(results, Assert.True);
        Assert.Equal(1, accounting.ReadEntryCount());
        Assert.Equal(bytesBeforeUpdate + expectedDelta, accounting.ReadEstimatedBytes());
    }

    private static NodeCacheEntry<string> CreateEntry(string value) => new() { Value = value, Version = 1 };

    private static async Task<bool> KeyExistsAsync(ClientCache<string> cache, string cacheName, string key, CancellationToken cancellationToken) =>
        (await cache.GetValueAsync(cacheName, key, cancellationToken).ConfigureAwait(false)).Found;

    private static (MemoryAdmissionCacheDecorator<string> Cache, ClientCache<string> Inner, MemoryUsageAccounting Accounting, CacheEntrySizeEstimator<string> Estimator)
        CreateLocalOwnerCache(string self, PhysicalCache<string> physical)
    {
        var inner = new ClientCache<string>(physical, physical);
        var accounting = new MemoryUsageAccounting();
        var estimator = new CacheEntrySizeEstimator<string>();
        var gate = CreatePermissiveGate(accounting, self);
        var cache = new MemoryAdmissionCacheDecorator<string>(inner, gate, estimator, accounting, new FixedOwnerLocator(self), self);
        return (cache, inner, accounting, estimator);
    }

    private static PressureGate CreatePermissiveGate(IMemoryUsageAccounting accounting, string nodeId)
    {
        var options = Microsoft.Extensions.Options.Options.Create(
            new PressureOptions
            {
                MaxEstimatedCacheBytes = 10_000_000_000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });
        return new PressureGate(new StateEvaluator(options), accounting, nodeId);
    }

    private static long EstimateEntryBytes(CacheEntrySizeEstimator<string> estimator, string cacheName, string key, NodeCacheEntry<string> entry) =>
        estimator.EstimateBytes(new CacheKey(cacheName, key), entry, false);

    private static long EstimateExpirationMetadataDelta(CacheEntrySizeEstimator<string> estimator, CacheKey keyValue, NodeCacheEntry<string> entryWithoutExpiration)
    {
        var withoutExpiration = estimator.EstimateBytes(keyValue, entryWithoutExpiration, false);
        var withExpiration = estimator.EstimateBytes(
            keyValue,
            new NodeCacheEntry<string>(
                entryWithoutExpiration.Value,
                entryWithoutExpiration.Version,
                DateTime.UnixEpoch,
                entryWithoutExpiration.Expiration,
                entryWithoutExpiration.Tags),
            false);
        return withExpiration - withoutExpiration;
    }

    private static async Task<T[]> RunSynchronizedConcurrentlyAsync<T>(int concurrency, Func<int, Task<T>> operation, CancellationToken cancellationToken)
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = new Task<T>[concurrency];
        for (var i = 0; i < concurrency; i++)
        {
            tasks[i] = RunAfterGateAsync(i);
        }

        await Task.Delay(50, cancellationToken).ConfigureAwait(false);
        _ = gate.TrySetResult();
        return await Task.WhenAll(tasks).ConfigureAwait(false);

        async Task<T> RunAfterGateAsync(int index)
        {
            await gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return await operation(index).ConfigureAwait(false);
        }
    }

    private static async Task RunSynchronizedConcurrentVoidAsync(int concurrency, Func<int, Task> operation, CancellationToken cancellationToken)
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = new Task[concurrency];
        for (var i = 0; i < concurrency; i++)
        {
            tasks[i] = RunAfterGateAsync(i);
        }

        await Task.Delay(50, cancellationToken).ConfigureAwait(false);
        _ = gate.TrySetResult();
        await Task.WhenAll(tasks).ConfigureAwait(false);
        return;

        async Task RunAfterGateAsync(int index)
        {
            await gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            await operation(index).ConfigureAwait(false);
        }
    }

    private sealed class FixedOwnerLocator : INodeLocator
    {
        private readonly string _owner;

        internal FixedOwnerLocator(string owner) => _owner = owner;

        string INodeLocator.GetOwner(string cacheName, string key) => _owner;
    }
}
