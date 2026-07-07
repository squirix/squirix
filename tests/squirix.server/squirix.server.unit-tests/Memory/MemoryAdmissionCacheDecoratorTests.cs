using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>
/// Unit tests for <see cref="MemoryAdmissionCacheDecorator{T}" /> local-owner accounting.
/// </summary>
public sealed class MemoryAdmissionCacheDecoratorTests : UnitTestBase
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

        Assert.True(await cache.TryAddEntryAsync(TestOperationIds.Default, CacheName, key, entry, DefaultCancellationToken));
        Assert.Equal(1, accounting.EntryCount);

        var results = await RunSynchronizedConcurrentlyAsync(
            ConcurrentRaceWidth,
            _ => cache.RemoveAsync(TestOperationIds.Default, CacheName, key, DefaultCancellationToken).AsTask(),
            DefaultCancellationToken);

        var removedCount = 0;
        foreach (var result in results)
        {
            if (result.Removed)
                removedCount++;
        }

        Assert.Equal(1, removedCount);
        Assert.Equal(0, accounting.EntryCount);
        Assert.Equal(0, accounting.EstimatedBytes);
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
        var entry = new CacheEntry<string>
        {
            Value = "v",
            Version = 1,
            ExpiresUtc = timeProvider.GetUtcNow().UtcDateTime.AddMinutes(10),
        };
        var expirationGrowth = EstimateExpirationMetadataDelta(estimator, keyValue, CreateEntry("v"));

        Assert.True(await cache.TryAddEntryAsync(TestOperationIds.Default, CacheName, key, entry, DefaultCancellationToken));
        var bytesWithExpiration = accounting.EstimatedBytes;

        Assert.True(await cache.RemoveExpirationAsync(TestOperationIds.Default, CacheName, key, DefaultCancellationToken));
        Assert.Equal(bytesWithExpiration - expirationGrowth, accounting.EstimatedBytes);
        Assert.Equal(1, accounting.EntryCount);
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

        await RunSynchronizedConcurrentVoidAsync(ConcurrentRaceWidth, _ => cache.SetEntryAsync(TestOperationIds.Default, CacheName, key, entry, DefaultCancellationToken).AsTask(), DefaultCancellationToken);

        Assert.Equal(1, accounting.EntryCount);
        Assert.Equal(expectedBytes, accounting.EstimatedBytes);
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

        Assert.True(await cache.TryAddEntryAsync(TestOperationIds.Default, CacheName, key, initial, DefaultCancellationToken));
        var bytesBeforeReplace = accounting.EstimatedBytes;
        var expectedDelta = EstimateEntryBytes(estimator, CacheName, key, replacement) - EstimateEntryBytes(estimator, CacheName, key, initial);

        await cache.SetEntryAsync(TestOperationIds.Default, CacheName, key, replacement, DefaultCancellationToken);

        Assert.Equal(1, accounting.EntryCount);
        Assert.Equal(bytesBeforeReplace + expectedDelta, accounting.EstimatedBytes);
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

        Assert.True(await cache.TryAddEntryAsync(TestOperationIds.Default, CacheName, key, entry, DefaultCancellationToken));
        var bytesBeforeTouch = accounting.EstimatedBytes;

        Assert.True(await cache.TouchAsync(TestOperationIds.Default, CacheName, key, TimeSpan.FromMinutes(5), DefaultCancellationToken));
        Assert.Equal(bytesBeforeTouch + expirationGrowth, accounting.EstimatedBytes);
        Assert.Equal(1, accounting.EntryCount);
    }

    /// <summary>Ensures TouchAsync does not change accounting when expiration metadata was already present.</summary>
    [Fact]
    public async Task TouchAsyncDoesNotChangeAccountingWhenExpirationMetadataAlreadyPresent()
    {
        const string key = "retouch-key";
        var timeProvider = new FakeTimeProvider();
        await using var physical = new PhysicalCache<string>(timeProvider);
        var (cache, _, accounting, _) = CreateLocalOwnerCache(Self, physical);
        var entry = new CacheEntry<string>
        {
            Value = "v",
            Version = 1,
            ExpiresUtc = timeProvider.GetUtcNow().UtcDateTime.AddMinutes(10),
        };

        Assert.True(await cache.TryAddEntryAsync(TestOperationIds.Default, CacheName, key, entry, DefaultCancellationToken));
        var bytesBeforeTouch = accounting.EstimatedBytes;

        Assert.True(await cache.TouchAsync(TestOperationIds.Default, CacheName, key, TimeSpan.FromMinutes(5), DefaultCancellationToken));
        Assert.Equal(bytesBeforeTouch, accounting.EstimatedBytes);
        Assert.Equal(1, accounting.EntryCount);
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
            _ => cache.TryAddEntryAsync(TestOperationIds.Default, CacheName, key, entry, DefaultCancellationToken).AsTask(),
            DefaultCancellationToken);

        var addedCount = 0;
        foreach (var added in results)
        {
            if (added)
                addedCount++;
        }

        Assert.Equal(1, addedCount);
        Assert.Equal(1, accounting.EntryCount);
        Assert.Equal(expectedBytes, accounting.EstimatedBytes);
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

        Assert.True(await cache.TryAddEntryAsync(TestOperationIds.Default, CacheName, key, initial, DefaultCancellationToken));
        var bytesBeforeUpdate = accounting.EstimatedBytes;
        const string updatedValue = "much-longer-value";
        var replacement = CreateEntry(updatedValue);
        var expectedDelta = EstimateEntryBytes(estimator, CacheName, key, replacement) - EstimateEntryBytes(estimator, CacheName, key, initial);

        Assert.True(await cache.UpdateAsync(TestOperationIds.Default, CacheName, key, updatedValue, DefaultCancellationToken));

        Assert.Equal(1, accounting.EntryCount);
        Assert.Equal(bytesBeforeUpdate + expectedDelta, accounting.EstimatedBytes);
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

        Assert.True(await cache.TryAddEntryAsync(TestOperationIds.Default, CacheName, key, initial, DefaultCancellationToken));
        var bytesBeforeUpdate = accounting.EstimatedBytes;
        var expectedDelta = EstimateEntryBytes(estimator, CacheName, key, replacement) - EstimateEntryBytes(estimator, CacheName, key, initial);

        var results = await RunSynchronizedConcurrentlyAsync(
            ConcurrentRaceWidth,
            _ => cache.UpdateAsync(TestOperationIds.Default, CacheName, key, updatedValue, DefaultCancellationToken).AsTask(),
            DefaultCancellationToken);

        Assert.All(results, Assert.True);
        Assert.Equal(1, accounting.EntryCount);
        Assert.Equal(bytesBeforeUpdate + expectedDelta, accounting.EstimatedBytes);
    }

    private static CacheEntry<string> CreateEntry(string value) => new() { Value = value, Version = 1 };

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

    private static long EstimateEntryBytes(CacheEntrySizeEstimator<string> estimator, string cacheName, string key, CacheEntry<string> entry) =>
        estimator.EstimateBytes(new CacheKey(cacheName, key), entry, false);

    private static long EstimateExpirationMetadataDelta(CacheEntrySizeEstimator<string> estimator, CacheKey keyValue, CacheEntry<string> entryWithoutExpiration)
    {
        var withoutExpiration = estimator.EstimateBytes(keyValue, entryWithoutExpiration, false);
        var withExpiration = estimator.EstimateBytes(
            keyValue,
            new CacheEntry<string>
            {
                Value = entryWithoutExpiration.Value,
                Version = entryWithoutExpiration.Version,
                Expiration = entryWithoutExpiration.Expiration,
                Tags = entryWithoutExpiration.Tags,
                ExpiresUtc = DateTime.UnixEpoch,
            },
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

    private sealed class FixedOwnerLocator(string owner) : INodeLocator
    {
        public string GetOwner(string cacheName, string key) => owner;
    }
}
