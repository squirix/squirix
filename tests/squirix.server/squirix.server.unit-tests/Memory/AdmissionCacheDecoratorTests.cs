using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>
/// Unit tests for <see cref="MemoryAdmissionCacheDecorator{T}" /> local-owner accounting.
/// </summary>
[Immutable]
public sealed class AdmissionCacheDecoratorTests : DisposableServerUnitTestBase
{
    private const string CacheName = "orders";
    private const int ConcurrentRaceWidth = 64;
    private const string Self = "node-a";

    private readonly Meter _testMeter = new("test");

    /// <summary>Ensures RemoveAsync accounts for one removed local-owner entry.</summary>
    [Fact]
    public async Task ConcurrentRemoveDeletesLocalKeyOnce()
    {
        const string key = "remove-race";
        var physical = new PhysicalCache<string>();
        var (cache, inner, accounting, _) = CreateLocalOwnerCache(Self, physical, _testMeter);
        var entry = CreateEntry("v");

        Assert.True(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, entry, DefaultCancellationToken));
        Assert.Equal(1, accounting.ReadEntryCount());

        var remove = new ConcurrentCacheOp(cache, key);
        var results = await RunSynchronizedConcurrentlyAsync(ConcurrentRaceWidth, remove.Remove, DefaultCancellationToken);

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

    /// <summary>Ensures concurrent local-owner SetAsync misses account memory for one physical entry only.</summary>
    [Fact]
    public async Task ConcurrentSetMissAccountsOneEntry()
    {
        const string key = "set-race";
        var physical = new PhysicalCache<string>();
        var (cache, inner, accounting, estimator) = CreateLocalOwnerCache(Self, physical, _testMeter);
        var entry = CreateEntry("v");
        var expectedBytes = EstimateEntryBytes(estimator, CacheName, key, entry);

        var set = new ConcurrentCacheOp(cache, key, entry);
        await RunSynchronizedConcurrentVoidAsync(ConcurrentRaceWidth, set.SetEntry, DefaultCancellationToken);

        Assert.Equal(1, accounting.ReadEntryCount());
        Assert.Equal(expectedBytes, accounting.ReadEstimatedBytes());
        Assert.True(await KeyExistsAsync(inner, CacheName, key, DefaultCancellationToken));
    }

    /// <summary>Ensures concurrent local-owner TryAddAsync misses account memory for one physical entry only.</summary>
    [Fact]
    public async Task ConcurrentTryAddMissAddsSingleEntry()
    {
        const string key = "try-add-race";
        var physical = new PhysicalCache<string>();
        var (cache, inner, accounting, estimator) = CreateLocalOwnerCache(Self, physical, _testMeter);
        var entry = CreateEntry("v");
        var expectedBytes = EstimateEntryBytes(estimator, CacheName, key, entry);

        var tryAdd = new ConcurrentCacheOp(cache, key, entry);
        var results = await RunSynchronizedConcurrentlyAsync(ConcurrentRaceWidth, tryAdd.TryAddEntry, DefaultCancellationToken);

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

    /// <summary>Ensures concurrent local-owner UpdateAsync applies to replace accounting once for one physical entry.</summary>
    [Fact]
    public async Task ConcurrentUpdateReplacesLocalKeyOnce()
    {
        const string key = "update-race";
        var physical = new PhysicalCache<string>();
        var (cache, _, accounting, estimator) = CreateLocalOwnerCache(Self, physical, _testMeter);
        var initial = CreateEntry("a");
        const string updatedValue = "much-longer-value";
        var replacement = CreateEntry(updatedValue);

        Assert.True(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, initial, DefaultCancellationToken));
        var bytesBeforeUpdate = accounting.ReadEstimatedBytes();
        var expectedDelta = EstimateEntryBytes(estimator, CacheName, key, replacement) - EstimateEntryBytes(estimator, CacheName, key, initial);

        var update = new ConcurrentCacheOp(cache, key, updatedValue: updatedValue);
        var results = await RunSynchronizedConcurrentlyAsync(ConcurrentRaceWidth, update.Update, DefaultCancellationToken);

        Assert.All(results, Assert.True);
        Assert.Equal(1, accounting.ReadEntryCount());
        Assert.Equal(bytesBeforeUpdate + expectedDelta, accounting.ReadEstimatedBytes());
    }

    /// <summary>Ensures RemoveExpirationAsync accounts for removed expiration metadata on a local-owner entry.</summary>
    [Fact]
    public async Task RemoveExpiryAccountsShrinkForLocalKey()
    {
        const string key = "remove-expiration-key";
        var timeProvider = new FakeTimeProvider();
        var physical = new PhysicalCache<string>(timeProvider);
        var (cache, _, accounting, estimator) = CreateLocalOwnerCache(Self, physical, _testMeter);
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

    /// <summary>Ensures SetAsync replace accounts for value-size growth on a local-owner entry.</summary>
    [Fact]
    public async Task SetReplaceAccountsValueDeltaForLocalKey()
    {
        const string key = "set-replace";
        var physical = new PhysicalCache<string>();
        var (cache, _, accounting, estimator) = CreateLocalOwnerCache(Self, physical, _testMeter);
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
    public async Task TouchAccountsExpiryGrowthForLocalKey()
    {
        const string key = "touch-key";
        var timeProvider = new FakeTimeProvider();
        var physical = new PhysicalCache<string>(timeProvider);
        var (cache, _, accounting, estimator) = CreateLocalOwnerCache(Self, physical, _testMeter);
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
    public async Task TouchChangesExpiryWhenMetadataPresent()
    {
        const string key = "retouch-key";
        var timeProvider = new FakeTimeProvider();
        var physical = new PhysicalCache<string>(timeProvider);
        var (cache, _, accounting, _) = CreateLocalOwnerCache(Self, physical, _testMeter);
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

    /// <summary>Ensures RemoveAsync subtracts recorded bytes rather than a stale pre-remove snapshot.</summary>
    [Fact]
    public async Task RemoveUsesRecordedBytesNotStale()
    {
        const string key = "remove-stale-snapshot";
        var small = CreateEntry("a");
        var large = CreateEntry("much-longer-value");
        var inner = new ScriptedRemoveInner();
        var accounting = new MemoryUsageAccounting();
        var estimator = new CacheEntrySizeEstimator<string>();
        var gate = CreatePermissiveGate(accounting, Self, _testMeter);
        var cache = new MemoryAdmissionCacheDecorator<string>(inner, gate, estimator, accounting, new FixedOwnerLocator(Self), Self);

        inner.GetResult = null;
        inner.TryAddResult = true;
        Assert.True(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, small, DefaultCancellationToken));

        inner.GetResult = small;
        await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, key, large, DefaultCancellationToken);
        Assert.Equal(1, accounting.ReadEntryCount());

        inner.GetResult = small;
        inner.RemoveResult = true;
        var result = await cache.RemoveAsync(UnitMutationOpIds.Default, CacheName, key, DefaultCancellationToken);

        Assert.True(result.Removed);
        Assert.Equal(0, accounting.ReadEntryCount());
        Assert.Equal(0, accounting.ReadEstimatedBytes());
    }

    /// <summary>Ensures UpdateAsync accounts for value-size growth on a local-owner entry.</summary>
    [Fact]
    public async Task UpdateAccountsValueDeltaForLocalKey()
    {
        const string key = "update-replace";
        var physical = new PhysicalCache<string>();
        var (cache, _, accounting, estimator) = CreateLocalOwnerCache(Self, physical, _testMeter);
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

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static NodeCacheEntry<string> CreateEntry(string value) => new() { Value = value, Version = 1 };

    private static (MemoryAdmissionCacheDecorator<string> Cache, ClientCache<string> Inner, MemoryUsageAccounting Accounting, CacheEntrySizeEstimator<string> Estimator)
        CreateLocalOwnerCache(string self, PhysicalCache<string> physical, Meter meter)
    {
        var inner = new ClientCache<string>(physical, physical);
        var accounting = new MemoryUsageAccounting();
        var estimator = new CacheEntrySizeEstimator<string>();
        var gate = CreatePermissiveGate(accounting, self, meter);
        var cache = new MemoryAdmissionCacheDecorator<string>(inner, gate, estimator, accounting, new FixedOwnerLocator(self), self);
        return (cache, inner, accounting, estimator);
    }

    private static PressureGate CreatePermissiveGate(IMemoryUsageAccounting accounting, string nodeId, Meter meter)
    {
        var options = Options.Create(
            new PressureOptions
            {
                MaxEstimatedCacheBytes = 10_000_000_000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });
        return new PressureGate(new StateEvaluator(options), accounting, nodeId, meter);
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

    private static async Task<bool> KeyExistsAsync(ClientCache<string> cache, string cacheName, string key, CancellationToken cancellationToken) =>
        (await cache.GetValueAsync(cacheName, key, cancellationToken).ConfigureAwait(false)).Found;

    private static async Task RunSynchronizedConcurrentVoidAsync(int concurrency, Func<int, Task> operation, CancellationToken cancellationToken)
    {
        var runner = new SynchronizedConcurrentVoidRunner(operation, cancellationToken);
        var tasks = new Task[concurrency];
        for (var i = 0; i < concurrency; i++)
            tasks[i] = runner.RunAfterGateAsync(i);

        await Task.Delay(50, cancellationToken).ConfigureAwait(false);
        runner.Release();
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    private static async Task<T[]> RunSynchronizedConcurrentlyAsync<T>(int concurrency, Func<int, Task<T>> operation, CancellationToken cancellationToken)
    {
        var runner = new SynchronizedConcurrentRunner<T>(operation, cancellationToken);
        var tasks = new Task<T>[concurrency];
        for (var i = 0; i < concurrency; i++)
            tasks[i] = runner.RunAfterGateAsync(i);

        await Task.Delay(50, cancellationToken).ConfigureAwait(false);
        runner.Release();
        return await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    [Immutable]
    private sealed class ConcurrentCacheOp
    {
        private readonly MemoryAdmissionCacheDecorator<string> _cache;
        private readonly NodeCacheEntry<string>? _entry;
        private readonly string _key;
        private readonly string? _updatedValue;

        internal ConcurrentCacheOp(MemoryAdmissionCacheDecorator<string> cache, string key, NodeCacheEntry<string>? entry = null, string? updatedValue = null)
        {
            _cache = cache;
            _key = key;
            _entry = entry;
            _updatedValue = updatedValue;
            Remove = RemoveCoreAsync;
            SetEntry = SetEntryCoreAsync;
            TryAddEntry = TryAddEntryCoreAsync;
            Update = UpdateCoreAsync;
        }

        internal Func<int, Task<CacheRemoveResult<string>>> Remove { get; }

        internal Func<int, Task> SetEntry { get; }

        internal Func<int, Task<bool>> TryAddEntry { get; }

        internal Func<int, Task<bool>> Update { get; }

        private Task<CacheRemoveResult<string>> RemoveCoreAsync(int index)
        {
            _ = index;
            return _cache.RemoveAsync(UnitMutationOpIds.Default, CacheName, _key, DefaultCancellationToken).AsTask();
        }

        private Task SetEntryCoreAsync(int index)
        {
            _ = index;
            var entry = ThrowHelper.Required(_entry, "Entry is required for SetEntryAsync.");
            return _cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, _key, entry, DefaultCancellationToken).AsTask();
        }

        private Task<bool> TryAddEntryCoreAsync(int index)
        {
            _ = index;
            var entry = ThrowHelper.Required(_entry, "Entry is required for TryAddEntryAsync.");
            return _cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, _key, entry, DefaultCancellationToken).AsTask();
        }

        private Task<bool> UpdateCoreAsync(int index)
        {
            _ = index;
            var updatedValue = ThrowHelper.Required(_updatedValue, "Updated value is required for UpdateAsync.");
            return _cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, _key, updatedValue, DefaultCancellationToken).AsTask();
        }
    }

    [Immutable]
    private sealed class ScriptedRemoveInner : ILogicalNamespacedCache<string>
    {
        internal NodeCacheEntry<string>? GetResult { get; set; }

        internal bool RemoveResult { get; set; }

        internal bool TryAddResult { get; set; }

        public ValueTask<NodeCacheEntry<string>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            _ = cacheName;
            _ = key;
            _ = cancellationToken;
            return ValueTask.FromResult(GetResult);
        }

        public ValueTask<NodeCacheValueResult<string>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            _ = cacheName;
            _ = key;
            _ = cancellationToken;
            var entry = GetResult;
            return ValueTask.FromResult(entry == null ? new NodeCacheValueResult<string>(false, null) : new NodeCacheValueResult<string>(true, entry.Value));
        }

        public ValueTask<CacheRemoveResult<string>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
        {
            _ = operationId;
            _ = cacheName;
            _ = key;
            _ = cancellationToken;
            var entry = GetResult;
            return ValueTask.FromResult(new CacheRemoveResult<string>(RemoveResult, entry?.Value));
        }

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
        {
            _ = operationId;
            _ = cacheName;
            _ = key;
            _ = cancellationToken;
            return ValueTask.FromResult(false);
        }

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken)
        {
            _ = operationId;
            _ = cacheName;
            _ = key;
            _ = entry;
            _ = cancellationToken;
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
        {
            _ = operationId;
            _ = cacheName;
            _ = key;
            _ = expiration;
            _ = cancellationToken;
            return ValueTask.FromResult(false);
        }

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken)
        {
            _ = operationId;
            _ = cacheName;
            _ = key;
            _ = entry;
            _ = cancellationToken;
            return ValueTask.FromResult(TryAddResult);
        }

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, string? value, CancellationToken cancellationToken)
        {
            _ = operationId;
            _ = cacheName;
            _ = key;
            _ = value;
            _ = cancellationToken;
            return ValueTask.FromResult(false);
        }
    }

    [Immutable]
    private sealed class SynchronizedConcurrentRunner<T>
    {
        private readonly CancellationToken _cancellationToken;
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Func<int, Task<T>> _operation;

        internal SynchronizedConcurrentRunner(Func<int, Task<T>> operation, CancellationToken cancellationToken)
        {
            _operation = operation;
            _cancellationToken = cancellationToken;
        }

        internal void Release() => _ = _gate.TrySetResult();

        internal async Task<T> RunAfterGateAsync(int index)
        {
            await _gate.Task.WaitAsync(_cancellationToken).ConfigureAwait(false);
            return await _operation(index).ConfigureAwait(false);
        }
    }

    [Immutable]
    private sealed class SynchronizedConcurrentVoidRunner
    {
        private readonly CancellationToken _cancellationToken;
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Func<int, Task> _operation;

        internal SynchronizedConcurrentVoidRunner(Func<int, Task> operation, CancellationToken cancellationToken)
        {
            _operation = operation;
            _cancellationToken = cancellationToken;
        }

        internal void Release() => _ = _gate.TrySetResult();

        internal async Task RunAfterGateAsync(int index)
        {
            await _gate.Task.WaitAsync(_cancellationToken).ConfigureAwait(false);
            await _operation(index).ConfigureAwait(false);
        }
    }
}
