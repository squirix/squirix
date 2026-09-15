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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Unit tests for <see cref="MemoryAdmissionCacheDecorator{T}" /> local-owner accounting.</summary>
[Immutable]
public sealed class AdmissionCacheDecoratorTests : DisposableServerUnitTestBase
{
    private const string CacheName = "orders";
    private const int ConcurrentRaceWidth = 64;
    private const string Self = "node-a";

    private readonly Meter _testMeter = new("test");

    /// <summary>Ensures RemoveAsync accounts for one removed local-owner entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentRemoveDeletesLocalKeyOnce(CancellationToken cancellationToken)
    {
        const string key = "remove-race";
        var physical = new PhysicalCache<string>();
        var (cache, inner, accounting, _) = CreateLocalOwnerCache(Self, physical, _testMeter);
        var entry = CreateEntry("v");

        _ = await Assert.That(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, entry, cancellationToken)).IsTrue();
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);

        var remove = new ConcurrentCacheOp(cache, key, cancellationToken);
        var results = await RunSynchronizedConcurrentlyAsync(ConcurrentRaceWidth, remove.Remove, cancellationToken);

        var removedCount = 0;
        foreach (var result in results)
        {
            if (result.Removed)
                removedCount++;
        }

        _ = await Assert.That(removedCount).IsEqualTo(1);
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(0);
        _ = await Assert.That(accounting.ReadEstimatedBytes()).IsEqualTo(0);
        _ = await Assert.That(await KeyExistsAsync(inner, CacheName, key, cancellationToken)).IsFalse();
    }

    /// <summary>Ensures concurrent local-owner SetAsync misses account memory for one physical entry only.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentSetMissAccountsOneEntry(CancellationToken cancellationToken)
    {
        const string key = "set-race";
        var physical = new PhysicalCache<string>();
        var (cache, inner, accounting, estimator) = CreateLocalOwnerCache(Self, physical, _testMeter);
        var entry = CreateEntry("v");
        var expectedBytes = EstimateEntryBytes(estimator, CacheName, key, entry);

        var set = new ConcurrentCacheOp(cache, key, cancellationToken, entry);
        await RunSynchronizedConcurrentVoidAsync(ConcurrentRaceWidth, set.SetEntry, cancellationToken);

        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
        _ = await Assert.That(accounting.ReadEstimatedBytes()).IsEqualTo(expectedBytes);
        _ = await Assert.That(await KeyExistsAsync(inner, CacheName, key, cancellationToken)).IsTrue();
    }

    /// <summary>Ensures concurrent local-owner TryAddAsync misses account memory for one physical entry only.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentTryAddMissAddsSingleEntry(CancellationToken cancellationToken)
    {
        const string key = "try-add-race";
        var physical = new PhysicalCache<string>();
        var (cache, inner, accounting, estimator) = CreateLocalOwnerCache(Self, physical, _testMeter);
        var entry = CreateEntry("v");
        var expectedBytes = EstimateEntryBytes(estimator, CacheName, key, entry);

        var tryAdd = new ConcurrentCacheOp(cache, key, cancellationToken, entry);
        var results = await RunSynchronizedConcurrentlyAsync(ConcurrentRaceWidth, tryAdd.TryAddEntry, cancellationToken);

        var addedCount = 0;
        foreach (var added in results)
        {
            if (added)
                addedCount++;
        }

        _ = await Assert.That(addedCount).IsEqualTo(1);
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
        _ = await Assert.That(accounting.ReadEstimatedBytes()).IsEqualTo(expectedBytes);
        _ = await Assert.That(await KeyExistsAsync(inner, CacheName, key, cancellationToken)).IsTrue();
    }

    /// <summary>Ensures concurrent local-owner UpdateAsync applies to replace accounting once for one physical entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentUpdateReplacesLocalKeyOnce(CancellationToken cancellationToken)
    {
        const string key = "update-race";
        var physical = new PhysicalCache<string>();
        var (cache, _, accounting, estimator) = CreateLocalOwnerCache(Self, physical, _testMeter);
        var initial = CreateEntry("a");
        const string updatedValue = "much-longer-value";
        var replacement = CreateEntry(updatedValue);

        _ = await Assert.That(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, initial, cancellationToken)).IsTrue();
        var bytesBeforeUpdate = accounting.ReadEstimatedBytes();
        var expectedDelta = EstimateEntryBytes(estimator, CacheName, key, replacement) - EstimateEntryBytes(estimator, CacheName, key, initial);

        var update = new ConcurrentCacheOp(cache, key, cancellationToken, updatedValue: updatedValue);
        var results = await RunSynchronizedConcurrentlyAsync(ConcurrentRaceWidth, update.Update, cancellationToken);

        _ = await Assert.That(results).All(static result => result);
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
        _ = await Assert.That(accounting.ReadEstimatedBytes()).IsEqualTo(bytesBeforeUpdate + expectedDelta);
    }

    /// <summary>Ensures RemoveExpirationAsync accounts for removed expiration metadata on a local-owner entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpiryAccountsShrinkForLocalKey(CancellationToken cancellationToken)
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

        _ = await Assert.That(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, entry, cancellationToken)).IsTrue();
        var bytesWithExpiration = accounting.ReadEstimatedBytes();

        _ = await Assert.That(await cache.RemoveExpirationAsync(UnitMutationOpIds.Default, CacheName, key, cancellationToken)).IsTrue();
        _ = await Assert.That(accounting.ReadEstimatedBytes()).IsEqualTo(bytesWithExpiration - expirationGrowth);
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
    }

    /// <summary>Ensures RemoveAsync subtracts recorded bytes rather than a stale pre-remove snapshot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveUsesRecordedBytesNotStale(CancellationToken cancellationToken)
    {
        const string key = "remove-stale-snapshot";
        var small = CreateEntry("a");
        var large = CreateEntry("much-longer-value");
        var inner = new ScriptedRemoveInner();
        var accounting = new MemoryUsageAccounting();
        var estimator = new CacheEntrySizeEstimator<string>();
        var gate = CreatePermissiveGate(accounting, Self, _testMeter);
        var cache = new MemoryAdmissionCacheDecorator<string>(inner, gate, estimator, accounting, RocksDoubles.CreateOwnerLocator(Self), Self);

        inner.GetResult = null;
        inner.TryAddResult = true;
        _ = await Assert.That(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, small, cancellationToken)).IsTrue();

        inner.GetResult = small;
        await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, key, large, cancellationToken);
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);

        inner.GetResult = small;
        inner.RemoveResult = true;
        var result = await cache.RemoveAsync(UnitMutationOpIds.Default, CacheName, key, cancellationToken);

        _ = await Assert.That(result.Removed).IsTrue();
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(0);
        _ = await Assert.That(accounting.ReadEstimatedBytes()).IsEqualTo(0);
    }

    /// <summary>Ensures SetEntryAsync accounts the entry when TryAdd loses the race and falls back to overwrite.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetFallbackAccountsEntry(CancellationToken cancellationToken)
    {
        const string key = "set-fallback-race";
        var entry = CreateEntry("v");
        var inner = new TryAddLosingInner();
        var accounting = new MemoryUsageAccounting();
        var estimator = new CacheEntrySizeEstimator<string>();
        var gate = CreatePermissiveGate(accounting, Self, _testMeter);
        var cache = new MemoryAdmissionCacheDecorator<string>(inner, gate, estimator, accounting, RocksDoubles.CreateOwnerLocator(Self), Self);
        var expectedBytes = EstimateEntryBytes(estimator, CacheName, key, entry);

        await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, key, entry, cancellationToken);

        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
        _ = await Assert.That(accounting.ReadEstimatedBytes()).IsEqualTo(expectedBytes);
        _ = await Assert.That(inner.SetCalled).IsTrue();
    }

    /// <summary>Ensures SetAsync replace accounts for value-size growth on a local-owner entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetReplaceAccountsValueDeltaForLocalKey(CancellationToken cancellationToken)
    {
        const string key = "set-replace";
        var physical = new PhysicalCache<string>();
        var (cache, _, accounting, estimator) = CreateLocalOwnerCache(Self, physical, _testMeter);
        var initial = CreateEntry("a");
        var replacement = CreateEntry("much-longer-value");

        _ = await Assert.That(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, initial, cancellationToken)).IsTrue();
        var bytesBeforeReplace = accounting.ReadEstimatedBytes();
        var expectedDelta = EstimateEntryBytes(estimator, CacheName, key, replacement) - EstimateEntryBytes(estimator, CacheName, key, initial);

        await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, key, replacement, cancellationToken);

        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
        _ = await Assert.That(accounting.ReadEstimatedBytes()).IsEqualTo(bytesBeforeReplace + expectedDelta);
    }

    /// <summary>Ensures TouchAsync accounts for added expiration metadata on a previously non-expiring entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchAccountsExpiryGrowthForLocalKey(CancellationToken cancellationToken)
    {
        const string key = "touch-key";
        var timeProvider = new FakeTimeProvider();
        var physical = new PhysicalCache<string>(timeProvider);
        var (cache, _, accounting, estimator) = CreateLocalOwnerCache(Self, physical, _testMeter);
        var keyValue = new CacheKey(CacheName, key);
        var entry = CreateEntry("v");
        var expirationGrowth = EstimateExpirationMetadataDelta(estimator, keyValue, entry);

        _ = await Assert.That(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, entry, cancellationToken)).IsTrue();
        var bytesBeforeTouch = accounting.ReadEstimatedBytes();

        _ = await Assert.That(await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, key, TimeSpan.FromMinutes(5), cancellationToken)).IsTrue();
        _ = await Assert.That(accounting.ReadEstimatedBytes()).IsEqualTo(bytesBeforeTouch + expirationGrowth);
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
    }

    /// <summary>Ensures TouchAsync does not change accounting when expiration metadata was already present.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchChangesExpiryWhenMetadataPresent(CancellationToken cancellationToken)
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

        _ = await Assert.That(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, entry, cancellationToken)).IsTrue();
        var bytesBeforeTouch = accounting.ReadEstimatedBytes();

        _ = await Assert.That(await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, key, TimeSpan.FromMinutes(5), cancellationToken)).IsTrue();
        _ = await Assert.That(accounting.ReadEstimatedBytes()).IsEqualTo(bytesBeforeTouch);
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
    }

    /// <summary>Ensures UpdateAsync accounts for value-size growth on a local-owner entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateAccountsValueDeltaForLocalKey(CancellationToken cancellationToken)
    {
        const string key = "update-replace";
        var physical = new PhysicalCache<string>();
        var (cache, _, accounting, estimator) = CreateLocalOwnerCache(Self, physical, _testMeter);
        var initial = CreateEntry("a");

        _ = await Assert.That(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, key, initial, cancellationToken)).IsTrue();
        var bytesBeforeUpdate = accounting.ReadEstimatedBytes();
        const string updatedValue = "much-longer-value";
        var replacement = CreateEntry(updatedValue);
        var expectedDelta = EstimateEntryBytes(estimator, CacheName, key, replacement) - EstimateEntryBytes(estimator, CacheName, key, initial);

        _ = await Assert.That(await cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, key, updatedValue, cancellationToken)).IsTrue();

        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
        _ = await Assert.That(accounting.ReadEstimatedBytes()).IsEqualTo(bytesBeforeUpdate + expectedDelta);
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
        var cache = new MemoryAdmissionCacheDecorator<string>(inner, gate, estimator, accounting, RocksDoubles.CreateOwnerLocator(self), self);
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
        private readonly CancellationToken _cancellationToken;
        private readonly NodeCacheEntry<string>? _entry;
        private readonly string _key;
        private readonly string? _updatedValue;

        internal ConcurrentCacheOp(
            MemoryAdmissionCacheDecorator<string> cache,
            string key,
            CancellationToken cancellationToken,
            NodeCacheEntry<string>? entry = null,
            string? updatedValue = null)
        {
            _cache = cache;
            _key = key;
            _cancellationToken = cancellationToken;
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

        private Task<CacheRemoveResult<string>> RemoveCoreAsync(int index) => _cache.RemoveAsync(UnitMutationOpIds.Default, CacheName, _key, _cancellationToken).AsTask();

        private Task SetEntryCoreAsync(int index)
        {
            var entry = ThrowHelper.Required(_entry, "Entry is required for SetEntryAsync.");
            return _cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, _key, entry, _cancellationToken).AsTask();
        }

        private Task<bool> TryAddEntryCoreAsync(int index)
        {
            var entry = ThrowHelper.Required(_entry, "Entry is required for TryAddEntryAsync.");
            return _cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, _key, entry, _cancellationToken).AsTask();
        }

        private Task<bool> UpdateCoreAsync(int index)
        {
            var updatedValue = ThrowHelper.Required(_updatedValue, "Updated value is required for UpdateAsync.");
            return _cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, _key, updatedValue, _cancellationToken).AsTask();
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

    [Immutable]
    private sealed class TryAddLosingInner : ILogicalNamespacedCache<string>
    {
        internal bool SetCalled { get; private set; }

        public ValueTask<NodeCacheEntry<string>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            _ = cacheName;
            _ = key;
            _ = cancellationToken;
            return ValueTask.FromResult<NodeCacheEntry<string>?>(null);
        }

        public ValueTask<NodeCacheValueResult<string>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            _ = cacheName;
            _ = key;
            _ = cancellationToken;
            return ValueTask.FromResult(new NodeCacheValueResult<string>(false, null));
        }

        public ValueTask<CacheRemoveResult<string>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
        {
            _ = operationId;
            _ = cacheName;
            _ = key;
            _ = cancellationToken;
            return ValueTask.FromResult(new CacheRemoveResult<string>(false, null));
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
            SetCalled = true;
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
            return ValueTask.FromResult(false);
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
}
