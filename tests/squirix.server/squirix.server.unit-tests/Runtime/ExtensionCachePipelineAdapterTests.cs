using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Xunit;

namespace Squirix.Server.UnitTests.Runtime;

/// <summary>
/// Verifies extension cache pipeline adapter behavior.
/// </summary>
public sealed class ExtensionCachePipelineAdapterTests
{
    /// <summary>
    /// Ensures entry-aware extension pipelines receive entry operations.
    /// </summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Fact]
    public async Task EntryOperationsUseEntryAwareDecoratedPipeline()
    {
        var core = new RecordingLogicalCache();
        var decorated = new RecordingEntryPipeline();
        var adapter = new ExtensionCachePipelineAdapter<object?>(core, decorated);
        var entry = new CacheEntry<object?> { Value = "value", Version = 7 };

        await adapter.SetAsync("cache", "key", entry, CancellationToken.None);
        var result = await adapter.GetEntryAsync("cache", "key", CancellationToken.None);

        Assert.Equal(1, decorated.InsertEntryCalls);
        Assert.Equal(1, decorated.GetEntryCalls);
        Assert.Equal(0, core.InsertEntryCalls);
        Assert.Equal(0, core.GetEntryCalls);
        Assert.Same(entry, result);
    }

    private sealed class RecordingEntryPipeline : ISquirixServerEntryCachePipeline<object?>
    {
        private CacheEntry<object?>? _entry;

        public int GetEntryCalls { get; private set; }

        public int InsertEntryCalls { get; private set; }

        public ValueTask AddAsync(string cacheName, string key, object? value, CancellationToken cancellationToken) => default;

        public ValueTask AddAsync(string cacheName, string key, CacheEntry<object?> entry, CancellationToken cancellationToken) => default;

        public ValueTask<bool> ContainsAsync(string cacheName, string key, CancellationToken cancellationToken) => new(false);

        public ValueTask<CacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            GetEntryCalls++;
            return new ValueTask<CacheEntry<object?>?>(_entry);
        }

        public ValueTask<TimeSpan?> GetExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => new((TimeSpan?)null);

        public ValueTask<object?> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => new((object?)null);

        public ValueTask InsertAsync(string cacheName, string key, object? value, CancellationToken cancellationToken) => default;

        public ValueTask InsertAsync(string cacheName, string key, CacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            InsertEntryCalls++;
            _entry = entry;
            return default;
        }

        public ValueTask<bool> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken) => new(false);

        public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => new(false);

        public ValueTask<bool> TryAddAsync(string cacheName, string key, object? value, CancellationToken cancellationToken) => new(false);

        public ValueTask<bool> TryAddAsync(string cacheName, string key, CacheEntry<object?> entry, CancellationToken cancellationToken) => new(false);
    }

    private sealed class RecordingLogicalCache : ILogicalNamespacedCache<object?>
    {
        public int GetEntryCalls { get; private set; }

        public int InsertEntryCalls { get; private set; }

        public ValueTask AddAsync(string cacheName, string key, object? value, CancellationToken cancellationToken) => default;

        public ValueTask AddAsync(string cacheName, string key, CacheEntry<object?> entry, CancellationToken cancellationToken) => default;

        public ValueTask<bool> ContainsAsync(string cacheName, string key, CancellationToken cancellationToken) => new(false);

        public ValueTask<CacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            GetEntryCalls++;
            return new ValueTask<CacheEntry<object?>?>((CacheEntry<object?>?)null);
        }

        public ValueTask<TimeSpan?> GetExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => new((TimeSpan?)null);

        public ValueTask<CacheValueResult<object?>> GetOrAddAsync(string cacheName, string key, CacheEntry<object?> entry, CancellationToken cancellationToken) =>
            new(new CacheValueResult<object?>(false, null));

        public ValueTask<object?> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => new((object?)null);

        public ValueTask<bool> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken) => new(false);

        public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => new(false);

        public ValueTask SetAsync(string cacheName, string key, object? value, CancellationToken cancellationToken) => default;

        public ValueTask SetAsync(string cacheName, string key, CacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            InsertEntryCalls++;
            return default;
        }

        public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => new(false);

        public ValueTask<bool> TryAddAsync(string cacheName, string key, object? value, CancellationToken cancellationToken) => new(false);

        public ValueTask<bool> TryAddAsync(string cacheName, string key, CacheEntry<object?> entry, CancellationToken cancellationToken) => new(false);

        public ValueTask<CacheValueResult<object?>> TryGetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            new(new CacheValueResult<object?>(false, null));

        public ValueTask<CacheRemoveResult<object?>> TryRemoveAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            new(new CacheRemoveResult<object?>(false, null));

        public ValueTask<bool> UpdateAsync(string cacheName, string key, object? value, CancellationToken cancellationToken) => new(false);
    }
}
