using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Xunit;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies extension cache pipeline adapter behavior.</summary>
public sealed class ExtensionCachePipelineAdapterTests
{
    /// <summary>Ensures entry-aware extension pipelines receive entry operations.</summary>
    [Fact]
    public async Task EntryOperationsUseEntryAwareDecoratedPipeline()
    {
        var core = new RecordingLogicalCache();
        var decorated = new RecordingEntryPipeline();
        var adapter = new ExtensionCachePipelineAdapter<object?>(core, decorated);
        var entry = new CacheEntry<object?> { Value = "value", Version = 7 };

        await adapter.SetEntryAsync("cache", "key", entry, CancellationToken.None);
        var result = await adapter.GetEntryAsync("cache", "key", CancellationToken.None);

        Assert.Equal(1, decorated.InsertEntryCalls);
        Assert.Equal(1, decorated.GetEntryCalls);
        Assert.Equal(0, core.InsertEntryCalls);
        Assert.Equal(0, core.GetEntryCalls);
        Assert.Same(entry, result);
    }

    /// <summary>Ensures value reads route through the decorated pipeline.</summary>
    [Fact]
    public async Task GetValueUsesDecoratedPipeline()
    {
        var core = new RecordingLogicalCache();
        var decorated = new RecordingEntryPipeline();
        var adapter = new ExtensionCachePipelineAdapter<object?>(core, decorated);

        _ = await adapter.GetValueAsync("cache", "key", CancellationToken.None);

        Assert.Equal(1, decorated.GetValueCalls);
        Assert.Equal(0, core.GetValueCalls);
    }

    private sealed class RecordingEntryPipeline : ISquirixServerEntryCachePipeline<object?>
    {
        private CacheEntry<object?>? _entry;

        public int GetEntryCalls { get; private set; }

        public int GetValueCalls { get; private set; }

        public int InsertEntryCalls { get; private set; }

        public ValueTask<CacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            GetEntryCalls++;
            return new ValueTask<CacheEntry<object?>?>(_entry);
        }

        public ValueTask<CacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            GetValueCalls++;
            return new ValueTask<CacheValueResult<object?>>(new CacheValueResult<object?>(_entry is not null, _entry?.Value));
        }

        public ValueTask SetEntryAsync(string cacheName, string key, CacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            InsertEntryCalls++;
            _entry = entry;
            return default;
        }

        public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => new(false);

        public ValueTask<bool> TryAddEntryAsync(string cacheName, string key, CacheEntry<object?> entry, CancellationToken cancellationToken) => new(false);

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            new(new CacheRemoveResult<object?>(false, null));

        public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => new(false);

        public ValueTask<bool> UpdateAsync(string cacheName, string key, object? value, CancellationToken cancellationToken) => new(false);
    }

    private sealed class RecordingLogicalCache : ILogicalNamespacedCache<object?>
    {
        public int GetEntryCalls { get; private set; }

        public int GetValueCalls { get; private set; }

        public int InsertEntryCalls { get; private set; }

        public ValueTask<CacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            GetEntryCalls++;
            return default;
        }

        public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => new(false);

        public ValueTask SetEntryAsync(string cacheName, string key, CacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            InsertEntryCalls++;
            return default;
        }

        public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => new(false);

        public ValueTask<bool> TryAddEntryAsync(string cacheName, string key, CacheEntry<object?> entry, CancellationToken cancellationToken) => new(false);

        public ValueTask<CacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            GetValueCalls++;
            return new(new CacheValueResult<object?>(false, null));
        }

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            new(new CacheRemoveResult<object?>(false, null));

        public ValueTask<bool> UpdateAsync(string cacheName, string key, object? value, CancellationToken cancellationToken) => new(false);
    }
}
