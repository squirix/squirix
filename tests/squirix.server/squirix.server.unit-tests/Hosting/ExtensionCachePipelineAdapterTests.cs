using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
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
        var entry = new NodeCacheEntry<object?> { Value = "value", Version = 7 };

        await adapter.SetEntryAsync(TestOperationIds.Default, "cache", "key", entry, CancellationToken.None);
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
        private NodeCacheEntry<object?>? _entry;

        public int GetEntryCalls { get; private set; }

        public int GetValueCalls { get; private set; }

        public int InsertEntryCalls { get; private set; }

        public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            GetEntryCalls++;
            return new ValueTask<NodeCacheEntry<object?>?>(_entry);
        }

        public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            GetValueCalls++;
            return new ValueTask<NodeCacheValueResult<object?>>(new NodeCacheValueResult<object?>(_entry is not null, _entry?.Value));
        }

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            InsertEntryCalls++;
            _entry = entry;
            return default;
        }

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => new(false);

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) => new(false);

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            new(new CacheRemoveResult<object?>(false, null));

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => new(false);

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken) => new(false);
    }

    private sealed class RecordingLogicalCache : ILogicalNamespacedCache<object?>
    {
        public int GetEntryCalls { get; private set; }

        public int GetValueCalls { get; private set; }

        public int InsertEntryCalls { get; private set; }

        public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            GetEntryCalls++;
            return default;
        }

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => new(false);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            InsertEntryCalls++;
            return default;
        }

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => new(false);

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) => new(false);

        public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            GetValueCalls++;
            return new ValueTask<NodeCacheValueResult<object?>>(new NodeCacheValueResult<object?>(false, null));
        }

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            new(new CacheRemoveResult<object?>(false, null));

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken) => new(false);
    }
}
