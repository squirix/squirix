using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>Covers journal-quota catch paths on metrics and tracing cache decorators.</summary>
[Immutable]
public sealed class CacheQuotaDecoratorTests : DisposableServerUnitTestBase
{
    private readonly Meter _testMeter = new("test");

    /// <summary>Metrics decorator rethrows journal capacity from void and result operations.</summary>
    [Fact]
    public async Task MetricsDecoratorRethrowsQuotaFault()
    {
        var inner = new ThrowingLogicalCache();
        var cache = new MetricsCacheDecorator<string>(inner, new CacheMetrics(_testMeter));

        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(cache.SetEntryAsync(UnitMutationOpIds.Default, "c", "k", CreateEntry(), DefaultCancellationToken));
        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException, bool>(
            cache.TryAddEntryAsync(UnitMutationOpIds.Default, "c", "k", CreateEntry(), DefaultCancellationToken));
    }

    /// <summary>Tracing decorator rethrows journal capacity from void and result operations.</summary>
    [Fact]
    public async Task TracingDecoratorRethrowsQuotaFault()
    {
        var inner = new ThrowingLogicalCache();
        var cache = new TracingCacheDecorator<string>(inner, "node-a");

        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(cache.SetEntryAsync(UnitMutationOpIds.Default, "c", "k", CreateEntry(), DefaultCancellationToken));
        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException, bool>(
            cache.TryAddEntryAsync(UnitMutationOpIds.Default, "c", "k", CreateEntry(), DefaultCancellationToken));
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static NodeCacheEntry<string> CreateEntry() => new() { Value = "v", Version = 1 };

    private sealed class ThrowingLogicalCache : ILogicalNamespacedCache<string>
    {
        public ValueTask<NodeCacheEntry<string>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromException<NodeCacheEntry<string>?>(new JournalCapacityExceededException());

        public ValueTask<NodeCacheValueResult<string>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromException<NodeCacheValueResult<string>>(new JournalCapacityExceededException());

        public ValueTask<CacheRemoveResult<string>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromException<CacheRemoveResult<string>>(new JournalCapacityExceededException());

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromException<bool>(new JournalCapacityExceededException());

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
            ValueTask.FromException(new JournalCapacityExceededException());

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
            ValueTask.FromException<bool>(new JournalCapacityExceededException());

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
            ValueTask.FromException<bool>(new JournalCapacityExceededException());

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, string? value, CancellationToken cancellationToken) =>
            ValueTask.FromException<bool>(new JournalCapacityExceededException());
    }
}
