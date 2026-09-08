using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
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
        var inner = CreateThrowingInner();
        var cache = new MetricsCacheDecorator<string>(inner, new CacheMetrics(_testMeter));

        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(cache.SetEntryAsync(UnitMutationOpIds.Default, "c", "k", CreateEntry(), DefaultCancellationToken));
        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException, bool>(
            cache.TryAddEntryAsync(UnitMutationOpIds.Default, "c", "k", CreateEntry(), DefaultCancellationToken));
    }

    /// <summary>Tracing decorator rethrows journal capacity from void and result operations.</summary>
    [Fact]
    public async Task TracingDecoratorRethrowsQuotaFault()
    {
        var inner = CreateThrowingInner();
        var cache = new TracingCacheDecorator<string>(inner, "node-a");

        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(cache.SetEntryAsync(UnitMutationOpIds.Default, "c", "k", CreateEntry(), DefaultCancellationToken));
        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException, bool>(
            cache.TryAddEntryAsync(UnitMutationOpIds.Default, "c", "k", CreateEntry(), DefaultCancellationToken));
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static NodeCacheEntry<string> CreateEntry() => new() { Value = "v", Version = 1 };

    private static ILogicalNamespacedCache<string> CreateThrowingInner()
    {
        var expectations = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = expectations.Setups.GetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromException<NodeCacheEntry<string>?>(new JournalCapacityExceededException()));
        _ = expectations.Setups.GetValueAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromException<NodeCacheValueResult<string>>(new JournalCapacityExceededException()));
        _ = expectations.Setups.RemoveAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromException<CacheRemoveResult<string>>(new JournalCapacityExceededException()));
        _ = expectations.Setups.RemoveExpirationAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromException<bool>(new JournalCapacityExceededException()));
#pragma warning disable CA2012 // Test double: Task-backed faulted ValueTask is awaited exactly once per test.
        _ = expectations.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromException(new JournalCapacityExceededException()));
#pragma warning restore CA2012
        _ = expectations.Setups.TouchAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromException<bool>(new JournalCapacityExceededException()));
        _ = expectations.Setups.TryAddEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromException<bool>(new JournalCapacityExceededException()));
        _ = expectations.Setups.UpdateAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromException<bool>(new JournalCapacityExceededException()));
        return expectations.Instance();
    }
}
