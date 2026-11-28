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
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>Covers journal-quota catch paths on metrics and tracing cache decorators.</summary>
[Immutable]
public sealed class CacheQuotaDecoratorTests : DisposableServerUnitTestBase
{
    private readonly Meter _testMeter = new("test");

    /// <summary>Metrics decorator rethrows journal capacity from void and result operations.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MetricsDecoratorRethrowsQuotaFault(CancellationToken cancellationToken)
    {
        var inner = CreateThrowingInner();
        var cache = new MetricsCacheDecorator<string>(inner, new CacheMetrics(_testMeter));

        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(cache.SetEntryAsync(UnitMutationOpIds.Default, "c", "k", CreateEntry(), cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException, bool>(cache.TryAddEntryAsync(UnitMutationOpIds.Default, "c", "k", CreateEntry(), cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
    }

    /// <summary>Tracing decorator rethrows journal capacity from void and result operations.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TracingDecoratorRethrowsQuotaFault(CancellationToken cancellationToken)
    {
        var inner = CreateThrowingInner();
        var cache = new TracingCacheDecorator<string>(inner, "node-a");

        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(cache.SetEntryAsync(UnitMutationOpIds.Default, "c", "k", CreateEntry(), cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException, bool>(cache.TryAddEntryAsync(UnitMutationOpIds.Default, "c", "k", CreateEntry(), cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static NodeCacheEntry<string> CreateEntry() => new() { Value = "v", Version = 1 };

    private static ILogicalNamespacedCache<string> CreateThrowingInner()
    {
        var expectations = ILogicalNamespacedCache<string>.Mock(MockBehavior.Strict);
        _ = expectations.GetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync(static (_, _, _) => ValueTask.FromException<NodeCacheEntry<string>?>(new JournalCapacityExceededException()));
        _ = expectations.GetValueAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync(static (_, _, _) => ValueTask.FromException<NodeCacheValueResult<string>>(new JournalCapacityExceededException()));
        _ = expectations.RemoveAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync(static (_, _, _, _) => ValueTask.FromException<bool>(new JournalCapacityExceededException()));
        _ = expectations.RemoveExpirationAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync(static (_, _, _, _) => ValueTask.FromException<bool>(new JournalCapacityExceededException()));
        _ = expectations.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync(static (_, _, _, _, _) => ValueTask.FromException(new JournalCapacityExceededException()));
        _ = expectations.TouchAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync(static (_, _, _, _, _) => ValueTask.FromException<bool>(new JournalCapacityExceededException()));
        _ = expectations.TryAddEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync(static (_, _, _, _, _) => ValueTask.FromException<bool>(new JournalCapacityExceededException()));
        _ = expectations.UpdateAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync(static (_, _, _, _, _) => ValueTask.FromException<bool>(new JournalCapacityExceededException()));
        return expectations.Object;
    }
}
