using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>Verifies <see cref="TracingCacheDecorator{T}" /> spans with a listener and its pass-through without one.</summary>
[Immutable]
public sealed class TracingCacheDecoratorTests
{
    /// <summary>With a listener attached, a get and a set still produce spans with the operation, node and result tags.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetAndSetCreateSpansWithListener(CancellationToken cancellationToken)
    {
        var nodeId = $"trace-node-{Guid.NewGuid():N}";
        var spans = new List<Activity>();
        var spansLock = new Lock();
        using var listener = new ActivityListener();
        listener.ShouldListenTo = static source => string.Equals(source.Name, ActivitySourceHolder.SourceName, StringComparison.OrdinalIgnoreCase);
        listener.Sample = static (ref _) => ActivitySamplingResult.AllData;
        listener.ActivityStopped = activity =>
        {
            if (!string.Equals(activity.GetTagItem("squirix.node_id") as string, nodeId, StringComparison.Ordinal))
                return;

            lock (spansLock)
                spans.Add(activity);
        };
        ActivitySource.AddActivityListener(listener);
        var innerExpectations = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = innerExpectations.Setups.GetValueAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                             .ReturnValue(ValueTask.FromResult(new NodeCacheValueResult<string>(false, null)));
        _ = innerExpectations.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                             .ReturnValue(ValueTask.CompletedTask);
        var cache = new TracingCacheDecorator<string>(innerExpectations.Instance(), nodeId);

        _ = await cache.GetValueAsync("c", "k", cancellationToken);
        await cache.SetEntryAsync("op", "c", "k", new NodeCacheEntry<string>("v"), cancellationToken);

        Activity[] captured;
        lock (spansLock)
            captured = [.. spans];

        var get = await Assert.That(captured).HasSingleItem(static span => string.Equals(span.DisplayName, "squirix.cache.get", StringComparison.Ordinal));
        _ = await Assert.That(get.GetTagItem("cache.operation")).IsEqualTo(CacheOperationNames.Get);
        _ = await Assert.That(get.GetTagItem("cache.result")).IsEqualTo(CacheOperationResults.NotFound);
        var set = await Assert.That(captured).HasSingleItem(static span => string.Equals(span.DisplayName, "squirix.cache.set", StringComparison.Ordinal));
        _ = await Assert.That(set.GetTagItem("cache.operation")).IsEqualTo(CacheOperationNames.Set);
    }

    /// <summary>The decorator returns the inner result and lets an unclassified inner fault pass through unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ResultsAndFaultsPassThrough(CancellationToken cancellationToken)
    {
        var failure = new InvalidOperationException("set failed");
        var innerExpectations = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = innerExpectations.Setups.GetValueAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                             .ReturnValue(ValueTask.FromResult(new NodeCacheValueResult<string>(true, "v")));
        _ = innerExpectations.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                             .Callback((_, _, _, _, _) => ValueTask.FromException(failure));
        var cache = new TracingCacheDecorator<string>(innerExpectations.Instance(), "node");

        var result = await cache.GetValueAsync("c", "k", cancellationToken);
        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(cache.SetEntryAsync("op", "c", "k", new NodeCacheEntry<string>("v"), cancellationToken));

        _ = await Assert.That(result).IsEqualTo(new NodeCacheValueResult<string>(true, "v"));
        _ = await Assert.That(thrown).IsSameReferenceAs(failure);
    }
}
