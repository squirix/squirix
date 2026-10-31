using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>Metric listeners can change dynamically without changing operation results or failure propagation.</summary>
[Immutable]
public sealed class MetricsCacheListenerTests : ServerUnitTestBase
{
    /// <summary>Either instrument alone enables observation, and removing both restores pass-through.</summary>
    /// <param name="flags">One for the counter, two for the histogram or three for both.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    public async Task ListenersAttachAndDetach(int flags, CancellationToken cancellationToken)
    {
        using var meter = new Meter("cache-listener-transition");
        var metrics = new CacheMetrics(meter);
        var physical = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        var cache = new MetricsCacheDecorator<string>(new ClientCache<string>(physical, physical), metrics);
        var counts = new List<(string Operation, string Result, string Cache, long Count)>();
        var durations = new List<double>();
        var instruments = new List<Instrument>();

        _ = await Assert.That(metrics.Enabled).IsFalse();
        await cache.SetEntryAsync("op", "cache", "key", new NodeCacheEntry<string>("v"), cancellationToken);
        _ = await Assert.That(await cache.GetValueAsync("cache", "key", cancellationToken)).IsEqualTo(new NodeCacheValueResult<string>(true, "v"));

        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, current) =>
            {
                if (!ReferenceEquals(instrument.Meter, meter))
                    return;
                if ((instrument is Counter<long> && (flags & 1) != 0) || (instrument is Histogram<double> && (flags & 2) != 0))
                {
                    instruments.Add(instrument);
                    current.EnableMeasurementEvents(instrument);
                }
            },
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) => counts.Add((Tag(tags, "operation"), Tag(tags, "result"), Tag(tags, "cache"), value)));
        listener.SetMeasurementEventCallback<double>((_, value, _, _) => durations.Add(value));
        listener.Start();

        _ = await Assert.That(metrics.Enabled).IsTrue();
        await cache.SetEntryAsync("op-2", "cache", "key", new NodeCacheEntry<string>("changed"), cancellationToken);
        _ = await Assert.That(await cache.GetValueAsync("cache", "key", cancellationToken)).IsEqualTo(new NodeCacheValueResult<string>(true, "changed"));
        _ = await Assert.That(await cache.GetValueAsync("cache", "missing", cancellationToken)).IsEqualTo(new NodeCacheValueResult<string>(false, null));

        _ = await Assert.That(counts.Count).IsEqualTo((flags & 1) != 0 ? 3 : 0);
        _ = await Assert.That(durations.Count).IsEqualTo((flags & 2) != 0 ? 3 : 0);
        if ((flags & 1) != 0)
        {
            _ = await Assert.That(counts[0]).IsEqualTo((CacheOperationNames.Set, CacheOperationResults.Ok, "cache", 1L));
            _ = await Assert.That(counts[1]).IsEqualTo((CacheOperationNames.Get, CacheOperationResults.Ok, "cache", 1L));
            _ = await Assert.That(counts[2]).IsEqualTo((CacheOperationNames.Get, CacheOperationResults.NotFound, "cache", 1L));
        }

        for (var i = 0; i < durations.Count; i++)
            _ = await Assert.That(durations[i]).IsGreaterThanOrEqualTo(0d);
        for (var i = 0; i < instruments.Count; i++)
            _ = listener.DisableMeasurementEvents(instruments[i]);

        _ = await Assert.That(metrics.Enabled).IsFalse();
        await cache.SetEntryAsync("op-3", "cache", "key", new NodeCacheEntry<string>("after"), cancellationToken);
        _ = await Assert.That(await cache.GetValueAsync("cache", "key", cancellationToken)).IsEqualTo(new NodeCacheValueResult<string>(true, "after"));
        _ = await Assert.That(counts.Count).IsEqualTo((flags & 1) != 0 ? 3 : 0);
        _ = await Assert.That(durations.Count).IsEqualTo((flags & 2) != 0 ? 3 : 0);
    }

    /// <summary>Classified errors keep their identity and are recorded once when observed.</summary>
    /// <param name="enabled">Whether a counter listener is attached.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task FailureKeepsIdentity(bool enabled, CancellationToken cancellationToken)
    {
        using var meter = new Meter("cache-listener-error");
        var results = new List<string>();
        var failure = new TimeoutException("inner deadline");
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.SetEntryAsync("op", "cache", "key", Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                 .Callback((_, _, _, _, _) => ValueTask.FromException(failure));
        var cache = new MetricsCacheDecorator<string>(inner.Instance(), new CacheMetrics(meter));
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, current) =>
            {
                if (enabled && ReferenceEquals(instrument.Meter, meter) && instrument is Counter<long>)
                    current.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>((_, _, tags, _) => results.Add(Tag(tags, "result")));
        listener.Start();

        var thrown = await NodeAsyncAssert.ThrowsAsync<TimeoutException>(cache.SetEntryAsync("op", "cache", "key", new NodeCacheEntry<string>("v"), cancellationToken));

        _ = await Assert.That(thrown).IsSameReferenceAs(failure);
        _ = await Assert.That(results.Count).IsEqualTo(enabled ? 1 : 0);
        if (enabled)
            _ = await Assert.That(results[0]).IsEqualTo(CacheOperationResults.DeadlineExceeded);
    }

    /// <summary>Observation waits for an asynchronous result and does not consume it more than once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PendingReadRecordsOnlyAfterCompletion(CancellationToken cancellationToken)
    {
        using var meter = new Meter("cache-listener-pending");
        var completion = new TaskCompletionSource<NodeCacheValueResult<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetValueAsync("cache", "key", Arg.Any<CancellationToken>()).ReturnValue(new ValueTask<NodeCacheValueResult<string>>(completion.Task));
        var cache = new MetricsCacheDecorator<string>(inner.Instance(), new CacheMetrics(meter));
        var counts = new List<long>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, current) =>
            {
                if (ReferenceEquals(instrument.Meter, meter) && instrument is Counter<long>)
                    current.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>((_, value, _, _) => counts.Add(value));
        listener.Start();

        var pending = cache.GetValueAsync("cache", "key", cancellationToken);
        _ = await Assert.That(counts.Count).IsEqualTo(0);
        completion.SetResult(new NodeCacheValueResult<string>(true, "v"));
        var result = await pending;

        _ = await Assert.That(result).IsEqualTo(new NodeCacheValueResult<string>(true, "v"));
        _ = await Assert.That(counts.Count).IsEqualTo(1);
        _ = await Assert.That(counts[0]).IsEqualTo(1L);
    }

    private static string Tag(ReadOnlySpan<KeyValuePair<string, object?>> tags, string name)
    {
        foreach (var tag in tags)
        {
            if (string.Equals(tag.Key, name, StringComparison.Ordinal))
                return tag.Value as string ?? string.Empty;
        }

        return string.Empty;
    }
}
