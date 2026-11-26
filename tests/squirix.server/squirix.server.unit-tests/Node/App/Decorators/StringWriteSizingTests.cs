using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>String writes account for retained memory and preserve encoded payload limits.</summary>
[Immutable]
public sealed class StringWriteSizingTests : ServerUnitTestBase
{
    /// <summary>Normal writes keep the encoded bytes, tags and earliest deadline and account for the UTF-8 payload.</summary>
    /// <param name="kind">Safe case id for empty, ASCII, Unicode or malformed UTF-16 input.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    public async Task StringWritePreservesValueAndFootprint(int kind, CancellationToken cancellationToken)
    {
        using var meter = new Meter("string-write-footprint");
        var clock = new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var value = StoredValue.FromText(ValueFor(kind));
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["tenant"] = "a" }.ToFrozenDictionary(StringComparer.Ordinal);
        var entry = new NodeCacheEntry<StoredValue>(value, 7, clock.GetUtcNow().UtcDateTime.AddMinutes(2), TimeSpan.FromMinutes(1), tags);
        var physical = new PhysicalCache<StoredValue>(clock, new CacheEntrySizeEstimator<StoredValue>());
        var cache = Create(physical, meter, 1_000_000);

        await cache.SetEntryAsync("op", "cache", "key", entry, cancellationToken);
        var stored = await Assert.That(physical.GetEntry(new CacheKey("cache", "key"), cancellationToken)).IsNotNull();

        _ = await Assert.That(stored.Value.Bytes).IsSameReferenceAs(value.Bytes);
        _ = await Assert.That(stored.Version).IsEqualTo(7L);
        _ = await Assert.That(stored.Tags).IsSameReferenceAs(tags);
        _ = await Assert.That(stored.ExpiresUtc).IsEqualTo(clock.GetUtcNow().UtcDateTime.AddMinutes(1));
        _ = await Assert.That(BytesOf(physical)).IsEqualTo(215L + value.Bytes.LongLength);
    }

    /// <summary>Admission accepts a write one byte below the memory limit and rejects it at the limit.</summary>
    /// <param name="kind">Safe id of the string shape.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    public async Task StringAdmissionChecksExactLimit(int kind, CancellationToken cancellationToken)
    {
        using var meter = new Meter("string-admission-boundary");
        var value = StoredValue.FromText(ValueFor(kind));
        var entry = new NodeCacheEntry<StoredValue>(value);
        var expectedBytes = 144L + value.Bytes.LongLength;
        var fitting = new PhysicalCache<StoredValue>(TimeProvider.System, new CacheEntrySizeEstimator<StoredValue>());
        var refused = new PhysicalCache<StoredValue>(TimeProvider.System, new CacheEntrySizeEstimator<StoredValue>());
        var fits = Create(fitting, meter, expectedBytes + 1);
        var tight = Create(refused, meter, expectedBytes);

        await fits.SetEntryAsync("op", "cache", "key", entry, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(tight.SetEntryAsync("op", "cache", "key", entry, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(BytesOf(fitting)).IsEqualTo(expectedBytes);
        _ = await Assert.That(BytesOf(refused)).IsEqualTo(0L);
        _ = await Assert.That(refused.GetValue(new CacheKey("cache", "key"), cancellationToken)).IsEqualTo(new NodeCacheValueResult<StoredValue>(false, default));
    }

    /// <summary>A growing write refused by admission leaves the resident of a full cache available.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedInsertKeepsEvictionCandidate(CancellationToken cancellationToken)
    {
        using var meter = new Meter("bounded-admission");
        var options = new EvictionOptions { Capacity = 1 };
        var physical = new PhysicalCache<StoredValue>(TimeProvider.System, new CacheEntrySizeEstimator<StoredValue>(), options);
        var cache = Create(physical, meter, 300, options);
        var value = new NodeCacheEntry<StoredValue>(StoredValue.FromText("v"));
        await cache.SetEntryAsync("first", "cache", "a", value, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(cache.SetEntryAsync("second", "cache", "b", value, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(physical.GetValue(new CacheKey("cache", "a"), cancellationToken).Found).IsTrue();
        _ = await Assert.That(physical.GetValue(new CacheKey("cache", "b"), cancellationToken).Found).IsFalse();
    }

    /// <summary>Payload checks fall back to exact sizing and include both deadline fields and tag bytes at the limit.</summary>
    /// <param name="unicode">Whether to use three-byte UTF-8 characters.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task TaggedStringPayloadChecksExactBoundary(bool unicode)
    {
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["tenant"] = "a" }.ToFrozenDictionary(StringComparer.Ordinal);
        var deadline = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var empty = new NodeCacheEntry<StoredValue>(StoredValue.FromText(string.Empty), 1, deadline, TimeSpan.FromMinutes(1), tags);
        var length = EntryLimitKit.LargeFixtureBytes - PreparedJournalEntry.MeasureEncodedLength(empty);
        var value = unicode ? new string('\u754c', length / 3) + new string('x', length % 3) : new string('x', length);
        var fitting = new NodeCacheEntry<StoredValue>(StoredValue.FromText(value), 1, deadline, TimeSpan.FromMinutes(1), tags);
        var oversized = new NodeCacheEntry<StoredValue>(StoredValue.FromText(value + "x"), 1, deadline, TimeSpan.FromMinutes(1), tags);

        _ = await Assert.That(PreparedJournalEntry.MeasureEncodedLength(fitting)).IsEqualTo(EntryLimitKit.LargeFixtureBytes);
        _ = await Assert.That(PreparedJournalEntry.MeasureEncodedLength(oversized)).IsEqualTo(EntryLimitKit.LargeFixtureBytes + 1);
    }

    private static MemoryAdmissionCacheDecorator<StoredValue> Create(PhysicalCache<StoredValue> physical, Meter meter, long limit, EvictionOptions? options = null)
    {
        var gate = new PressureGate(
            new StateEvaluator(new PressureOptions { MaxEstimatedCacheBytes = limit, HighPressureThresholdPercent = 99, CriticalPressureThresholdPercent = 100 }),
            new MemoryUsageAccounting(physical),
            "node",
            meter);
        return new MemoryAdmissionCacheDecorator<StoredValue>(
            new ClientCache<StoredValue>(physical, physical),
            gate,
            CacheEntrySizeEstimator<StoredValue>.ForEviction(new CacheEntrySizeEstimator<StoredValue>(), options ?? new EvictionOptions()));
    }

    private static string ValueFor(int kind) => kind switch
    {
        0 => string.Empty,
        1 => new string('x', 4096),
        2 => new string('\u754c', 4096),
        _ => new string(['\ud800', 'x', '\udc00']),
    };

    private static long BytesOf(ILocalCacheStats stats) => stats.EstimatedBytes;
}
