using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>Conservative string bounds preserve exact footprint, admission and encoded payload limits.</summary>
[Immutable]
public sealed class StringWriteSizingTests : ServerUnitTestBase
{
    /// <summary>Normal writes keep the original value, tags and earliest deadline and account exact UTF-8 bytes.</summary>
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
        var value = ValueFor(kind);
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["tenant"] = "a" }.ToFrozenDictionary(StringComparer.Ordinal);
        var entry = new NodeCacheEntry<object?>(value, 7, clock.GetUtcNow().UtcDateTime.AddMinutes(2), TimeSpan.FromMinutes(1), tags);
        var physical = new PhysicalCache<object?>(clock, new ObjectCacheEntrySizeEstimator());
        var cache = Create(physical, meter, 1_000_000);

        await cache.SetEntryAsync("op", "cache", "key", entry, cancellationToken);
        var stored = await Assert.That(await physical.GetEntryAsync(new CacheKey("cache", "key"), cancellationToken)).IsNotNull();

        _ = await Assert.That(stored.Value).IsEqualTo(value);
        _ = await Assert.That(stored.Version).IsEqualTo(7L);
        _ = await Assert.That(stored.Tags).IsSameReferenceAs(tags);
        _ = await Assert.That(stored.ExpiresUtc).IsEqualTo(clock.GetUtcNow().UtcDateTime.AddMinutes(1));
        _ = await Assert.That(BytesOf(physical)).IsEqualTo(215L + Encoding.UTF8.GetByteCount(value));
    }

    /// <summary>An overly conservative bound falls back to the exact byte count at the admission boundary.</summary>
    /// <param name="kind">Safe id of the string shape.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    public async Task StringAdmissionFallsBackAtExactLimit(int kind, CancellationToken cancellationToken)
    {
        using var meter = new Meter("string-admission-boundary");
        var value = ValueFor(kind);
        var entry = new NodeCacheEntry<object?>(value);
        var expectedBytes = 144L + Encoding.UTF8.GetByteCount(value);
        var fitting = new PhysicalCache<object?>(TimeProvider.System, new ObjectCacheEntrySizeEstimator());
        var refused = new PhysicalCache<object?>(TimeProvider.System, new ObjectCacheEntrySizeEstimator());
        var fits = Create(fitting, meter, expectedBytes + 1);
        var tight = Create(refused, meter, expectedBytes);

        await fits.SetEntryAsync("op", "cache", "key", entry, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(tight.SetEntryAsync("op", "cache", "key", entry, cancellationToken));

        _ = await Assert.That(BytesOf(fitting)).IsEqualTo(expectedBytes);
        _ = await Assert.That(BytesOf(refused)).IsEqualTo(0L);
        _ = await Assert.That(await refused.GetValueAsync(new CacheKey("cache", "key"), cancellationToken)).IsEqualTo(new NodeCacheValueResult<object?>(false, null));
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
        var empty = new NodeCacheEntry<object?>(string.Empty, 1, deadline, TimeSpan.FromMinutes(1), tags);
        var length = EntryLimits.MaxEntrySizeBytes - JournalEntryPayload.MeasureSerializedBytes(empty);
        var value = unicode ? new string('\u754c', length / 3) + new string('x', length % 3) : new string('x', length);
        var fitting = new NodeCacheEntry<object?>(value, 1, deadline, TimeSpan.FromMinutes(1), tags);
        var oversized = new NodeCacheEntry<object?>(value + "x", 1, deadline, TimeSpan.FromMinutes(1), tags);

        JournalEntryPayload.EnsureEncodedLengthWithinLimit(fitting);
        var error = NodeExceptionAssert.For<SquirixException>().Throws(oversized, static entry => JournalEntryPayload.EnsureEncodedLengthWithinLimit(entry));

        _ = await Assert.That(JournalEntryPayload.MeasureSerializedBytes(fitting)).IsEqualTo(EntryLimits.MaxEntrySizeBytes);
        _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.PayloadTooLarge);
    }

    /// <summary>The short-string shortcut must not skip tag validation.</summary>
    [Test]
    public async Task ShortStringStillRejectsOversizedTag()
    {
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["tenant"] = new('x', EntryLimits.MaxEntryTagValueUtf8Bytes + 1) }.ToFrozenDictionary(StringComparer.Ordinal);
        var entry = new NodeCacheEntry<object?>("x", tags: tags);

        var error = NodeExceptionAssert.For<SquirixException>().Throws(entry, static candidate => JournalEntryPayload.EnsureEncodedLengthWithinLimit(candidate));

        _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.InvalidEntryTags);
    }

    private static MemoryAdmissionCacheDecorator<object?> Create(PhysicalCache<object?> physical, Meter meter, long limit)
    {
        var gate = new PressureGate(
            new StateEvaluator(new PressureOptions { MaxEstimatedCacheBytes = limit, HighPressureThresholdPercent = 99, CriticalPressureThresholdPercent = 100 }),
            new MemoryUsageAccounting(physical),
            "node",
            meter);
        return new MemoryAdmissionCacheDecorator<object?>(
            new OwnerPutPayloadGuardDecorator<object?>(new ClientCache<object?>(physical, physical)),
            gate,
            new ObjectCacheEntrySizeEstimator());
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
