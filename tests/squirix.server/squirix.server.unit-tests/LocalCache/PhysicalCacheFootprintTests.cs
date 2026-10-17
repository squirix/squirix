using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.LocalCache;

/// <summary>Every <see cref="PhysicalCache{T}" /> operation that stores, resizes or drops an entry moves its estimated bytes by the estimate of that entry.</summary>
[Immutable]
public sealed class PhysicalCacheFootprintTests : ServerUnitTestBase
{
    private static readonly CacheEntrySizeEstimator<string> Estimator = new();
    private static readonly CacheKey KeyA = new("ns", "a");
    private static readonly CacheKey KeyB = new("ns", "b");
    private static readonly DateTimeOffset Start = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>An add records the estimate of the entry; a refused add changes nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddRecordsEstimate(CancellationToken cancellationToken)
    {
        var cache = Create(new FakeTimeProvider(Start));
        var entry = Entry("value");

        _ = await Assert.That(await cache.TryAddAsync(KeyA, entry, cancellationToken)).IsTrue();
        _ = await Assert.That(await cache.TryAddAsync(KeyA, Entry("a much longer value"), cancellationToken)).IsFalse();

        _ = await Assert.That(BytesOf(cache)).IsEqualTo(Cost(KeyA, entry));
    }

    /// <summary>A set over an entry moves the total by the difference of the two estimates.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplaceMovesByEstimateDelta(CancellationToken cancellationToken)
    {
        var cache = Create(new FakeTimeProvider(Start));
        var small = Entry("a");
        var large = Entry("a much longer value");

        await cache.SetAsync(KeyA, small, cancellationToken);
        var before = BytesOf(cache);
        await cache.SetAsync(KeyA, large, cancellationToken);

        _ = await Assert.That(before).IsEqualTo(Cost(KeyA, small));
        _ = await Assert.That(BytesOf(cache)).IsEqualTo(before + Cost(KeyA, large) - Cost(KeyA, small));
    }

    /// <summary>A remove subtracts the bytes recorded for the entry; removing an absent key subtracts nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveReleasesEstimate(CancellationToken cancellationToken)
    {
        var cache = Create(new FakeTimeProvider(Start));
        var other = Entry("other");
        await cache.SetAsync(KeyA, Entry("value"), cancellationToken);
        await cache.SetAsync(KeyB, other, cancellationToken);

        _ = await cache.RemoveAsync(KeyA, cancellationToken);
        _ = await cache.RemoveAsync(KeyA, cancellationToken);

        _ = await Assert.That(BytesOf(cache)).IsEqualTo(Cost(KeyB, other));

        _ = await Assert.That(await cache.RemoveRecoveryAsync(KeyB, cancellationToken)).IsTrue();
        _ = await Assert.That(BytesOf(cache)).IsEqualTo(0L);
    }

    /// <summary>A read that drops an entry past its deadline releases its bytes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LazyExpiryReleasesEstimate(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        var cache = Create(clock);
        var live = Entry("live");
        await cache.SetAsync(KeyA, Entry("short lived", Start.UtcDateTime.AddMinutes(1)), cancellationToken);
        await cache.SetAsync(KeyB, live, cancellationToken);
        clock.Advance(TimeSpan.FromMinutes(2));

        var read = await cache.GetValueAsync(KeyA, cancellationToken);

        _ = await Assert.That(read.Found).IsFalse();
        _ = await Assert.That(BytesOf(cache)).IsEqualTo(Cost(KeyB, live));
    }

    /// <summary>Under committed expiry an entry past its deadline stays counted until a committed record removes it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommittedExpiryKeepsEstimateUntilRemoved(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        var cache = Create(clock, null, CacheExpiryAuthority.CommittedRecords);
        var entry = Entry("short lived", Start.UtcDateTime.AddMinutes(1));
        await cache.SetAsync(KeyA, entry, cancellationToken);
        clock.Advance(TimeSpan.FromMinutes(2));

        _ = await cache.GetValueAsync(KeyA, cancellationToken);
        var afterRead = BytesOf(cache);
        _ = await cache.RemoveRecoveryAsync(KeyA, cancellationToken);

        _ = await Assert.That(afterRead).IsEqualTo(Cost(KeyA, entry));
        _ = await Assert.That(BytesOf(cache)).IsEqualTo(0L);
    }

    /// <summary>An eviction subtracts the bytes recorded for the evicted entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EvictionReleasesEstimate(CancellationToken cancellationToken)
    {
        var cache = Create(new FakeTimeProvider(Start), new EvictionOptions { Capacity = 1, Policy = EvictionPolicyType.Fifo });
        var newest = Entry("newest value");
        await cache.SetAsync(KeyA, Entry("a much longer value that goes first"), cancellationToken);

        await cache.SetAsync(KeyB, newest, cancellationToken);

        _ = await Assert.That((await cache.GetValueAsync(KeyA, cancellationToken)).Found).IsFalse();
        _ = await Assert.That(BytesOf(cache)).IsEqualTo(Cost(KeyB, newest));
    }

    /// <summary>An add over an entry past its deadline releases the old entry and records the new one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddOverExpiredEntryReplacesEstimate(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        var cache = Create(clock);
        var replacement = Entry("new");
        await cache.SetAsync(KeyA, Entry("short lived and fairly long", Start.UtcDateTime.AddMinutes(1)), cancellationToken);
        clock.Advance(TimeSpan.FromMinutes(2));

        _ = await Assert.That(await cache.TryAddAsync(KeyA, replacement, cancellationToken)).IsTrue();

        _ = await Assert.That(BytesOf(cache)).IsEqualTo(Cost(KeyA, replacement));
    }

    /// <summary>A touch that gives an entry its first deadline adds the deadline to its estimate; touching again changes nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchRecordsDeadlineEstimate(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        var cache = Create(clock);
        await cache.SetAsync(KeyA, Entry("value"), cancellationToken);
        var withDeadline = Cost(KeyA, Entry("value", Start.UtcDateTime.AddMinutes(5)));

        _ = await Assert.That(await cache.TouchAsync(KeyA, TimeSpan.FromMinutes(5), cancellationToken)).IsTrue();
        var afterFirst = BytesOf(cache);
        _ = await Assert.That(await cache.TouchAsync(KeyA, TimeSpan.FromMinutes(10), cancellationToken)).IsTrue();

        _ = await Assert.That(afterFirst).IsEqualTo(withDeadline);
        _ = await Assert.That(BytesOf(cache)).IsEqualTo(withDeadline);
    }

    /// <summary>An update re-estimates the stored entry with the new value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateRecordsNewValueEstimate(CancellationToken cancellationToken)
    {
        var cache = Create(new FakeTimeProvider(Start));
        const string updated = "a much longer value";
        await cache.SetAsync(KeyA, Entry("a", Start.UtcDateTime.AddMinutes(5)), cancellationToken);

        _ = await Assert.That(await cache.UpdateAsync(KeyA, updated, cancellationToken)).IsTrue();

        _ = await Assert.That(BytesOf(cache)).IsEqualTo(Cost(KeyA, Entry(updated, Start.UtcDateTime.AddMinutes(5))));
    }

    /// <summary>Removing the expiration of an entry takes the deadline out of its estimate.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpirationReleasesDeadlineEstimate(CancellationToken cancellationToken)
    {
        var cache = Create(new FakeTimeProvider(Start));
        await cache.SetAsync(KeyA, Entry("value", Start.UtcDateTime.AddMinutes(5)), cancellationToken);

        _ = await Assert.That(await cache.RemoveExpirationAsync(KeyA, cancellationToken)).IsTrue();

        _ = await Assert.That(BytesOf(cache)).IsEqualTo(Cost(KeyA, Entry("value")));
    }

    /// <summary>A recovery insert records the estimate of the entry, and inserting the key again replaces it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveryInsertRecordsEstimate(CancellationToken cancellationToken)
    {
        var cache = Create(new FakeTimeProvider(Start));
        var first = Entry("first");
        var second = Entry("second and longer");

        await cache.InsertRecoveryAsync(KeyA, first, cancellationToken);
        var afterFirst = BytesOf(cache);
        await cache.InsertRecoveryAsync(KeyA, second, cancellationToken);

        _ = await Assert.That(afterFirst).IsEqualTo(Cost(KeyA, first));
        _ = await Assert.That(BytesOf(cache)).IsEqualTo(Cost(KeyA, second));
    }

    /// <summary>An estimator that throws on a set leaves the cache as it was, for a new key and for a key already stored.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetEstimatorFailureLeavesCacheUntouched(CancellationToken cancellationToken)
    {
        var armed = false;
        var estimator = new ICacheEntrySizeEstimatorCreateExpectations<string>();
        _ = estimator.Setups.EstimateBytes(Arg.Any<CacheKey>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<bool>())
                     .Callback((_, _, _) => armed ? throw new InvalidOperationException("estimator failure") : 100L);
        var cache = new PhysicalCache<string>(new FakeTimeProvider(Start), estimator.Instance());
        await cache.SetAsync(KeyA, Entry("stored"), cancellationToken);
        armed = true;

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(cache, static c => _ = c.SetAsync(KeyB, Entry("new"), CancellationToken.None).AsTask());
        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(cache, static c => _ = c.SetAsync(KeyA, Entry("replacement"), CancellationToken.None).AsTask());

        _ = await Assert.That(BytesOf(cache)).IsEqualTo(100L);
        _ = await Assert.That(CountOf(cache)).IsEqualTo(1);
        _ = await Assert.That((await cache.GetValueAsync(KeyA, cancellationToken)).Value).IsEqualTo("stored");
    }

    /// <summary>An estimator that throws on an update leaves the stored value and the total as they were.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateEstimatorFailureKeepsEntry(CancellationToken cancellationToken)
    {
        var armed = false;
        var estimator = new ICacheEntrySizeEstimatorCreateExpectations<string>();
        _ = estimator.Setups.EstimateBytes(Arg.Any<CacheKey>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<bool>())
                     .Callback((_, _, _) => armed ? throw new InvalidOperationException("estimator failure") : 100L);
        var cache = new PhysicalCache<string>(new FakeTimeProvider(Start), estimator.Instance());
        await cache.SetAsync(KeyA, Entry("stored"), cancellationToken);
        armed = true;

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(cache, static c => _ = c.UpdateAsync(KeyA, "updated", CancellationToken.None).AsTask());

        _ = await Assert.That(BytesOf(cache)).IsEqualTo(100L);
        _ = await Assert.That((await cache.GetValueAsync(KeyA, cancellationToken)).Value).IsEqualTo("stored");
    }

    /// <summary>An estimator that throws on an update or a first touch leaves the eviction order as it was: the oldest entry is still the one evicted.</summary>
    /// <param name="touch">Whether the failing operation is a touch rather than an update.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task EstimatorFailureKeepsEvictionOrder(bool touch, CancellationToken cancellationToken)
    {
        var armed = false;
        var estimator = new ICacheEntrySizeEstimatorCreateExpectations<string>();
        _ = estimator.Setups.EstimateBytes(Arg.Any<CacheKey>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<bool>())
                     .Callback((_, _, _) => armed ? throw new InvalidOperationException("estimator failure") : 100L);
        var cache = new PhysicalCache<string>(new FakeTimeProvider(Start), estimator.Instance(), new EvictionOptions { Capacity = 2, Policy = EvictionPolicyType.Lru });
        await cache.SetAsync(KeyA, Entry("oldest"), cancellationToken);
        await cache.SetAsync(KeyB, Entry("newest"), cancellationToken);
        armed = true;

        _ = touch
            ? NodeExceptionAssert.For<InvalidOperationException>().Throws(cache, static c => _ = c.TouchAsync(KeyA, TimeSpan.FromMinutes(1), CancellationToken.None).AsTask())
            : NodeExceptionAssert.For<InvalidOperationException>().Throws(cache, static c => _ = c.UpdateAsync(KeyA, "updated", CancellationToken.None).AsTask());

        armed = false;
        await cache.SetAsync(new CacheKey("ns", "c"), Entry("third"), cancellationToken);

        _ = await Assert.That(await cache.RawReader.GetEntryRawAsync(KeyA, cancellationToken)).IsNull();
        _ = await Assert.That((await cache.RawReader.GetEntryRawAsync(KeyB, cancellationToken))?.Value).IsEqualTo("newest");
    }

    private static long BytesOf(ILocalCacheStats stats) => stats.EstimatedBytes;

    private static int CountOf(ILocalCacheStats stats) => stats.EntryCount;

    private static long Cost(CacheKey key, NodeCacheEntry<string> entry) => Estimator.EstimateBytes(key, entry, false);

    private static PhysicalCache<string> Create(TimeProvider clock, EvictionOptions? eviction = null, CacheExpiryAuthority expiry = CacheExpiryAuthority.LocalClock) =>
        new(clock, Estimator, eviction, expiry);

    private static NodeCacheEntry<string> Entry(string value, DateTime? expiresUtc = null) => new(value, 1, expiresUtc);
}
