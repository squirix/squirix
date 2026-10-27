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

/// <summary>Eviction under LFU and estimator failures keep the total of <see cref="PhysicalCache{T}" /> equal to the sum of the entries it holds.</summary>
[Immutable]
public sealed class PhysicalCacheEvictionFootprintTests : ServerUnitTestBase
{
    private static readonly CacheEntrySizeEstimator<string> Estimator = new();
    private static readonly CacheKey KeyA = new("ns", "a");
    private static readonly CacheKey KeyB = new("ns", "b");
    private static readonly CacheKey KeyC = new("ns", "c");
    private static readonly DateTimeOffset Start = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>An estimator that throws on an add leaves the cache as it was, an entry past its deadline included.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddEstimatorFailureLeavesCacheUntouched(CancellationToken cancellationToken)
    {
        var armed = false;
        var clock = new FakeTimeProvider(Start);
        var cache = new PhysicalCache<string>(clock, CreateFailingEstimator(() => armed));
        await cache.SetAsync(KeyA, Entry("short lived", Start.UtcDateTime.AddMinutes(1)), cancellationToken);
        clock.Advance(TimeSpan.FromMinutes(2));
        armed = true;

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(cache, static c => _ = c.TryAddAsync(KeyA, Entry("new"), CancellationToken.None).AsTask());
        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(cache, static c => _ = c.TryAddAsync(KeyB, Entry("other"), CancellationToken.None).AsTask());

        _ = await Assert.That(BytesOf(cache)).IsEqualTo(100L);
        _ = await Assert.That(CountOf(cache)).IsEqualTo(1);
    }

    /// <summary>An estimator that throws on remove-expiration leaves the deadline and the total as they were.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpirationFailureKeepsDeadline(CancellationToken cancellationToken)
    {
        var armed = false;
        var deadline = Start.UtcDateTime.AddMinutes(5);
        var cache = new PhysicalCache<string>(new FakeTimeProvider(Start), CreateFailingEstimator(() => armed));
        await cache.SetAsync(KeyA, Entry("stored", deadline), cancellationToken);
        armed = true;

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(cache, static c => _ = c.RemoveExpirationAsync(KeyA, CancellationToken.None).AsTask());

        _ = await Assert.That(BytesOf(cache)).IsEqualTo(100L);
        _ = await Assert.That((await cache.RawReader.GetEntryRawAsync(KeyA, cancellationToken))?.ExpiresUtc).IsEqualTo(deadline);
    }

    /// <summary>Under LFU the entry with the lowest frequency is evicted and its bytes leave the total.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LfuEvictionReleasesVictimBytes(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>(new FakeTimeProvider(Start), Estimator, new EvictionOptions { Capacity = 2, Policy = EvictionPolicyType.Lfu });
        var popular = Entry("popular");
        var third = Entry("third and larger");
        await cache.SetAsync(KeyA, popular, cancellationToken);
        await cache.SetAsync(KeyB, Entry("rarely read value"), cancellationToken);
        _ = await cache.GetValueAsync(KeyA, cancellationToken);
        _ = await cache.GetValueAsync(KeyA, cancellationToken);

        await cache.SetAsync(KeyC, third, cancellationToken);

        _ = await Assert.That((await cache.GetValueAsync(KeyB, cancellationToken)).Found).IsFalse();
        _ = await Assert.That(CountOf(cache)).IsEqualTo(2);
        _ = await Assert.That(BytesOf(cache)).IsEqualTo(Cost(KeyA, popular) + Cost(KeyC, third));
    }

    /// <summary>Under LFU the entry just inserted can be the victim; the total then stays the sum of the entries that remain.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LfuEvictionOfNewcomerKeepsTotal(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>(new FakeTimeProvider(Start), Estimator, new EvictionOptions { Capacity = 2, Policy = EvictionPolicyType.Lfu });
        var first = Entry("first");
        var second = Entry("second one");
        await cache.SetAsync(KeyA, first, cancellationToken);
        await cache.SetAsync(KeyB, second, cancellationToken);
        _ = await cache.GetValueAsync(KeyA, cancellationToken);
        _ = await cache.GetValueAsync(KeyB, cancellationToken);

        await cache.SetAsync(KeyC, Entry("a newcomer that is the least used"), cancellationToken);

        _ = await Assert.That((await cache.GetValueAsync(KeyC, cancellationToken)).Found).IsFalse();
        _ = await Assert.That(CountOf(cache)).IsEqualTo(2);
        _ = await Assert.That(BytesOf(cache)).IsEqualTo(Cost(KeyA, first) + Cost(KeyB, second));
    }

    private static long BytesOf(ILocalCacheStats stats) => stats.EstimatedBytes;

    private static long Cost(CacheKey key, NodeCacheEntry<string> entry) => Estimator.EstimateBytes(key, entry, false);

    private static int CountOf(ILocalCacheStats stats) => stats.EntryCount;

    private static ICacheEntrySizeEstimator<string> CreateFailingEstimator(Func<bool> armed)
    {
        var estimator = new ICacheEntrySizeEstimatorCreateExpectations<string>();
        _ = estimator.Setups.EstimateBytes(Arg.Any<CacheKey>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<bool>())
                     .Callback((_, _, _) => armed() ? throw new InvalidOperationException("estimator failure") : 100L);
        return estimator.Instance();
    }

    private static NodeCacheEntry<string> Entry(string value, DateTime? expiresUtc = null) => new(value, 1, expiresUtc);
}
