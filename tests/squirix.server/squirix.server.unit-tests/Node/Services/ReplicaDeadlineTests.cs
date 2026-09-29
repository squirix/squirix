using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Replicated records pin absolute expiration deadlines at prepare time, so every apply of a record sets the same deadline.</summary>
public sealed class ReplicaDeadlineTests : ServerUnitTestBase
{
    private const string CacheName = "cache";
    private const string Key = "k";
    private static readonly TimeSpan Expiration = TimeSpan.FromMinutes(5);

    /// <summary>A prepared Touch carries the absolute deadline, prepare time plus the expiration, instead of the relative expiration.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchRecordCarriesAbsoluteDeadline(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var prepared = clock.GetUtcNow().UtcDateTime;
        var cache = NewCache(clock);
        var factory = new ReplicaMutationFactory(cache, "g1", 1UL, clock);
        await ApplyAsync(cache, factory.PrepareSet("op-0", CacheName, Key, new NodeCacheEntry<object?> { Value = "v1" }, 1UL), cancellationToken);

        var touch = Decode(await factory.PrepareTouchAsync("op-1", CacheName, Key, Expiration, 2UL, cancellationToken));

        _ = await Assert.That(touch.ExpiresUtcTicks).IsEqualTo(prepared.Add(Expiration).Ticks);
    }

    /// <summary>A Touch applied later than its prepare keeps the pinned deadline instead of extending it from the apply time.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchAppliesPinnedDeadline(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var cache = NewCache(clock);
        var factory = new ReplicaMutationFactory(cache, "g1", 1UL, clock);
        await ApplyAsync(cache, factory.PrepareSet("op-1", CacheName, Key, new NodeCacheEntry<object?> { Value = "v1" }, 1UL), cancellationToken);
        var deadline = clock.GetUtcNow().UtcDateTime.Add(Expiration);
        var touch = await factory.PrepareTouchAsync("op-2", CacheName, Key, Expiration, 2UL, cancellationToken);

        clock.Advance(TimeSpan.FromMinutes(2));
        await ApplyAsync(cache, touch, cancellationToken);
        clock.Advance(TimeSpan.FromMinutes(2));
        await ApplyAsync(cache, touch, cancellationToken);

        var entry = await cache.GetEntryAsync(CacheName, Key, cancellationToken);
        _ = await Assert.That(entry?.ExpiresUtc).IsEqualTo(deadline);
    }

    /// <summary>A Touch applied after its pinned deadline writes that deadline, so the entry is expired instead of being kept alive from the apply time.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PastTouchDeadlineExpiresEntry(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var cache = NewCache(clock);
        var factory = new ReplicaMutationFactory(cache, "g1", 1UL, clock);
        await ApplyAsync(cache, factory.PrepareSet("op-1", CacheName, Key, new NodeCacheEntry<object?> { Value = "v1" }, 1UL), cancellationToken);
        var touch = await factory.PrepareTouchAsync("op-2", CacheName, Key, TimeSpan.FromMinutes(1), 2UL, cancellationToken);

        clock.Advance(TimeSpan.FromMinutes(10));
        await ApplyAsync(cache, touch, cancellationToken);

        var read = await cache.GetValueAsync(CacheName, Key, cancellationToken);
        _ = await Assert.That(read.Found).IsFalse().Because("A Touch whose deadline passed must expire the entry, not leave it untouched.");
    }

    /// <summary>A retry of a Touch prepared at a later time keeps the operation fingerprint: the deadline is not part of the operation identity.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchFingerprintIgnoresPrepareTime(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var cache = NewCache(clock);
        var factory = new ReplicaMutationFactory(cache, "g1", 1UL, clock);
        await ApplyAsync(cache, factory.PrepareSet("op-0", CacheName, Key, new NodeCacheEntry<object?> { Value = "v1" }, 1UL), cancellationToken);

        var first = await factory.PrepareTouchAsync("op-1", CacheName, Key, Expiration, 1UL, cancellationToken);
        clock.Advance(TimeSpan.FromHours(1));
        var retry = await factory.PrepareTouchAsync("op-1", CacheName, Key, Expiration, 1UL, cancellationToken);

        _ = await Assert.That(retry.OperationFingerprint.Span.SequenceEqual(first.OperationFingerprint.Span)).IsTrue();
        _ = await Assert.That(Decode(retry).ExpiresUtcTicks).IsNotEqualTo(Decode(first).ExpiresUtcTicks);
    }

    /// <summary>A Set or TryAdd with a relative expiration, applied twice with time advanced in between, sets the same deadline both times.</summary>
    /// <param name="kind">The replicated write kind.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(ReplicaMutationKinds.Set)]
    [Arguments(ReplicaMutationKinds.TryAdd)]
    public async Task RelativeExpirationReplaysToSameDeadline(string kind, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var deadline = clock.GetUtcNow().UtcDateTime.Add(Expiration);
        var factory = new ReplicaMutationFactory(NewCache(clock), "g1", 1UL, clock);
        var entry = new NodeCacheEntry<object?> { Value = "v1", Expiration = Expiration };
        var write = string.Equals(kind, ReplicaMutationKinds.Set, StringComparison.Ordinal) ? factory.PrepareSet("op-1", CacheName, Key, entry, 1UL)
            : await factory.PrepareTryAddAsync("op-1", CacheName, Key, entry, 1UL, cancellationToken);

        clock.Advance(TimeSpan.FromMinutes(1));
        var first = NewCache(clock);
        await ApplyAsync(first, write, cancellationToken);
        clock.Advance(TimeSpan.FromMinutes(3));
        var replayed = NewCache(clock);
        await ApplyAsync(replayed, write, cancellationToken);

        _ = await Assert.That((await first.GetEntryAsync(CacheName, Key, cancellationToken))?.ExpiresUtc).IsEqualTo(deadline);
        _ = await Assert.That((await replayed.GetEntryAsync(CacheName, Key, cancellationToken))?.ExpiresUtc).IsEqualTo(deadline);
    }

    /// <summary>A write with both expirations pins the earlier deadline, and a write with neither pins none.</summary>
    [Test]
    public async Task SetPinsEarlierDeadline()
    {
        var clock = new FakeTimeProvider();
        var now = clock.GetUtcNow().UtcDateTime;
        var factory = new ReplicaMutationFactory(NewCache(clock), "g1", 1UL, clock);

        var absoluteFirst = factory.PrepareSet("op-1", CacheName, Key, new NodeCacheEntry<object?> { Value = "v1", ExpiresUtc = now.AddMinutes(2), Expiration = Expiration }, 1UL);
        var relativeFirst = factory.PrepareSet("op-2", CacheName, Key, new NodeCacheEntry<object?> { Value = "v1", ExpiresUtc = now.AddHours(1), Expiration = Expiration }, 2UL);
        var never = factory.PrepareSet("op-3", CacheName, Key, new NodeCacheEntry<object?> { Value = "v1" }, 3UL);

        _ = await Assert.That(Decode(absoluteFirst).ExpiresUtcTicks).IsEqualTo(now.AddMinutes(2).Ticks);
        _ = await Assert.That(Decode(relativeFirst).ExpiresUtcTicks).IsEqualTo(now.Add(Expiration).Ticks);
        _ = await Assert.That(Decode(never).ExpiresUtcTicks).IsEqualTo(0L);
    }

    private static Task ApplyAsync(ILogicalNamespacedCache<object?> cache, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
        ReplicaCacheApplier.ApplyAsync(cache, Decode(mutation), cancellationToken);

    private static ReplicaLogRecord Decode(PreparedReplicaMutation mutation) =>
        ReplicaLogCodec.Decode(mutation.CanonicalPayload) ?? ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException("The prepared record must decode."));

    private static ClientCache<object?> NewCache(TimeProvider clock)
    {
        var physical = new PhysicalCache<object?>(clock);
        return new ClientCache<object?>(physical, physical);
    }
}
