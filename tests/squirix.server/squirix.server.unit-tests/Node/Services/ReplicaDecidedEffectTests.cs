using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A replicated record carries the effect the leader decided at prepare time: applying it later, on any clock, writes the decided
/// entry even when the key expired in between.
/// </summary>
public sealed class ReplicaDecidedEffectTests : ServerUnitTestBase
{
    private const string CacheName = "cache";
    private const string Key = "k";

    private static readonly TimeSpan Ttl = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan Wait = TimeSpan.FromMinutes(5);

    /// <summary>The same records applied to two caches whose clocks are apart write identical entries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SkewedClocksApplyIdenticalEntries(CancellationToken cancellationToken)
    {
        var leaderClock = new FakeTimeProvider();
        var leader = new Harness(leaderClock);
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["t"] = "1" }.ToFrozenDictionary(StringComparer.Ordinal);
        var records = new List<PreparedReplicaMutation>();
        await LeaderAppliesAsync(leader, records, leader.Factory.PrepareSet("op-1", CacheName, Key, new NodeCacheEntry<object?>("v1", 3, null, Ttl * 10, tags), 1UL), cancellationToken);
        await LeaderAppliesAsync(leader, records, await leader.Factory.PrepareTouchAsync("op-2", CacheName, Key, Ttl * 20, 2UL, cancellationToken), cancellationToken);
        await LeaderAppliesAsync(leader, records, await leader.Factory.PrepareUpdateAsync("op-3", CacheName, Key, "v2", 3UL, cancellationToken), cancellationToken);
        await LeaderAppliesAsync(leader, records, await leader.Factory.PrepareTryAddAsync("op-4", CacheName, Key, new NodeCacheEntry<object?>("v3"), 4UL, cancellationToken), cancellationToken);

        var early = new Harness(new FakeTimeProvider(leaderClock.GetUtcNow().AddSeconds(-30)));
        var late = new Harness(new FakeTimeProvider(leaderClock.GetUtcNow().AddSeconds(30)));
        foreach (var record in records)
        {
            await early.ApplyAsync(record, cancellationToken);
            await late.ApplyAsync(record, cancellationToken);
        }

        var expected = await leader.RawAsync(cancellationToken);
        await AssertSameEntryAsync(await early.RawAsync(cancellationToken), expected);
        await AssertSameEntryAsync(await late.RawAsync(cancellationToken), expected);
    }

    /// <summary>A removal of the expiration decided while the key was live clears the deadline even when the key expired before the apply.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpirationExpiresBeforeApply(CancellationToken cancellationToken)
    {
        var harness = new Harness(new FakeTimeProvider());
        await harness.SeedAsync(new NodeCacheEntry<object?>("v1", 1, harness.Now.Add(Ttl)), cancellationToken);
        var prepared = await harness.Factory.PrepareRemoveExpirationAsync("op-2", CacheName, Key, 2UL, cancellationToken);

        harness.Clock.Advance(Wait);
        await harness.ApplyAsync(prepared, cancellationToken);

        _ = await Assert.That(ReplicaOutcomeCodec.DecodeApplied(prepared.OutcomePayload)).IsTrue();
        var raw = await harness.RawAsync(cancellationToken);
        _ = await Assert.That(raw?.ExpiresUtc).IsNull();
        _ = await Assert.That(raw?.Value).IsEqualTo("v1");
    }

    /// <summary>A remove decided while the key was live deletes it when applied after the key expired.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpiringBeforeApplyDeletes(CancellationToken cancellationToken)
    {
        var harness = new Harness(new FakeTimeProvider());
        await harness.SeedAsync(new NodeCacheEntry<object?>("v1", 1, harness.Now.Add(Ttl)), cancellationToken);
        var prepared = await harness.Factory.PrepareRemoveAsync("op-2", CacheName, Key, 2UL, cancellationToken);

        harness.Clock.Advance(Wait);
        await harness.ApplyAsync(prepared, cancellationToken);

        _ = await Assert.That(ReplicaOutcomeCodec.DecodeApplied(prepared.OutcomePayload)).IsTrue();
        _ = await Assert.That(await harness.RawAsync(cancellationToken)).IsNull();
    }

    /// <summary>A remove decided while the key was absent still deletes the key, so a replica that holds it converges.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveOfAbsentKeyStillDeletes(CancellationToken cancellationToken)
    {
        var leader = new Harness(new FakeTimeProvider());
        var replica = new Harness(new FakeTimeProvider());
        await replica.SeedAsync(new NodeCacheEntry<object?>("v1"), cancellationToken);
        var prepared = await leader.Factory.PrepareRemoveAsync("op-1", CacheName, Key, 1UL, cancellationToken);

        await replica.ApplyAsync(prepared, cancellationToken);

        _ = await Assert.That(ReplicaOutcomeCodec.DecodeApplied(prepared.OutcomePayload)).IsFalse();
        _ = await Assert.That(await replica.RawAsync(cancellationToken)).IsNull();
    }

    /// <summary>A try-add decided false while the key was live stays rejected when applied after the key expired.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddExpiringBeforeApplyStaysRejected(CancellationToken cancellationToken)
    {
        var harness = new Harness(new FakeTimeProvider());
        await harness.SeedAsync(new NodeCacheEntry<object?>("a", 1, harness.Now.Add(Ttl)), cancellationToken);
        var prepared = await harness.Factory.PrepareTryAddAsync("op-2", CacheName, Key, new NodeCacheEntry<object?>("b"), 2UL, cancellationToken);

        harness.Clock.Advance(Wait);
        await harness.ApplyAsync(prepared, cancellationToken);

        _ = await Assert.That(ReplicaOutcomeCodec.DecodeApplied(prepared.OutcomePayload)).IsFalse();
        _ = await Assert.That((await harness.RawAsync(cancellationToken))?.Value).IsEqualTo("a");
    }

    /// <summary>The leader decides expiry once: the tombstone it commits deletes the key on replicas whose clocks are apart, and before it the key stays.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpireRecordDeletesOnSkewedClocks(CancellationToken cancellationToken)
    {
        var leaderClock = new FakeTimeProvider();
        var leader = new Harness(leaderClock, CacheExpiryAuthority.CommittedRecords);
        var set = leader.Factory.PrepareSet("op-1", CacheName, Key, new NodeCacheEntry<object?>("v1", 1, null, Ttl), 1UL);
        await leader.ApplyAsync(set, cancellationToken);
        var early = new Harness(new FakeTimeProvider(leaderClock.GetUtcNow().AddSeconds(-30)), CacheExpiryAuthority.CommittedRecords);
        var late = new Harness(new FakeTimeProvider(leaderClock.GetUtcNow().AddSeconds(90)), CacheExpiryAuthority.CommittedRecords);
        await early.ApplyAsync(set, cancellationToken);
        await late.ApplyAsync(set, cancellationToken);
        _ = await Assert.That((await late.Cache.GetEntryAsync(CacheName, Key, cancellationToken))?.Value).IsEqualTo("v1");

        leaderClock.Advance(Ttl);
        var (tombstone, _) = await leader.Factory.PrepareExpireAsync(CacheName, Key, 2UL, cancellationToken);
        await early.ApplyAsync(tombstone!, cancellationToken);
        await late.ApplyAsync(tombstone!, cancellationToken);

        _ = await Assert.That(await early.RawAsync(cancellationToken)).IsNull();
        _ = await Assert.That(await late.RawAsync(cancellationToken)).IsNull();
    }

    /// <summary>A tombstone takes the identity of the expired entry under the scope reserved for expiration, and reports nothing applied.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpireRecordCarriesExpiredIdentity(CancellationToken cancellationToken)
    {
        var harness = new Harness(new FakeTimeProvider(), CacheExpiryAuthority.CommittedRecords);
        var deadline = harness.Now.Add(Ttl);
        await harness.SeedAsync(new NodeCacheEntry<object?>("v1", 4, deadline), cancellationToken);
        harness.Clock.Advance(Ttl);

        var (tombstone, current) = await harness.Factory.PrepareExpireAsync(CacheName, Key, 2UL, cancellationToken);

        var record = ReplicaLogCodec.Decode(tombstone!.CanonicalPayload);
        _ = await Assert.That(current).IsNull();
        _ = await Assert.That(tombstone.OperationScope).IsEqualTo(ReplicaExpirationOperationId.OperationScope);
        _ = await Assert.That(tombstone.OperationId).IsEqualTo(ReplicaExpirationOperationId.Create("g1", CacheName, Key, 4, deadline));
        _ = await Assert.That(record?.MutationKind).IsEqualTo(ReplicaMutationKinds.Expire);
        _ = await Assert.That(record?.ExpiresUtcTicks).IsEqualTo(deadline.Ticks);
        _ = await Assert.That(ReplicaOutcomeCodec.DecodeApplied(tombstone.OutcomePayload)).IsFalse();
    }

    /// <summary>A key that is live, or absent, needs no tombstone; the live entry is handed back.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpireOfLiveKeyPreparesNothing(CancellationToken cancellationToken)
    {
        var harness = new Harness(new FakeTimeProvider(), CacheExpiryAuthority.CommittedRecords);
        var (absentTombstone, absentCurrent) = await harness.Factory.PrepareExpireAsync(CacheName, Key, 1UL, cancellationToken);
        await harness.SeedAsync(new NodeCacheEntry<object?>("v1", 1, harness.Now.Add(Ttl)), cancellationToken);

        var (tombstone, current) = await harness.Factory.PrepareExpireAsync(CacheName, Key, 2UL, cancellationToken);

        _ = await Assert.That(absentTombstone).IsNull();
        _ = await Assert.That(absentCurrent).IsNull();
        _ = await Assert.That(tombstone).IsNull();
        _ = await Assert.That(current?.Value).IsEqualTo("v1");
    }

    /// <summary>An update, a touch and an expiration removal that find the key expired delete it and report nothing applied.</summary>
    /// <param name="kind">The conditional mutation kind.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    [Arguments(ReplicaMutationKinds.Update)]
    [Arguments(ReplicaMutationKinds.Touch)]
    [Arguments(ReplicaMutationKinds.RemoveExpiration)]
    public async Task ExpiredKeyFoldsIntoConditionalWrite(string kind, CancellationToken cancellationToken)
    {
        var harness = new Harness(new FakeTimeProvider(), CacheExpiryAuthority.CommittedRecords);
        await harness.SeedAsync(new NodeCacheEntry<object?>("v1", 1, harness.Now.Add(Ttl)), cancellationToken);
        harness.Clock.Advance(Ttl);

        var prepared = kind switch
        {
            ReplicaMutationKinds.Update => await harness.Factory.PrepareUpdateAsync("op-2", CacheName, Key, "v2", 2UL, cancellationToken),
            ReplicaMutationKinds.Touch => await harness.Factory.PrepareTouchAsync("op-2", CacheName, Key, Ttl, 2UL, cancellationToken),
            _ => await harness.Factory.PrepareRemoveExpirationAsync("op-2", CacheName, Key, 2UL, cancellationToken),
        };
        await harness.ApplyAsync(prepared, cancellationToken);

        _ = await Assert.That(ReplicaOutcomeCodec.DecodeApplied(prepared.OutcomePayload)).IsFalse();
        _ = await Assert.That(await harness.RawAsync(cancellationToken)).IsNull();
    }

    /// <summary>A try-add writes over an expired key, and a remove of it deletes it but reports nothing removed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredKeyCountsAsAbsentForAddAndRemove(CancellationToken cancellationToken)
    {
        var harness = new Harness(new FakeTimeProvider(), CacheExpiryAuthority.CommittedRecords);
        await harness.SeedAsync(new NodeCacheEntry<object?>("v1", 1, harness.Now.Add(Ttl)), cancellationToken);
        harness.Clock.Advance(Ttl);

        var add = await harness.Factory.PrepareTryAddAsync("op-2", CacheName, Key, new NodeCacheEntry<object?>("v2", 1, harness.Now.Add(Ttl)), 2UL, cancellationToken);
        await harness.ApplyAsync(add, cancellationToken);
        var added = await harness.RawAsync(cancellationToken);
        harness.Clock.Advance(Ttl);
        var remove = await harness.Factory.PrepareRemoveAsync("op-3", CacheName, Key, 3UL, cancellationToken);
        await harness.ApplyAsync(remove, cancellationToken);

        _ = await Assert.That(ReplicaOutcomeCodec.DecodeApplied(add.OutcomePayload)).IsTrue();
        _ = await Assert.That(added?.Value).IsEqualTo("v2");
        _ = await Assert.That(ReplicaOutcomeCodec.DecodeApplied(remove.OutcomePayload)).IsFalse();
        _ = await Assert.That(await harness.RawAsync(cancellationToken)).IsNull();
    }

    /// <summary>A sub-millisecond TTL is rounded up to the next whole millisecond, so the entry is still live when it is decided.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SubMillisecondTtlRoundsUp(CancellationToken cancellationToken)
    {
        var harness = new Harness(new FakeTimeProvider());
        var prepared = harness.Factory.PrepareSet("op-1", CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, TimeSpan.FromTicks(5000)), 1UL);

        await harness.ApplyAsync(prepared, cancellationToken);

        var record = ReplicaLogCodec.Decode(prepared.CanonicalPayload);
        _ = await Assert.That(record?.ExpiresUtcTicks).IsEqualTo(harness.Now.Ticks + TimeSpan.TicksPerMillisecond);
        _ = await Assert.That((await harness.Cache.GetEntryAsync(CacheName, Key, cancellationToken))?.Value).IsEqualTo("v");
    }

    /// <summary>A touch decided while the key was live writes the decided deadline even when the key expired before the apply.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchExpiringBeforeApplyKeepsDeadline(CancellationToken cancellationToken)
    {
        var harness = new Harness(new FakeTimeProvider());
        await harness.SeedAsync(new NodeCacheEntry<object?>("v1", 1, harness.Now.Add(Ttl)), cancellationToken);
        var decided = harness.Now.Add(Ttl * 10);
        var prepared = await harness.Factory.PrepareTouchAsync("op-2", CacheName, Key, Ttl * 10, 2UL, cancellationToken);

        harness.Clock.Advance(Wait);
        await harness.ApplyAsync(prepared, cancellationToken);

        _ = await Assert.That(ReplicaOutcomeCodec.DecodeApplied(prepared.OutcomePayload)).IsTrue();
        var raw = await harness.RawAsync(cancellationToken);
        _ = await Assert.That(raw?.ExpiresUtc).IsEqualTo(decided);
        _ = await Assert.That((await harness.Cache.GetEntryAsync(CacheName, Key, cancellationToken))?.Value).IsEqualTo("v1");
    }

    /// <summary>A touch keeps the version and the tags of the entry it decided on.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchKeepsVersionAndTags(CancellationToken cancellationToken)
    {
        var harness = new Harness(new FakeTimeProvider());
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["t"] = "1" }.ToFrozenDictionary(StringComparer.Ordinal);
        await harness.SeedAsync(new NodeCacheEntry<object?>("v1", 7, null, null, tags), cancellationToken);
        var prepared = await harness.Factory.PrepareTouchAsync("op-2", CacheName, Key, Ttl, 2UL, cancellationToken);

        await harness.ApplyAsync(prepared, cancellationToken);

        var raw = await harness.RawAsync(cancellationToken);
        _ = await Assert.That(raw?.Version).IsEqualTo(7L);
        _ = await Assert.That(raw?.Tags?["t"]).IsEqualTo("1");
    }

    /// <summary>An update decided while the key was live writes the new value with the decided deadline even when the key expired before the apply.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateExpiringBeforeApplyWritesEntry(CancellationToken cancellationToken)
    {
        var harness = new Harness(new FakeTimeProvider());
        var deadline = harness.Now.Add(Ttl);
        await harness.SeedAsync(new NodeCacheEntry<object?>("v1", 1, deadline), cancellationToken);
        var prepared = await harness.Factory.PrepareUpdateAsync("op-2", CacheName, Key, "v2", 2UL, cancellationToken);

        harness.Clock.Advance(Wait);
        await harness.ApplyAsync(prepared, cancellationToken);

        _ = await Assert.That(ReplicaOutcomeCodec.DecodeApplied(prepared.OutcomePayload)).IsTrue();
        var raw = await harness.RawAsync(cancellationToken);
        _ = await Assert.That(raw?.Value).IsEqualTo("v2");
        _ = await Assert.That(raw?.ExpiresUtc).IsEqualTo(deadline);
    }

    /// <summary>An update keeps the version and the tags of the entry it decided on.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateKeepsVersionAndTags(CancellationToken cancellationToken)
    {
        var harness = new Harness(new FakeTimeProvider());
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["t"] = "1" }.ToFrozenDictionary(StringComparer.Ordinal);
        await harness.SeedAsync(new NodeCacheEntry<object?>("v1", 7, null, null, tags), cancellationToken);
        var prepared = await harness.Factory.PrepareUpdateAsync("op-2", CacheName, Key, "v2", 2UL, cancellationToken);

        await harness.ApplyAsync(prepared, cancellationToken);

        var raw = await harness.RawAsync(cancellationToken);
        _ = await Assert.That(raw?.Value).IsEqualTo("v2");
        _ = await Assert.That(raw?.Version).IsEqualTo(7L);
        _ = await Assert.That(raw?.Tags?["t"]).IsEqualTo("1");
    }

    private static async Task LeaderAppliesAsync(Harness leader, List<PreparedReplicaMutation> records, PreparedReplicaMutation prepared, CancellationToken cancellationToken)
    {
        await leader.ApplyAsync(prepared, cancellationToken);
        records.Add(prepared);
    }

    private static async Task AssertSameEntryAsync(NodeCacheEntry<object?>? actual, NodeCacheEntry<object?>? expected)
    {
        _ = await Assert.That(actual).IsNotNull();
        _ = await Assert.That(expected).IsNotNull();
        _ = await Assert.That(actual!.Value).IsEqualTo(expected!.Value);
        _ = await Assert.That(actual.ExpiresUtc).IsEqualTo(expected.ExpiresUtc);
        _ = await Assert.That(actual.Version).IsEqualTo(expected.Version);
        _ = await Assert.That(actual.Tags?["t"]).IsEqualTo(expected.Tags?["t"]);
    }

    /// <summary>A cache on its own clock with the factory that prepares against it.</summary>
    private sealed class Harness
    {
        private readonly PhysicalCache<object?> _physical;

        internal Harness(FakeTimeProvider clock, CacheExpiryAuthority expiry = CacheExpiryAuthority.LocalClock)
        {
            Clock = clock;
            _physical = new PhysicalCache<object?>(clock, expiry: expiry);
            Cache = new ClientCache<object?>(_physical, _physical);
            Factory = new ReplicaMutationFactory(Cache, "g1", 1UL, clock, NullLogger.Instance);
        }

        internal ClientCache<object?> Cache { get; }

        internal FakeTimeProvider Clock { get; }

        internal ReplicaMutationFactory Factory { get; }

        internal DateTime Now => Clock.GetUtcNow().UtcDateTime;

        internal Task ApplyAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
            ReplicaCacheApplier.ApplyAsync(
                Cache,
                ReplicaLogCodec.Decode(mutation.CanonicalPayload) ?? ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException("The prepared record must decode.")),
                cancellationToken);

        internal ValueTask<NodeCacheEntry<object?>?> RawAsync(CancellationToken cancellationToken) => _physical.RawReader.GetEntryRawAsync(new CacheKey(CacheName, Key), cancellationToken);

        internal Task SeedAsync(NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
            ApplyAsync(Factory.PrepareSet("seed", CacheName, Key, entry, 1UL), cancellationToken);
    }
}
