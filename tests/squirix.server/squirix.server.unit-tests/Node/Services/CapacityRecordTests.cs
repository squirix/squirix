using System;
using System.Buffers.Binary;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Compatibility and replay of a leader-selected eviction.</summary>
[Immutable]
public sealed class CapacityRecordTests : ServerUnitTestBase
{
    /// <summary>A restarted copy with only the deletion applied finishes the record. A follower with different access history makes the same change.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PartialApplyReplaysLeaderDecision(CancellationToken cancellationToken)
    {
        const ushort standardVersion = 8;
        const ushort evictionVersion = 9;
        var options = new EvictionOptions { Capacity = 2 };
        var leader = Create(options);
        var follower = Create(options);
        var local = new ClientCache<object?>(leader, leader);
        var remote = new ClientCache<object?>(follower, follower);
        var factory = new ReplicaMutationFactory(local, "group", 1, TimeProvider.System, NullLogger.Instance, leader.SelectCapacityVictim);
        const string scope = "orders";
        var a = new CacheKey(scope, "a");
        var b = new CacheKey(scope, "b");
        var c = new CacheKey(scope, "c");
        var value = new NodeCacheEntry<object?>("value");
        foreach (var key in new[] { a, b })
        {
            var normal = factory.PrepareSet(key.Key, scope, key.Key, value, 1).CanonicalPayload;
            _ = await Assert.That(BinaryPrimitives.ReadUInt16LittleEndian(normal.Span)).IsEqualTo(standardVersion);
            var original = ReplicaLogCodec.DecodeBorrowed(normal)!.Value;
            await ReplicaCacheApplier.ApplyAsync(local, in original, cancellationToken);
            await ReplicaCacheApplier.ApplyAsync(remote, in original, cancellationToken);
        }

        _ = leader.GetValue(a, cancellationToken);
        _ = follower.GetValue(b, cancellationToken);
        var bytes = factory.PrepareSet("insert", scope, c.Key, value, 3).CanonicalPayload;
        _ = await Assert.That(BinaryPrimitives.ReadUInt16LittleEndian(bytes.Span)).IsEqualTo(evictionVersion);
        _ = await Assert.That(ReplicaLogCodec.IsWellFormed(bytes.Span, 3, 1)).IsTrue();
        _ = await Assert.That(ReplicaLogCodec.IsWellFormed(bytes.Span[..^1], 3, 1)).IsFalse();
        _ = await Assert.That(ReplicaLogCodec.DecodeBorrowed(bytes[..^1])).IsNull();
        var record = ReplicaLogCodec.DecodeBorrowed(bytes)!.Value;
        _ = await Assert.That(record.EvictedKey).IsEqualTo(b.Key);

        // The node journal recovered the deletion, but the process stopped before journaling the insertion.
        _ = leader.Remove(b, cancellationToken);
        await ReplicaCacheApplier.ApplyAsync(local, in record, cancellationToken);
        await ReplicaCacheApplier.ApplyAsync(remote, in record, cancellationToken);
        await ReplicaCacheApplier.ApplyAsync(remote, in record, cancellationToken);
        foreach (var copy in new[] { leader, follower })
        {
            _ = await Assert.That(copy.GetValue(a, cancellationToken).Found).IsTrue();
            _ = await Assert.That(copy.GetValue(b, cancellationToken).Found).IsFalse();
            _ = await Assert.That(copy.GetValue(c, cancellationToken).Value).IsEqualTo("value");
        }
    }

    /// <summary>A new key that loses LFU admission is absent after both apply and replay. The retained resident can still be replaced.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LfuCanDiscardIncomingReplicatedKey(CancellationToken cancellationToken)
    {
        const string scope = "users";
        var physical = Create(new EvictionOptions { Capacity = 1, Policy = EvictionPolicyType.Lfu });
        var local = new ClientCache<object?>(physical, physical);
        var factory = new ReplicaMutationFactory(local, "group", 1, TimeProvider.System, NullLogger.Instance, physical.SelectCapacityVictim);
        var first = new CacheKey(scope, "first");
        var second = new CacheKey(scope, "second");
        var value = new NodeCacheEntry<object?>("initial");
        physical.Set(first, value, cancellationToken);
        _ = physical.GetValue(first, cancellationToken);

        var incoming = await factory.PrepareTryAddAsync("add", scope, second.Key, value, 1, cancellationToken);
        var rejected = ReplicaLogCodec.DecodeBorrowed(incoming.CanonicalPayload)!.Value;
        _ = await Assert.That(rejected.EvictedKey).IsEqualTo(second.Key);
        await ReplicaCacheApplier.ApplyAsync(local, in rejected, cancellationToken);
        await ReplicaCacheApplier.ApplyAsync(local, in rejected, cancellationToken);
        _ = await Assert.That(physical.GetValue(second, cancellationToken).Found).IsFalse();
        _ = await Assert.That(physical.GetValue(first, cancellationToken).Value).IsEqualTo("initial");

        var update = factory.PrepareSet("replace", scope, first.Key, new NodeCacheEntry<object?>("replacement"), 2);
        var record = ReplicaLogCodec.DecodeBorrowed(update.CanonicalPayload)!.Value;
        _ = await Assert.That(record.EvictedKey).IsNull();
        await ReplicaCacheApplier.ApplyAsync(local, in record, cancellationToken);
        _ = await Assert.That(physical.GetValue(first, cancellationToken).Value).IsEqualTo("replacement");
    }

    private static PhysicalCache<object?> Create(EvictionOptions options) =>
        new(TimeProvider.System, new CacheEntrySizeEstimator<object?>(), options, CacheExpiryAuthority.CommittedRecords);
}
