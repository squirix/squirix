using System;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Verifies the stored distributed expiration policy and the owner's initial upsert decision.</summary>
public sealed class DistributedCachePolicyTests : ServerUnitTestBase
{
    /// <summary>An explicit Unix epoch cap and empty payload have an unambiguous representation.</summary>
    [Test]
    public async Task EmptyPayloadKeepsExplicitEpochCap()
    {
        var policy = new DistributedCachePolicy(0, null);
        var encoded = policy.Encode([]);
        byte[] golden = [0x53, 0x51, 0x44, 0x43, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        _ = await Assert.That(encoded.AsSpan().SequenceEqual(golden)).IsTrue();
        _ = await Assert.That(DistributedCachePolicy.TryRead(encoded, out var decoded)).IsTrue();
        _ = await Assert.That(decoded).IsEqualTo(policy);
    }

    /// <summary>Future or internally inconsistent headers cannot be used as renewal policies.</summary>
    [Test]
    public async Task MalformedPolicyHeaderIsRejected()
    {
        var encoded = new DistributedCachePolicy(1, null).Encode([]);
        _ = await Assert.That(DistributedCachePolicy.TryRead(encoded.AsSpan(0, 21), out _)).IsFalse();
        encoded[4] = 2;
        _ = await Assert.That(DistributedCachePolicy.TryRead(encoded, out _)).IsFalse();
        encoded[4] = 1;
        encoded[5] = 4;
        _ = await Assert.That(DistributedCachePolicy.TryRead(encoded, out _)).IsFalse();
        encoded[5] = 0;
        _ = await Assert.That(DistributedCachePolicy.TryRead(encoded, out _)).IsFalse();
        encoded[5] = 3;
        _ = await Assert.That(DistributedCachePolicy.TryRead(encoded, out _)).IsFalse();
    }

    /// <summary>The prepare step carries owned bytes and the exact chosen deadline together.</summary>
    [Test]
    public async Task SetPinsDeadlineAndOwnsPayload()
    {
        var now = DateTime.UnixEpoch.AddTicks(1000);
        var policy = new DistributedCachePolicy(2, 50_000);
        byte[] source = [0, 255, 128];
        var write = new DistributedCacheWrite(in policy, source);
        var decision = ReplicaMutationDecisions.DecideSet(new NodeCacheEntry<object?>(write, expiresUtc: write.DeadlineForSizing), now);
        source[1] = 0;
        var entry = decision.Entry!.ObjectEntry;
        var stored = (await Assert.That(entry.Value).IsTypeOf<byte[]>())!;
        _ = await Assert.That(DistributedCachePolicy.TryRead(stored, out var decoded)).IsTrue();
        _ = await Assert.That(decoded).IsEqualTo(policy);
        _ = await Assert.That(stored.AsSpan(22).SequenceEqual(new byte[] { 0, 255, 128 })).IsTrue();
        var expectedDeadline = DateTime.UnixEpoch.AddMilliseconds(2);
        _ = await Assert.That(decision.ExpiresUtcTicks).IsEqualTo(expectedDeadline.Ticks);
        _ = await Assert.That(entry.ExpiresUtc).IsNull();
        _ = await Assert.That(entry.Expiration).IsNull();
    }

    /// <summary>A renewal cannot shorten the current deadline or cross its cap.</summary>
    [Test]
    public async Task RenewalFloorsAndClampsDeadline()
    {
        var policy = new DistributedCachePolicy(4, 16_000);
        var epoch = DateTime.UnixEpoch;
        _ = await Assert.That(policy.TryChooseDeadline(epoch.AddTicks(1000), null, out var initial)).IsTrue();
        _ = await Assert.That(initial).IsEqualTo(epoch.AddMilliseconds(1));
        _ = await Assert.That(policy.TryChooseDeadline(epoch, epoch.AddMilliseconds(3), out var kept)).IsTrue();
        _ = await Assert.That(kept).IsEqualTo(epoch.AddMilliseconds(3));
        _ = await Assert.That(policy.TryChooseDeadline(epoch.AddMilliseconds(3), kept, out var clamped)).IsTrue();
        _ = await Assert.That(clamped).IsEqualTo(epoch.AddMilliseconds(4));
        _ = await Assert.That(policy.TryChooseDeadline(DateTime.MaxValue, null, out _)).IsFalse();
    }

    /// <summary>A result-only access distinguishes a present empty value from a missing key.</summary>
    [Test]
    public async Task EmptyAccessRemainsPresent()
    {
        var policy = new DistributedCachePolicy(null, null);
        var entry = new NodeCacheEntry<object?>(policy.Encode([]));
        var present = ReplicaMutationDecisions.DecideDistributedAccess(entry, DateTime.UnixEpoch, false);
        var missing = ReplicaMutationDecisions.DecideDistributedAccess(null, DateTime.UnixEpoch, false);
        _ = await Assert.That(present.Entry).IsNull();
        var payload = ReplicaOutcomeCodec.DecodeAccess(present.Outcome);
        _ = await Assert.That(payload).IsNotNull();
        _ = await Assert.That(payload.GetValueOrDefault().Length).IsEqualTo(0);
        _ = await Assert.That(ReplicaOutcomeCodec.DecodeAccess(missing.Outcome)).IsNull();
    }

    /// <summary>An access at the inclusive deadline records expiry even if its sliding interval could extend it.</summary>
    [Test]
    public async Task AccessAtDeadlineDecidesExpiry()
    {
        var deadline = DateTime.UnixEpoch.AddSeconds(1);
        var policy = new DistributedCachePolicy(null, TimeSpan.FromSeconds(10).Ticks);
        var entry = new NodeCacheEntry<object?>(policy.Encode([1]), expiresUtc: deadline);
        var decision = ReplicaMutationDecisions.DecideDistributedAccess(entry, deadline, false);
        _ = await Assert.That(decision.Entry).IsNull();
        _ = await Assert.That(ReplicaOutcomeCodec.DecodeAccess(decision.Outcome)).IsNull();
        _ = await Assert.That(decision.ExpiresUtcTicks).IsEqualTo(deadline.Ticks);
    }
}
