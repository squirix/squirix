using System;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Protects fixed-size mutation outcomes and rejects response-bearing older data.</summary>
public sealed class ReplicaOutcomeCodecTests : ServerUnitTestBase
{
    /// <summary>A successful remove keeps no part of the deleted value.</summary>
    /// <param name="length">Deleted payload size.</param>
    [Test]
    [Arguments(0)]
    [Arguments(65536)]
    [Arguments(4190208)]
    public async Task RemoveRetainsOnlyBoolean(int length)
    {
        var now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var entry = new NodeCacheEntry<object?>(new byte[length], 123, now.AddHours(1));

        var decision = ReplicaMutationDecisions.DecideRemove(entry, now);

        await SequenceAssert.EqualMemoryAsync(new byte[] { 1, 0, 0, 0, 0 }, decision.Outcome);
        _ = await Assert.That(ReplicaOutcomeCodec.DecodeApplied(decision.Outcome)).IsTrue();
        _ = await Assert.That(decision.Entry).IsNull();
        _ = await Assert.That(decision.ExpiresUtcTicks).IsEqualTo(0L);
    }

    /// <summary>Absent and expired entries report false without retaining a value.</summary>
    /// <param name="expired">Whether a stored entry has expired.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task MissingRemoveRetainsOnlyBoolean(bool expired)
    {
        var now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var entry = expired ? new NodeCacheEntry<object?>(new byte[65536], 1, now) : null;

        var decision = ReplicaMutationDecisions.DecideRemove(entry, now);

        await SequenceAssert.EqualMemoryAsync(new byte[5], decision.Outcome);
        _ = await Assert.That(ReplicaOutcomeCodec.DecodeApplied(decision.Outcome)).IsFalse();
    }

    /// <summary>Noncanonical flags, truncated data and payload-bearing outcomes fail closed.</summary>
    /// <param name="kind">Malformed encoding.</param>
    [Test]
    [Arguments("truncated")]
    [Arguments("flag")]
    [Arguments("length")]
    [Arguments("payload")]
    public async Task RejectsMalformedOrPayloadBearingOutcome(string kind)
    {
        byte[] payload = kind switch
        {
            "truncated" => [1, 0],
            "flag" => [2, 0, 0, 0, 0],
            "length" => [1, 1, 0, 0, 0],
            "payload" => [1, 1, 0, 0, 0, 42],
            _ => [],
        };

        _ = await Assert.That(ReplicaOutcomeCodec.TryDecode(payload, out _, out _)).IsFalse();
        var failure = NodeExceptionAssert.For<InvalidOperationException>().Throws(payload, static value => _ = ReplicaOutcomeCodec.DecodeApplied(value));
        _ = await Assert.That(failure.Message).IsEqualTo("Committed outcome payload is malformed.");
    }
}
