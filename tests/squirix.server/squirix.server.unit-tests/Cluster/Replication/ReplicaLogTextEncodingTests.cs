using System;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Text shortcuts preserve canonical bytes, strict decoding and bounded string reuse.</summary>
public sealed class ReplicaLogTextEncodingTests : ServerUnitTestBase
{
    /// <summary>Known wire kinds reuse existing strings on both owning and borrowed decode paths.</summary>
    /// <param name="recordKind">The record kind.</param>
    /// <param name="mutationKind">The mutation kind.</param>
    [Test]
    [Arguments("UserMutation", "set")]
    [Arguments("UserMutation", "remove")]
    [Arguments("UserMutation", "tryadd")]
    [Arguments("UserMutation", "update")]
    [Arguments("UserMutation", "touch")]
    [Arguments("UserMutation", "removeexpiration")]
    [Arguments("Expiration", "expire")]
    [Arguments("LeaderTerm", "leader-noop")]
    public async Task KnownKindsReuseStrings(string recordKind, string mutationKind)
    {
        var record = Record() with { RecordKind = recordKind, MutationKind = mutationKind };
        var bytes = ReplicaLogCodec.Encode(in record);
        var owned = ReplicaLogCodec.Decode(bytes).GetValueOrDefault();
        var borrowed = ReplicaLogCodec.DecodeBorrowed(bytes).GetValueOrDefault();

        _ = await Assert.That(owned.RecordKind).IsEqualTo(recordKind);
        _ = await Assert.That(owned.MutationKind).IsEqualTo(mutationKind);
        _ = await Assert.That(borrowed.RecordKind).IsSameReferenceAs(owned.RecordKind);
        _ = await Assert.That(borrowed.MutationKind).IsSameReferenceAs(owned.MutationKind);
    }

    /// <summary>Unknown values remain ordinary strings and are not globally interned.</summary>
    [Test]
    public async Task UnknownKindsRemainUnchanged()
    {
        var record = Record() with { RecordKind = "custom-record", MutationKind = "custom-mutation" };
        var bytes = ReplicaLogCodec.Encode(in record);
        var first = ReplicaLogCodec.Decode(bytes).GetValueOrDefault();
        var second = ReplicaLogCodec.Decode(bytes).GetValueOrDefault();

        _ = await Assert.That(first.RecordKind).IsEqualTo(record.RecordKind);
        _ = await Assert.That(first.MutationKind).IsEqualTo(record.MutationKind);
        _ = await Assert.That(ReferenceEquals(first.RecordKind, second.RecordKind)).IsFalse();
        _ = await Assert.That(ReferenceEquals(first.MutationKind, second.MutationKind)).IsFalse();
    }

    /// <summary>The textual key path has exactly the existing binary key encoding.</summary>
    /// <param name="key">The textual key.</param>
    [Test]
    [Arguments("")]
    [Arguments("key")]
    [Arguments("\u00E9\u4E2D\U0001F600")]
    [Arguments("a\0b")]
    public async Task TextKeyPreservesCanonicalBytes(string key)
    {
        var binary = Record() with { KeyPayload = Encoding.UTF8.GetBytes(key) };
        var expected = ReplicaLogCodec.Encode(in binary);
        var textual = Record() with { KeyPayload = ReadOnlyMemory<byte>.Empty };

        var actual = ReplicaLogCodec.Encode(in textual, key);

        await SequenceAssert.EqualAsync(expected, actual);
        _ = await Assert.That(ReplicaLogCodec.IsWellFormed(actual, 1, 2)).IsTrue();
    }

    private static ReplicaLogRecord Record() => new(1, 2, "op", "cache", new byte[] { 1 }, "UserMutation", "cache", new byte[] { 2 }, "set", new byte[] { 3 }, new byte[] { 4 }, 0, 0, 0);
}
