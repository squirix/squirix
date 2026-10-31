using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Decoded fields own their storage, and decoding observes the exact input memory bounds.</summary>
[Immutable]
public sealed class ReplicaLogOwnershipTests : ServerUnitTestBase
{
    /// <summary>Changing the source after decoding cannot alter any byte field or string in the decoded record.</summary>
    [Test]
    public async Task DecodeOwnsFieldsAfterInputChanges()
    {
        var encoded = ReplicaLogCodec.Encode(CreateRecord());
        byte[] original = [.. encoded];
        var decoded = ReplicaLogCodec.Decode(encoded);
        _ = await Assert.That(decoded).IsNotNull();

        encoded.AsSpan().Fill(0xCC);

        _ = await Assert.That(ReplicaLogCodec.Encode(decoded!.Value).AsSpan().SequenceEqual(original)).IsTrue();
    }

    /// <summary>Bytes outside the supplied slice are ignored, and a truncated slice cannot borrow the remaining backing bytes.</summary>
    [Test]
    public async Task DecodeHonorsSliceBounds()
    {
        var encoded = ReplicaLogCodec.Encode(CreateRecord());
        byte[] backing = [0xFF, .. encoded, 0xFF];
        var slice = backing.AsMemory(1, encoded.Length);

        var decoded = ReplicaLogCodec.Decode(slice);

        _ = await Assert.That(decoded).IsNotNull();
        _ = await Assert.That(ReplicaLogCodec.Encode(decoded!.Value).AsSpan().SequenceEqual(encoded)).IsTrue();
        _ = await Assert.That(ReplicaLogCodec.Decode(backing.AsMemory(1, encoded.Length - 1))).IsNull();
    }

    private static ReplicaLogRecord CreateRecord() => new(
        42,
        3,
        "operation-é",
        "scope-中",
        ReadOnlyMemory<byte>.Of(1, 2),
        "mutation",
        "cache-😀",
        ReadOnlyMemory<byte>.Of(3, 4),
        "Set",
        ReadOnlyMemory<byte>.Of(5, 6),
        ReadOnlyMemory<byte>.Of(7, 8),
        100,
        200,
        300);
}
