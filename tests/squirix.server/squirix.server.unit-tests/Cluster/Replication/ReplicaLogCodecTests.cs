using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Rejection tests for <see cref="ReplicaLogCodec" /> decoding.</summary>
[Immutable]
public sealed class ReplicaLogCodecTests : ServerUnitTestBase
{
    /// <summary>Verifies that an unknown version is rejected.</summary>
    [Test]
    public async Task DecodeRejectsBadVersion() => _ = await Assert.That(ReplicaLogCodec.Decode(new ReadOnlyMemory<byte>([0, 0]))).IsNull();

    /// <summary>Verifies that an empty payload is rejected.</summary>
    [Test]
    public async Task DecodeRejectsEmpty() => _ = await Assert.That(ReplicaLogCodec.Decode(ReadOnlyMemory<byte>.Empty)).IsNull();

    /// <summary>Verifies that cutting the payload at any position is rejected.</summary>
    /// <remarks>
    /// Every length-prefixed section reader fails closed, so each truncation length
    /// exercises a distinct rejection branch of the decoder.
    /// </remarks>
    [Test]
    public async Task DecodeRejectsEveryTruncation()
    {
        var encoded = ReplicaLogCodec.Encode(CreateRecord());

        for (var length = 0; length < encoded.Length; length++)
            _ = await Assert.That(ReplicaLogCodec.Decode(encoded.AsMemory(0, length))).IsNull();
    }

    /// <summary>Verifies that malformed UTF-8 inside a string field is rejected.</summary>
    [Test]
    public async Task DecodeRejectsMalformedUtf8()
    {
        var encoded = ReplicaLogCodec.Encode(CreateRecord());

        // Layout: version u16 | logIndex u64 | term u64 | operation id length-prefixed string.
        const int versionSize = sizeof(ushort);
        const int logIndexSize = sizeof(ulong);
        const int termSize = sizeof(ulong);
        const int lengthPrefixSize = sizeof(int);
        encoded[versionSize + logIndexSize + termSize + lengthPrefixSize] = 0xFF;

        _ = await Assert.That(ReplicaLogCodec.Decode(encoded)).IsNull();
    }

    /// <summary>Verifies that trailing bytes are rejected.</summary>
    [Test]
    public async Task DecodeRejectsTrailingBytes()
    {
        var encoded = ReplicaLogCodec.Encode(CreateRecord());

        _ = await Assert.That(ReplicaLogCodec.Decode(new ReadOnlyMemory<byte>([.. encoded, 0]))).IsNull();
    }

    /// <summary>Verifies that a truncated payload is rejected.</summary>
    [Test]
    public async Task DecodeRejectsTruncated()
    {
        var encoded = ReplicaLogCodec.Encode(CreateRecord());

        _ = await Assert.That(ReplicaLogCodec.Decode(encoded.AsMemory(0, encoded.Length - 1))).IsNull();
    }

    /// <summary>Creates a valid record for codec tests.</summary>
    /// <returns>The record to encode.</returns>
    private static ReplicaLogRecord CreateRecord() => new(
        1,
        2,
        "op",
        "scope",
        new ReadOnlyMemory<byte>([1]),
        "kind",
        "cache",
        new ReadOnlyMemory<byte>([2]),
        "mutation",
        new ReadOnlyMemory<byte>([3]),
        new ReadOnlyMemory<byte>([4]),
        5,
        6,
        7,
        8);
}
