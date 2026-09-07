using System;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Rejection tests for <see cref="ReplicaLogCodec" /> decoding.</summary>
[Immutable]
public sealed class ReplicaLogCodecTests : ServerUnitTestBase
{
    /// <summary>Verifies that an empty payload is rejected.</summary>
    [Fact]
    public void DecodeRejectsEmpty() =>
        Assert.Null(ReplicaLogCodec.Decode(ReadOnlyMemory<byte>.Empty));

    /// <summary>Verifies that an unknown version is rejected.</summary>
    [Fact]
    public void DecodeRejectsBadVersion() =>
        Assert.Null(ReplicaLogCodec.Decode(new ReadOnlyMemory<byte>([0, 0])));

    /// <summary>Verifies that a truncated payload is rejected.</summary>
    [Fact]
    public void DecodeRejectsTruncated()
    {
        var encoded = ReplicaLogCodec.Encode(CreateRecord());

        Assert.Null(ReplicaLogCodec.Decode(encoded.AsMemory(0, encoded.Length - 1)));
    }

    /// <summary>Verifies that trailing bytes are rejected.</summary>
    [Fact]
    public void DecodeRejectsTrailingBytes()
    {
        var encoded = ReplicaLogCodec.Encode(CreateRecord());

        Assert.Null(ReplicaLogCodec.Decode(new ReadOnlyMemory<byte>([.. encoded, 0])));
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
