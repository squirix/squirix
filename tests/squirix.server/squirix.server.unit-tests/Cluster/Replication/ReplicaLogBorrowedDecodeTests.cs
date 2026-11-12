using System;
using System.Buffers.Binary;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Decoding borrows binary fields from the supplied canonical payload.</summary>
public sealed class ReplicaLogBorrowedDecodeTests : ServerUnitTestBase
{
    /// <summary>Every binary field is an exact slice of a nonzero-offset canonical input.</summary>
    [Test]
    public async Task BinaryFieldsBorrowSourceSlices()
    {
        var encoded = ReplicaLogCodec.Encode(Record());
        byte[] padded = [0xFF, .. encoded, 0xFF];
        var source = padded.AsMemory(1, encoded.Length);
        var result = ReplicaLogCodec.DecodeBorrowed(source);
        _ = await Assert.That(result).IsNotNull();
        var decoded = result.GetValueOrDefault();
        var offset = 2 + (2 * sizeof(ulong));
        ReadOnlyMemory<byte>[] binary = [decoded.OperationFingerprint, decoded.KeyPayload, decoded.MutationPayload, decoded.OutcomePayload];
        var next = 0;
        for (var field = 0; field < 9; field++)
        {
            var length = BinaryPrimitives.ReadInt32LittleEndian(source.Span[offset..]);
            offset += sizeof(int);
            if (field is 2 or 5 or 7 or 8)
                _ = await Assert.That(binary[next++].Equals(source.Slice(offset, length))).IsTrue();
            offset += length;
        }

        await SequenceAssert.EqualAsync(encoded, ReplicaLogCodec.Encode(in decoded));
        _ = await Assert.That(ReplicaLogCodec.DecodeBorrowed(source[..^1])).IsNull();
    }

    /// <summary>Borrowed decoding preserves empty fields and all scalar and string values.</summary>
    [Test]
    public async Task EmptyFieldsRoundTrip()
    {
        var record = Record() with { OperationFingerprint = default, KeyPayload = default, MutationPayload = default, OutcomePayload = default };
        var encoded = ReplicaLogCodec.Encode(in record);
        var result = ReplicaLogCodec.DecodeBorrowed(encoded);
        _ = await Assert.That(result).IsNotNull();
        var decoded = result.GetValueOrDefault();

        await SequenceAssert.EqualAsync(encoded, ReplicaLogCodec.Encode(in decoded));
    }

    /// <summary>Every truncation, invalid version, invalid length, malformed UTF-8 and trailing byte is refused.</summary>
    [Test]
    public async Task BorrowedDecodeRejectsMalformedData()
    {
        var encoded = ReplicaLogCodec.Encode(Record());
        for (var size = 0; size < encoded.Length; size++)
            _ = await Assert.That(ReplicaLogCodec.DecodeBorrowed(encoded.AsMemory(0, size))).IsNull();

        _ = await Assert.That(ReplicaLogCodec.DecodeBorrowed(ReadOnlyMemory<byte>.Of(0, 0))).IsNull();
        _ = await Assert.That(ReplicaLogCodec.DecodeBorrowed(new ReadOnlyMemory<byte>([.. encoded, 0]))).IsNull();
        const int firstField = 2 + (2 * sizeof(ulong));
        encoded[firstField + sizeof(int)] = 0xFF;
        _ = await Assert.That(ReplicaLogCodec.DecodeBorrowed(encoded)).IsNull();
        encoded = ReplicaLogCodec.Encode(Record());
        BinaryPrimitives.WriteInt32LittleEndian(encoded.AsSpan(firstField), -1);
        _ = await Assert.That(ReplicaLogCodec.DecodeBorrowed(encoded)).IsNull();
    }

    private static ReplicaLogRecord Record() => new(
        1,
        2,
        "operation-é",
        "scope",
        new byte[] { 1, 2 },
        "UserMutation",
        "cache",
        new byte[] { 3, 4 },
        "Set",
        new byte[] { 5, 6, 7 },
        new byte[] { 8, 9 },
        10,
        11,
        12);
}
