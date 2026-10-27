using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Rejection tests for <see cref="ReplicaLogCodec" /> decoding.</summary>
[Immutable]
public sealed class ReplicaLogCodecTests : ServerUnitTestBase
{
    /// <summary>SHA-256 of the canonical encoding of each golden record, in order.</summary>
    private static readonly string[] GoldenHashes = [
        "4A5C2AC7B8884630CA28A940B5BD407E81EAA83E4FD114246DE5AC41901FC3F1",
        "94035FC42AA4B3EDE8628B67504A12D584EFFDC8F92A1FFC4EDD60BD299B6AE1",
        "06A65961E260A75C11D9723C233DE928A1AC972683038A526A4926674B5052EF",
        "069976C331225FC306C924C19CB54652852C6A14761ADF01525826ECDBAF65C6",
    ];

    /// <summary>Verifies that an unknown version is rejected.</summary>
    [Test]
    public async Task DecodeRejectsBadVersion() => _ = await Assert.That(ReplicaLogCodec.Decode(ReadOnlyMemory<byte>.Of(0, 0))).IsNull();

    /// <summary>Verifies that a version 1 record, whose Touch carried a relative duration, is refused instead of being read as a deadline.</summary>
    [Test]
    public async Task DecodeRefusesVersionOne()
    {
        var encoded = ReplicaLogCodec.Encode(CreateRecord());
        _ = await Assert.That(ReplicaLogCodec.Decode(encoded)).IsNotNull();

        BinaryPrimitives.WriteUInt16LittleEndian(encoded, 1);

        _ = await Assert.That(ReplicaLogCodec.Decode(encoded)).IsNull();
    }

    /// <summary>Verifies that a version 3 record, which cannot carry an expiration tombstone, is refused instead of being read.</summary>
    [Test]
    public async Task DecodeRefusesVersionThree()
    {
        var encoded = ReplicaLogCodec.Encode(CreateRecord());

        BinaryPrimitives.WriteUInt16LittleEndian(encoded, 3);

        _ = await Assert.That(ReplicaLogCodec.Decode(encoded)).IsNull();
    }

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

    /// <summary>Verifies that the canonical bytes of varied records stay byte-identical and decode back to the same record.</summary>
    [Test]
    public async Task EncodePinsCanonicalBytes()
    {
        var records = CreateGoldenRecords();
        _ = await Assert.That(records.Length).IsEqualTo(GoldenHashes.Length);

        for (var i = 0; i < records.Length; i++)
        {
            var encoded = ReplicaLogCodec.Encode(in records[i]);

            _ = await Assert.That(Convert.ToHexString(SHA256.HashData(encoded))).IsEqualTo(GoldenHashes[i]);

            var decoded = ReplicaLogCodec.Decode(encoded);
            _ = await Assert.That(decoded).IsNotNull();
            _ = await Assert.That(ReplicaLogCodec.Encode(decoded!.Value).AsSpan().SequenceEqual(encoded)).IsTrue();
            _ = await Assert.That(decoded.Value.OperationId).IsEqualTo(records[i].OperationId);
            _ = await Assert.That(decoded.Value.CacheName).IsEqualTo(records[i].CacheName);
            _ = await Assert.That(decoded.Value.MutationPayload.Span.SequenceEqual(records[i].MutationPayload.Span)).IsTrue();
            _ = await Assert.That(decoded.Value.PayloadChecksum).IsEqualTo(records[i].PayloadChecksum);
        }
    }

    /// <summary>Creates the records whose encodings are pinned.</summary>
    /// <returns>The golden records.</returns>
    private static ReplicaLogRecord[] CreateGoldenRecords()
    {
        const int largePayloadSize = 70 * 1024;
        var large = new byte[largePayloadSize];
        for (var i = 0; i < large.Length; i++)
            large[i] = byte.CreateTruncating((i * 31) + 7);

        return
        [
            new ReplicaLogRecord(0, 0, string.Empty, string.Empty, ReadOnlyMemory<byte>.Empty, string.Empty, string.Empty, ReadOnlyMemory<byte>.Empty, string.Empty, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, 0, 0, 0, 0),
            new ReplicaLogRecord(
                ulong.MaxValue,
                ulong.MaxValue,
                "op-1",
                "user",
                new byte[16] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 },
                "mutation",
                "orders",
                new byte[] { 1, 2, 3 },
                "Set",
                new byte[] { 4, 5 },
                new byte[] { 6 },
                long.MaxValue,
                long.MaxValue,
                long.MaxValue,
                uint.MaxValue),
            new ReplicaLogRecord(
                1,
                2,
                "op-é",
                "中文范围",
                new byte[] { 0xFF },
                "k😀ind",
                "caché-中",
                new byte[] { 0, 0, 0 },
                "😀",
                new byte[] { 0xFE, 0x00 },
                new byte[] { 0x80 },
                long.MinValue,
                long.MinValue,
                long.MinValue,
                0),
            new ReplicaLogRecord(
                123456789,
                987654321,
                "operation",
                "scope",
                new byte[16],
                "kind",
                "cache",
                large,
                "mutation",
                large,
                large,
                -1,
                1,
                638000000000000000,
                0xDEADBEEF),
        ];
    }

    /// <summary>Creates a valid record for codec tests.</summary>
    /// <returns>The record to encode.</returns>
    private static ReplicaLogRecord CreateRecord() => new(
        1,
        2,
        "op",
        "scope",
        ReadOnlyMemory<byte>.Of(1),
        "kind",
        "cache",
        ReadOnlyMemory<byte>.Of(2),
        "mutation",
        ReadOnlyMemory<byte>.Of(3),
        ReadOnlyMemory<byte>.Of(4),
        5,
        6,
        7,
        8);
}
