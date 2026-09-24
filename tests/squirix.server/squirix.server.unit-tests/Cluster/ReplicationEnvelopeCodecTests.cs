using System;
using System.Buffers.Binary;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Round-trip coverage for the closed replication envelope codec.</summary>
[Immutable]
public sealed class ReplicationEnvelopeCodecTests : ServerUnitTestBase
{
    /// <summary>Decode reads the golden wire bytes back into the fixed envelope fields.</summary>
    [Test]
    public async Task DecodeReadsGoldenWireBytes()
    {
        var decoded = EnvelopeCodec.Decode(GoldenWireBytes());

        _ = await Assert.That(decoded.SchemaVersion).IsEqualTo(EnvelopeCodec.SchemaVersion);
        _ = await Assert.That(decoded.GroupId).IsEqualTo("group-a");
        await SequenceAssert.EqualMemoryAsync(ReadOnlyMemory<byte>.Of(1, 2, 3, 4, 5, 6, 7, 8), decoded.TopologyFingerprint);
        _ = await Assert.That(decoded.ConfigurationGeneration).IsEqualTo(9UL);
        _ = await Assert.That(decoded.Term).IsEqualTo(11UL);
        _ = await Assert.That(decoded.LeaderNodeId).IsEqualTo("leader-1");
        _ = await Assert.That(decoded.SenderNodeId).IsEqualTo("sender-2");
        _ = await Assert.That(decoded.LogIndex).IsEqualTo(13UL);
        _ = await Assert.That(decoded.CommitIndex).IsEqualTo(17UL);
        _ = await Assert.That(decoded.PayloadChecksum).IsEqualTo(0xA5A5_A5A5u);
    }

    /// <summary>Encode emits the documented golden wire bytes for a fixed envelope.</summary>
    [Test]
    public Task EncodeMatchesGoldenWireBytes()
    {
        var envelope = new Envelope(EnvelopeCodec.SchemaVersion, "group-a", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }, 9, 11, "leader-1", "sender-2", 13, 17, 0xA5A5_A5A5);

        return SequenceAssert.EqualAsync(GoldenWireBytes(), EnvelopeCodec.Encode(envelope));
    }

    /// <summary>Verifies that an envelope missing its commit index is rejected as truncated.</summary>
    [Test]
    public void MissingCommitIndexIsRejected()
    {
        var payload = EnvelopeCodec.Encode(CreateValidEnvelope());

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value.AsSpan(0, value.Length - 8)));
    }

    /// <summary>Verifies that a fingerprint length prefix missing from the buffer is rejected.</summary>
    [Test]
    public void MissingFingerprintLengthIsRejected()
    {
        var payload = CreateFixedLengthPayload(9);

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value));
    }

    /// <summary>Verifies that a negative fingerprint length prefix is rejected.</summary>
    [Test]
    public void NegativeFingerprintLengthIsRejected()
    {
        var payload = CreateFixedLengthPayload(0, -1);

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value));
    }

    /// <summary>Verifies that a negative group id length prefix is rejected.</summary>
    [Test]
    public void NegativeGroupIdLengthIsRejected()
    {
        var payload = CreateFixedLengthPayload(-1);

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value));
    }

    /// <summary>Verifies that an oversized fingerprint length prefix is rejected.</summary>
    [Test]
    public void OversizedFingerprintLengthIsRejected()
    {
        var payload = CreateFixedLengthPayload(0, 100);

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value));
    }

    /// <summary>Verifies that an oversized group id length prefix is rejected.</summary>
    [Test]
    public void OversizedGroupIdLengthIsRejected()
    {
        var payload = CreateFixedLengthPayload(1000);

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value));
    }

    /// <summary>Verifies that a payload shorter than the fixed header is rejected as truncated.</summary>
    [Test]
    public void PayloadShorterThanFixedHeaderIsRejected()
    {
        var payload = BufferKit.ToOwnedBytes(47, 0, static (_, _) => { });

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value));
    }

    /// <summary>Encode/decode preserves the mandatory envelope fields.</summary>
    [Test]
    public async Task RoundTripPreservesRequiredFields()
    {
        var fingerprint = BufferKit.CopyToOwned([1, 2, 3, 4, 5, 6, 7, 8]);
        var envelope = new Envelope(EnvelopeCodec.SchemaVersion, "group-a", fingerprint, 9, 11, "leader-1", "sender-2", 13, 17, 0xA5A5_A5A5);

        var decoded = EnvelopeCodec.Decode(EnvelopeCodec.Encode(envelope));

        _ = await Assert.That(decoded.SchemaVersion).IsEqualTo(envelope.SchemaVersion);
        _ = await Assert.That(decoded.GroupId).IsEqualTo(envelope.GroupId);
        await SequenceAssert.EqualMemoryAsync(envelope.TopologyFingerprint, decoded.TopologyFingerprint);
        _ = await Assert.That(decoded.ConfigurationGeneration).IsEqualTo(envelope.ConfigurationGeneration);
        _ = await Assert.That(decoded.Term).IsEqualTo(envelope.Term);
        _ = await Assert.That(decoded.LeaderNodeId).IsEqualTo(envelope.LeaderNodeId);
        _ = await Assert.That(decoded.SenderNodeId).IsEqualTo(envelope.SenderNodeId);
        _ = await Assert.That(decoded.LogIndex).IsEqualTo(envelope.LogIndex);
        _ = await Assert.That(decoded.CommitIndex).IsEqualTo(envelope.CommitIndex);
        _ = await Assert.That(decoded.PayloadChecksum).IsEqualTo(envelope.PayloadChecksum);
    }

    /// <summary>Verifies that a leader node id length prefix missing from the buffer is rejected.</summary>
    [Test]
    public void TruncatedLeaderNodeIdLengthIsRejected()
    {
        var payload = CreateFixedLengthPayload(6, 1);

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value));
    }

    /// <summary>Verifies that an unsupported envelope schema version is rejected.</summary>
    [Test]
    public void UnsupportedSchemaVersionIsRejected()
    {
        var payload = EnvelopeCodec.Encode(CreateValidEnvelope());
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), 99);

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value));
    }

    private static byte[] CreateFixedLengthPayload(int groupIdLength, int? fingerprintLength = null) => BufferKit.ToOwnedBytes(
        48,
        (groupIdLength, fingerprintLength),
        static (state, buffer) =>
        {
            BinaryPrimitives.WriteUInt32LittleEndian(buffer, EnvelopeCodec.SchemaVersion);
            BinaryPrimitives.WriteInt32LittleEndian(buffer[32..], state.groupIdLength);
            if (state.fingerprintLength is { } fpLength)
                BinaryPrimitives.WriteInt32LittleEndian(buffer[(36 + state.groupIdLength)..], fpLength);
        });

    private static Envelope CreateValidEnvelope() => new(
        EnvelopeCodec.SchemaVersion,
        "group-a",
        new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 },
        9,
        11,
        "leader-1",
        "sender-2",
        13,
        17,
        0xA5A5_A5A5);

    /// <summary>
    /// Hand-built golden encoding of the fixed envelope above (schema 1, group-a,
    /// fingerprint 01..08, generation 9, term 11, leader-1, sender-2, log 13,
    /// commit 17, checksum A5A5A5A5): u32 schema, u32 checksum, u64 generation,
    /// u64 term, u64 log index, then i32-prefixed group, fingerprint, leader,
    /// sender blobs, then u64 commit index. A symmetric encode/decode bug that a
    /// pure round-trip cannot see fails against these bytes.
    /// </summary>
    private static byte[] GoldenWireBytes() =>
    [
        0x01, 0x00, 0x00, 0x00, 0xA5, 0xA5, 0xA5, 0xA5,
        0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x0B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x0D, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x07, 0x00, 0x00, 0x00, 0x67, 0x72, 0x6F, 0x75, 0x70, 0x2D, 0x61,
        0x08, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x08, 0x00, 0x00, 0x00, 0x6C, 0x65, 0x61, 0x64, 0x65, 0x72, 0x2D, 0x31,
        0x08, 0x00, 0x00, 0x00, 0x73, 0x65, 0x6E, 0x64, 0x65, 0x72, 0x2D, 0x32,
        0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
}
