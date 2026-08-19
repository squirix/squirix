using System;
using System.Buffers.Binary;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Round-trip coverage for the closed replication envelope codec.</summary>
[Immutable]
public sealed class ReplicationEnvelopeCodecTests : ServerUnitTestBase
{
    /// <summary>Encode/decode preserves the mandatory envelope fields.</summary>
    [Fact]
    public void RoundTripPreservesRequiredFields()
    {
        var fingerprint = BufferKit.CopyToOwned([1, 2, 3, 4, 5, 6, 7, 8]);
        var envelope = new Envelope(EnvelopeCodec.SchemaVersion, "group-a", fingerprint, 9, 11, "leader-1", "sender-2", 13, 17, 0xA5A5_A5A5);

        var decoded = EnvelopeCodec.Decode(EnvelopeCodec.Encode(envelope));

        Assert.Equal(envelope.SchemaVersion, decoded.SchemaVersion);
        Assert.Equal(envelope.GroupId, decoded.GroupId);
        Assert.Equal(envelope.TopologyFingerprint, decoded.TopologyFingerprint);
        Assert.Equal(envelope.ConfigurationGeneration, decoded.ConfigurationGeneration);
        Assert.Equal(envelope.Term, decoded.Term);
        Assert.Equal(envelope.LeaderNodeId, decoded.LeaderNodeId);
        Assert.Equal(envelope.SenderNodeId, decoded.SenderNodeId);
        Assert.Equal(envelope.LogIndex, decoded.LogIndex);
        Assert.Equal(envelope.CommitIndex, decoded.CommitIndex);
        Assert.Equal(envelope.PayloadChecksum, decoded.PayloadChecksum);
    }

    /// <summary>Verifies that a payload shorter than the fixed header is rejected as truncated.</summary>
    [Fact]
    public void PayloadShorterThanFixedHeaderIsRejected()
    {
        var payload = BufferKit.ToOwnedBytes(47, 0, static (_, _) => { });

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value));
    }

    /// <summary>Verifies that an unsupported envelope schema version is rejected.</summary>
    [Fact]
    public void UnsupportedSchemaVersionIsRejected()
    {
        var payload = EnvelopeCodec.Encode(CreateValidEnvelope());
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), 99);

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value));
    }

    /// <summary>Verifies that an envelope missing its commit index is rejected as truncated.</summary>
    [Fact]
    public void MissingCommitIndexIsRejected()
    {
        var payload = EnvelopeCodec.Encode(CreateValidEnvelope());

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value.AsSpan(0, value.Length - 8)));
    }

    /// <summary>Verifies that a fingerprint length prefix missing from the buffer is rejected.</summary>
    [Fact]
    public void MissingFingerprintLengthIsRejected()
    {
        var payload = CreateFixedLengthPayload(9);

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value));
    }

    /// <summary>Verifies that a negative fingerprint length prefix is rejected.</summary>
    [Fact]
    public void NegativeFingerprintLengthIsRejected()
    {
        var payload = CreateFixedLengthPayload(0, -1);

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value));
    }

    /// <summary>Verifies that an oversized fingerprint length prefix is rejected.</summary>
    [Fact]
    public void OversizedFingerprintLengthIsRejected()
    {
        var payload = CreateFixedLengthPayload(0, 100);

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value));
    }

    /// <summary>Verifies that a negative group id length prefix is rejected.</summary>
    [Fact]
    public void NegativeGroupIdLengthIsRejected()
    {
        var payload = CreateFixedLengthPayload(-1);

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value));
    }

    /// <summary>Verifies that an oversized group id length prefix is rejected.</summary>
    [Fact]
    public void OversizedGroupIdLengthIsRejected()
    {
        var payload = CreateFixedLengthPayload(1000);

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value));
    }

    /// <summary>Verifies that a leader node id length prefix missing from the buffer is rejected.</summary>
    [Fact]
    public void TruncatedLeaderNodeIdLengthIsRejected()
    {
        var payload = CreateFixedLengthPayload(6, 1);

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(payload, static value => _ = EnvelopeCodec.Decode(value));
    }

    private static Envelope CreateValidEnvelope() => new(EnvelopeCodec.SchemaVersion, "group-a", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }, 9, 11, "leader-1", "sender-2", 13, 17, 0xA5A5_A5A5);

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
}
