using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Round-trip coverage for the closed replication envelope codec.</summary>
public sealed class ReplicationEnvelopeCodecTests : ServerUnitTestBase
{
    /// <summary>Encode/decode preserves the mandatory envelope fields.</summary>
    [Fact]
    public void RoundTripPreservesRequiredFields()
    {
        // ZA0302: tiny fixture payload owned by the assertion below.
#pragma warning disable ZA0302
        var fingerprint = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
#pragma warning restore ZA0302
        var envelope = new Envelope(
            EnvelopeCodec.SchemaVersion,
            "group-a",
            fingerprint,
            9,
            11,
            "leader-1",
            "sender-2",
            13,
            17,
            0xA5A5_A5A5);

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
}
