using System;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Checks the wire layout of fixed-width frame headers.</summary>
public sealed class BinaryFrameHeaderTests
{
    /// <summary>Request flags, maximum body size, timeout and reply status occupy the defined packed bytes.</summary>
    [Test]
    public async Task RequestAndReplyHeadersUsePackedWords()
    {
        const int id = 0x10203040;
        const int control = Frames.TracedRequest | (127 << 16) | ushort.MaxValue;
        var request = GC.AllocateUninitializedArray<byte>(Frames.RequestBytes);
        var reply = GC.AllocateUninitializedArray<byte>(Frames.ReplyBytes);
        byte[] expectedRequest = [0x10, 0x20, 0x30, 0x40, 0xff, 0x80, 0, 0, 0xff, 0xff];
        byte[] expectedReply = [0x10, 0x20, 0x30, 0x40, 16, 0x80, 0, 0];
        Frames.WriteRequest(request, id, Frames.MaxRequestBytes, control);
        Frames.WriteReply(reply, id, Frames.MaxResponseBytes, 16);

        _ = await Assert.That(request.AsSpan().SequenceEqual(expectedRequest)).IsTrue();
        _ = await Assert.That(reply.AsSpan().SequenceEqual(expectedReply)).IsTrue();
        _ = await Assert.That(Frames.ReadRequest(expectedRequest)).IsEqualTo((id, Frames.MaxRequestBytes, control));
        _ = await Assert.That(Frames.ReadReply(expectedReply)).IsEqualTo((id, Frames.MaxResponseBytes, 16));

        Frames.WriteRequestTimeout(request, 256);
        _ = await Assert.That(Frames.ReadRequest(request)).IsEqualTo((id, Frames.MaxRequestBytes, (control & ~ushort.MaxValue) | 256));
    }

    /// <summary>The header encodes the operation in one byte immediately before the metadata lengths.</summary>
    [Test]
    public async Task RequestHeaderEncodesNumericOperation()
    {
        var request = new PeerFrames.Request(5, 250, PeerOperation.AppendReplicaEntries, 3, 4, 5);
        var header = GC.AllocateUninitializedArray<byte>(PeerFrames.RequestBytes);
        byte[] expected = [.. "SQP2"u8, 16, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 250, 0, 24, 16, 5];
        PeerFrames.WriteRequest(header, in request);

        _ = await Assert.That(header.AsSpan().SequenceEqual(expected)).IsTrue();
        var decoded = PeerFrames.ReadRequest(expected);
        _ = await Assert.That(decoded).IsEqualTo(request);
        _ = await Assert.That(decoded.Metadata).IsEqualTo(12);
    }

    /// <summary>Peer replies fit one word without truncating the largest supported body.</summary>
    [Test]
    public async Task ReplyHeaderPreservesMaximumBodyAndStatus()
    {
        const int size = 8 * 1024 * 1024;
        var header = GC.AllocateUninitializedArray<byte>(PeerFrames.ReplyBytes);
        byte[] expected = [.. "SQP2"u8, 16, 0x80, 0, 0];
        PeerFrames.WriteReply(header, 16, size);

        _ = await Assert.That(header.AsSpan().SequenceEqual(expected)).IsTrue();
        _ = await Assert.That(PeerFrames.ReadReply(expected)).IsEqualTo((16, size));
    }

    /// <summary>Canonical ring fingerprints use the digest bytes and a header flag.</summary>
    [Test]
    public async Task RingDigestFitsInThirtyTwoBytes()
    {
        const string canonical = "00112233445566778899AABBCCDDEEFF00112233445566778899AABBCCDDEEFF";
        const string lowercase = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        var (digest, isDigest) = PeerFrames.EncodeFingerprint(canonical);
        var (literal, literalIsDigest) = PeerFrames.EncodeFingerprint(lowercase);
        var header = GC.AllocateUninitializedArray<byte>(PeerFrames.RequestBytes);
        var request = new PeerFrames.Request(0, -1, PeerOperation.GetValue, digest.Length, WireTraceParent.ByteCount, 0, isDigest, true);
        PeerFrames.WriteRequest(header, in request);

        _ = await Assert.That(isDigest).IsTrue();
        _ = await Assert.That(digest.Length).IsEqualTo(32);
        _ = await Assert.That(Convert.ToHexString(digest)).IsEqualTo(canonical);
        _ = await Assert.That(literalIsDigest).IsFalse();
        _ = await Assert.That(PeerFrames.Text.GetString(literal)).IsEqualTo(lowercase);
        _ = await Assert.That(PeerFrames.ReadRequest(header)).IsEqualTo(request);
        _ = await Assert.That(PeerFrames.ReadRequest(header).Metadata).IsEqualTo(32 + WireTraceParent.ByteCount);
    }
}
