using System;
using System.Buffers.Binary;
using System.IO;
using System.IO.Pipelines;
using System.Net.Security;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Checks peer I/O independently of application dispatch.</summary>
public sealed class PeerSessionDeadlineTests
{
    /// <summary>Short and extended peer frames retain the version-two protocol identifiers.</summary>
    /// <param name="length">The short-form length or the marker for an extended length.</param>
    [Test]
    [Arguments(0)]
    [Arguments(Frames.ExtendedSize)]
    public async Task PeerFramesKeepVersionTwo(int length)
    {
        var requestHeader = GC.AllocateUninitializedArray<byte>(PeerFrames.RequestBytes);
        var replyHeader = GC.AllocateUninitializedArray<byte>(PeerFrames.ReplyBytes);
        var request = new PeerFrames.Request(length, -1, PeerOperation.Ping, 0, 0, 0);
        PeerFrames.WriteRequest(requestHeader, in request);
        PeerFrames.WriteReply(replyHeader, 0, length);

        const uint versionTwoMagic = 0x53515032;
        _ = await Assert.That(PeerFrames.Protocol).IsEqualTo(new SslApplicationProtocol("squirix-peer/2"));
        _ = await Assert.That(BinaryPrimitives.ReadUInt32BigEndian(requestHeader)).IsEqualTo(versionTwoMagic);
        _ = await Assert.That(BinaryPrimitives.ReadUInt32BigEndian(replyHeader)).IsEqualTo(versionTwoMagic);
        _ = await Assert.That(PeerFrames.ReadRequest(requestHeader).Body).IsEqualTo(length);
        _ = await Assert.That(PeerFrames.ReadReply(replyHeader).Size).IsEqualTo(length);
    }

    /// <summary>The sender omits optional tracing fields outside the encoded byte budget.</summary>
    [Test]
    public async Task TraceMetadataHonorsEncodedByteLimit()
    {
        var trace = new string('é', Frames.MaxTraceStateLength / 2);
        _ = await Assert.That(NativePeerClient.TraceText(trace, Frames.MaxTraceStateLength, out var count)).IsEqualTo(trace);
        _ = await Assert.That(count).IsEqualTo(Frames.MaxTraceStateLength);
        _ = await Assert.That(NativePeerClient.TraceText(trace + "x", Frames.MaxTraceStateLength, out var omitted)).IsEmpty();
        _ = await Assert.That(omitted).IsEqualTo(0);
    }

    /// <summary>A reply with an incompatible header reports data loss through the peer-call contract.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InvalidReplyHeaderReportsDataLoss(CancellationToken cancellationToken)
    {
        await using var input = new MemoryStream(new byte[PeerFrames.ReplyBytes]);
        var error = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(NativePeerClient.ReadReplyAsync(input, SetAsyncResponse.Parser, new byte[PeerFrames.ReplyBytes], cancellationToken), TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        _ = await Assert.That(error.StatusCode).IsEqualTo(FrameStatus.DataLoss);
    }

    /// <summary>Large forwarded bytes borrow a private frame and text replies retain their consumed prefix.</summary>
    /// <param name="binary">Whether the response contains binary data.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task LargeForwardedReplyPreservesFrame(bool binary, CancellationToken cancellationToken)
    {
        const int payloadLength = 65536;
        const int bodyLength = payloadLength + 1;
        var frame = new byte[PeerFrames.ReplyBytes + bodyLength + 1];
        PeerFrames.WriteReply(frame, 0, bodyLength);
        frame[PeerFrames.ReplyBytes] = binary ? byte.CreateChecked(31) : byte.CreateChecked(11);
        frame.AsSpan(PeerFrames.ReplyBytes + 1, payloadLength).Fill(120);
        frame[^1] = 29;
        await using var stream = new MemoryStream(frame);
        var header = GC.AllocateUninitializedArray<byte>(PeerFrames.ReplyBytes);

        var response = await NativePeerClient.ReadReplyAsync(stream, GetValueAsyncResponse.Parser, header, cancellationToken);
        _ = await Assert.That(response.Found).IsTrue();
        if (binary)
        {
            var bytes = response.Value!.BinaryValue;
            _ = await Assert.That(MemoryMarshal.TryGetArray(bytes, out var privateFrame)).IsTrue();
            _ = await Assert.That(privateFrame.Offset).IsEqualTo(1);
            _ = await Assert.That(bytes.Span.SequenceEqual(frame.AsSpan(PeerFrames.ReplyBytes + 1, payloadLength))).IsTrue();
        }
        else
        {
            _ = await Assert.That(response.Value!.StringValue).IsEqualTo(new string('x', payloadLength));
        }

        _ = await Assert.That(stream.ReadByte()).IsEqualTo(29);
    }

    /// <summary>A call that already spent its budget fails before its endpoint resolver can start a connection.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredBudgetSkipsPeerDial(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var node = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a");
        using var certificate = MtlsCertificate.Create(node, bundle.Ca);
        var verifier = new PeerHandshakeVerifier(certificate.PeerChains!, TimeProvider.System, NullLogger.Instance);
        var options = new PeerSocketOptions(new SslClientAuthenticationOptions(), static _ => throw new InvalidOperationException("Expired calls cannot dial."));
        using var client = new NativePeerClient((new Uri("https://localhost"), "node-b"), certificate, verifier, new TrackedConnections(), TimeSpan.FromSeconds(1), null, options);
        var clock = new FakeTimeProvider();
        var budget = ServerDeadlineBudget.Start(clock.GetUtcNow().UtcDateTime.AddSeconds(1), clock);
        clock.Advance(TimeSpan.FromSeconds(2));
        var result = client.SendWithDeadlineAsync(PeerOperation.Ping, new SetAsyncResponse(), SetAsyncResponse.Parser, budget, cancellationToken);
        var error = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(result, TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        _ = await Assert.That(error.StatusCode).IsEqualTo(FrameStatus.DeadlineExceeded);
    }

    /// <summary>EOF before the next request header ends the session normally.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EndOfStreamBetweenHeadersEndsSession(CancellationToken cancellationToken)
    {
        await using var input = new MemoryStream();
        var hasHeader = await NativePeerEndpoint.TryReadHeaderAsync(input, new byte[PeerFrames.RequestBytes], TimeProvider.System, cancellationToken);
        _ = await Assert.That(hasHeader).IsFalse();
    }

    /// <summary>EOF after part of a request header remains a diagnostic failure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EndOfStreamDuringHeaderReportsTruncation(CancellationToken cancellationToken)
    {
        await using var input = new MemoryStream(new byte[PeerFrames.RequestBytes - 1]);
        var pending = NativePeerEndpoint.TryReadHeaderAsync(input, new byte[PeerFrames.RequestBytes], TimeProvider.System, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<EndOfStreamException, bool>(pending, TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
    }

    /// <summary>A peer session waiting for a new frame releases the read when its idle time elapses.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IdleHeaderReadExpires(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var pipe = new Pipe();
        await using var input = pipe.Reader.AsStream();
        var pending = NativePeerEndpoint.TryReadHeaderAsync(input, new byte[PeerFrames.RequestBytes], clock, cancellationToken);
        _ = await Assert.That(pending.IsCompleted).IsFalse();
        clock.Advance(PeerFrames.IdleTimeout);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, bool>(pending, TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        await pipe.Writer.CompleteAsync();
    }

    /// <summary>Reply cleanup expires independently even when the listener is still serving other sessions.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BlockedReplyWriteExpires(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: 1, resumeWriterThreshold: 1));
        await using var output = pipe.Writer.AsStream();
#pragma warning disable CA2025 // The bounded assertion observes the write before the stream leaves its scope.
        var pending = NativePeerEndpoint.WriteAsync(output, new BinaryResponse(FrameStatus.DeadlineExceeded), clock, cancellationToken);
#pragma warning restore CA2025
        _ = await Assert.That(pending.IsCompleted).IsFalse();
        clock.Advance(RpcAttemptTimeout.OperationDeadline);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(pending, TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        await pipe.Reader.CompleteAsync();
    }
}
