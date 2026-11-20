using System;
using System.IO;
using System.IO.Pipelines;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Checks cancellation of peer I/O independently of application dispatch.</summary>
public sealed class PeerSessionDeadlineTests
{
    /// <summary>The sender omits optional tracing fields outside the encoded byte budget.</summary>
    [Test]
    public async Task TraceMetadataHonorsEncodedByteLimit()
    {
        var trace = new string('é', Frames.MaxTraceStateLength / 2);
        _ = await Assert.That(NativePeerClient.TraceText(trace, Frames.MaxTraceStateLength)).IsEqualTo(trace);
        _ = await Assert.That(NativePeerClient.TraceText(trace + "x", Frames.MaxTraceStateLength)).IsEmpty();
    }

    /// <summary>A reply with an incompatible header reports data loss through the peer-call contract.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InvalidReplyHeaderReportsDataLoss(CancellationToken cancellationToken)
    {
        await using var input = new MemoryStream(new byte[PeerFrames.ReplyBytes]);
        var error = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(NativePeerClient.ReadReplyAsync(input, Empty.Parser, new byte[PeerFrames.ReplyBytes], cancellationToken), TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        _ = await Assert.That(error.StatusCode).IsEqualTo(FrameStatus.DataLoss);
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
        using var budget = ServerRpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime.AddSeconds(1), clock);
        clock.Advance(TimeSpan.FromSeconds(2));
        var result = client.SendAsync("ping", new Empty(), Empty.Parser, cancellationToken);
        var error = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(result, TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        _ = await Assert.That(error.StatusCode).IsEqualTo(FrameStatus.DeadlineExceeded);
    }

    /// <summary>A peer session waiting for a new frame releases the read when its idle time elapses.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IdleHeaderReadExpires(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var pipe = new Pipe();
        await using var input = pipe.Reader.AsStream();
        var pending = NativePeerEndpoint.ReadHeaderAsync(input, new byte[PeerFrames.RequestBytes], clock, cancellationToken);
        _ = await Assert.That(pending.IsCompleted).IsFalse();
        clock.Advance(PeerFrames.IdleTimeout);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(pending, TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
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
