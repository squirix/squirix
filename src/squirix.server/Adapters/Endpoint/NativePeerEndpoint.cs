using System;
using System.Buffers;
using System.Net;
using System.Net.Security;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Invocation;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Dispatches authenticated peer frames on the cluster listener.</summary>
internal sealed class NativePeerEndpoint
{
    private readonly IServiceProvider _services;
    private readonly VerifiedPeerCertificates _verifier;
    private readonly int _port;
    private readonly bool _replication;

    internal NativePeerEndpoint(IServiceProvider services, int port, bool replication)
    {
        _services = services;
        _verifier = services.GetRequiredService<VerifiedPeerCertificates>();
        _port = port;
        _replication = replication;
    }

    internal static Task<BinaryResponse> ForwardAsync(HttpContext context, ReadOnlyMemory<byte> body, string peer)
    {
        if (context.Request.RouteValues["operation"] is not string name || !Enum.TryParse<BinaryOperation>(name, out var operation) || !string.Equals(Enum.GetName(operation), name, StringComparison.Ordinal) || operation == BinaryOperation.Unspecified)
            return Task.FromResult(new BinaryResponse(FrameStatus.Unimplemented));
        var fingerprint = context.Request.Headers[RemoteInvocationContract.RingFingerprintHeaderName];
        context.RequestServices.GetRequiredService<RingAgreement>().EnsureInboundAgreement(fingerprint.Count == 1 ? fingerprint[0] : null, peer);
        var request = new BinaryDispatch.Request(context, DateTime.MaxValue, context.Request.Headers["traceparent"], context.Request.Headers["tracestate"], Task.CompletedTask, true);
        return context.RequestServices.GetRequiredService<BinaryDispatch>().ExecuteAsync(operation, body, in request).AsTask();
    }

    internal async Task ServeAsync(SslStream stream, IPEndPoint remote, CancellationToken graceful, CancellationToken abort)
    {
        if (stream.NegotiatedApplicationProtocol != PeerFrames.Protocol || stream.RemoteCertificate is not X509Certificate2 certificate)
            return;
        using var buffer = MemoryPool<byte>.Shared.Rent(PeerFrames.RequestBytes);
        var header = buffer.Memory[..PeerFrames.RequestBytes];
        while (!graceful.IsCancellationRequested)
        {
            await stream.ReadExactlyAsync(header, graceful).ConfigureAwait(false);
            if (!_verifier.TryVerify(certificate, out _))
                return;
            var frame = PeerFrames.ReadRequest(header.Span);
            if (!await ProcessAsync(stream, frame, certificate, remote, graceful, abort).ConfigureAwait(false))
                return;
        }
    }

    private static async Task WriteAsync(SslStream stream, BinaryResponse reply, CancellationToken cancellationToken)
    {
        var length = reply.Payload?.CalculateSize() ?? 0;
        if (length > EntryLimits.MaxSendMessageSizeBytes)
        {
            reply = new BinaryResponse(FrameStatus.ResourceExhausted);
            length = 0;
        }

        var packet = ArrayPool<byte>.Shared.Rent(PeerFrames.ReplyBytes + length);
        try
        {
            PeerFrames.WriteReply(packet, Unsafe.BitCast<FrameStatus, int>(reply.Status), length);
            reply.Payload?.WriteTo(packet.AsSpan(PeerFrames.ReplyBytes, length));
            await stream.WriteAsync(packet.AsMemory(0, PeerFrames.ReplyBytes + length), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(packet, true);
        }
    }

    private async Task<bool> ProcessAsync(SslStream stream, PeerFrames.Request frame, X509Certificate2 certificate, IPEndPoint remote, CancellationToken graceful, CancellationToken abort)
    {
        var clock = _services.GetRequiredService<TimeProvider>();
        var started = clock.GetTimestamp();
        TimeSpan? budget = frame.Budget < 0 ? null : TimeSpan.FromTicks(frame.Budget);
        using var deadlineScope = budget is { } time ? ServerRpcDeadlineContext.Push(new ServerDeadlineBudget(clock, time, started)) : null;
        using var deadline = new TimedCancellation(budget ?? Timeout.InfiniteTimeSpan, clock, abort);
        var meta = ArrayPool<byte>.Shared.Rent(frame.Metadata);
        var body = ArrayPool<byte>.Shared.Rent(frame.Body);
        BinaryResponse reply;
        try
        {
            var token = deadline.Token;
            using var reading = CancellationTokenSource.CreateLinkedTokenSource(token, graceful);
            await stream.ReadExactlyAsync(meta.AsMemory(0, frame.Metadata), reading.Token).ConfigureAwait(false);
            await stream.ReadExactlyAsync(body.AsMemory(0, frame.Body), reading.Token).ConfigureAwait(false);
            if (!_verifier.TryVerify(certificate, out var peer))
                return false;
            reply = await DispatchAsync(frame, meta, body.AsMemory(0, frame.Body), certificate, peer, remote, token).ConfigureAwait(false);
        }
        catch (ServerCallException error)
        {
            reply = new BinaryResponse(error.StatusCode, error.Error);
        }
        catch (SquirixException error)
        {
            reply = BinaryResponse.FromError(error);
        }
        catch (InvalidProtocolBufferException)
        {
            reply = new BinaryResponse(FrameStatus.InvalidArgument);
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested && !abort.IsCancellationRequested)
        {
            await WriteAsync(stream, new BinaryResponse(FrameStatus.DeadlineExceeded), abort).ConfigureAwait(false);
            return false;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(body, true);
            ArrayPool<byte>.Shared.Return(meta, true);
        }

        await WriteAsync(stream, reply, abort).ConfigureAwait(false);
        return true;
    }

    private async Task<BinaryResponse> DispatchAsync(PeerFrames.Request frame, byte[] meta, ReadOnlyMemory<byte> body, X509Certificate2 certificate, string peer, IPEndPoint remote, CancellationToken cancellationToken)
    {
        var path = PeerFrames.Text.GetString(meta, 0, frame.Path);
        if (string.Equals(path, "ping", StringComparison.Ordinal))
            return new BinaryResponse(body.IsEmpty ? FrameStatus.Ok : FrameStatus.InvalidArgument);
        var scope = _services.CreateAsyncScope();
        await using var lifetime = scope.ConfigureAwait(false);
        var http = new DefaultHttpContext { RequestServices = scope.ServiceProvider, RequestAborted = cancellationToken };
        http.Connection.ClientCertificate = certificate;
        http.Connection.RemoteIpAddress = remote.Address;
        http.Connection.RemotePort = remote.Port;
        http.Connection.LocalPort = _port;
        http.Features.Set(InternalListenerMarker.Instance);
        http.Request.Path = PeerProtocol.Prefix + path;
        var offset = frame.Path;
        if (frame.Fingerprint != 0)
            http.Request.Headers[RemoteInvocationContract.RingFingerprintHeaderName] = PeerFrames.Text.GetString(meta, offset, frame.Fingerprint);
        offset += frame.Fingerprint;
        if (frame.Parent != 0)
            http.Request.Headers["traceparent"] = PeerFrames.Text.GetString(meta, offset, frame.Parent);
        offset += frame.Parent;
        if (frame.State != 0)
            http.Request.Headers["tracestate"] = PeerFrames.Text.GetString(meta, offset, frame.State);

        if (path.StartsWith("owner/", StringComparison.Ordinal))
        {
            http.Request.RouteValues["operation"] = path["owner/".Length..];
            return await ForwardAsync(http, body, peer).ConfigureAwait(false);
        }

        if (!_replication || !path.StartsWith("replication/", StringComparison.Ordinal))
            return new BinaryResponse(FrameStatus.Unimplemented);
        http.Request.RouteValues["operation"] = path["replication/".Length..];
        return await PeerDispatch.DispatchAsync(http, body, peer).ConfigureAwait(false);
    }
}
