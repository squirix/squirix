using System;
using System.Buffers;
using System.Net;
using System.Net.Security;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
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

    internal async Task ServeAsync(SslStream stream, IPEndPoint remote, CancellationToken graceful, CancellationToken abort)
    {
        ArgumentNullException.ThrowIfNull(remote);
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
            if (!await ProcessAsync(stream, frame, certificate, graceful, abort).ConfigureAwait(false))
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

    private async Task<bool> ProcessAsync(SslStream stream, PeerFrames.Request frame, X509Certificate2 certificate, CancellationToken graceful, CancellationToken abort)
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
            reading.CancelAfter(TimeSpan.FromSeconds(30));
            await stream.ReadExactlyAsync(meta.AsMemory(0, frame.Metadata), reading.Token).ConfigureAwait(false);
            await stream.ReadExactlyAsync(body.AsMemory(0, frame.Body), reading.Token).ConfigureAwait(false);
            if (!_verifier.TryVerify(certificate, out var peer))
                return false;
            reply = await DispatchAsync(frame, meta, body.AsMemory(0, frame.Body), certificate, peer, token).ConfigureAwait(false);
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

    private async Task<BinaryResponse> DispatchAsync(PeerFrames.Request frame, byte[] meta, ReadOnlyMemory<byte> body, X509Certificate2 certificate, string peer, CancellationToken cancellationToken)
    {
        var path = PeerFrames.Text.GetString(meta, 0, frame.Path);
        if (string.Equals(path, "ping", StringComparison.Ordinal))
            return new BinaryResponse(body.IsEmpty ? FrameStatus.Ok : FrameStatus.InvalidArgument);
        var scope = _services.CreateAsyncScope();
        await using var lifetime = scope.ConfigureAwait(false);
        var services = scope.ServiceProvider;
        var offset = frame.Path;
        var fingerprint = frame.Fingerprint == 0 ? null : PeerFrames.Text.GetString(meta, offset, frame.Fingerprint);
        offset += frame.Fingerprint;
        var parent = frame.Parent == 0 ? null : PeerFrames.Text.GetString(meta, offset, frame.Parent);
        offset += frame.Parent;
        var state = frame.State == 0 ? null : PeerFrames.Text.GetString(meta, offset, frame.State);
        var fullPath = PeerProtocol.Prefix + path;
        if (path.StartsWith("owner/", StringComparison.Ordinal))
        {
            var name = path["owner/".Length..];
            if (!Enum.TryParse<BinaryOperation>(name, out var operation) || !string.Equals(Enum.GetName(operation), name, StringComparison.Ordinal) || operation == BinaryOperation.Unspecified)
                return new BinaryResponse(FrameStatus.Unimplemented);
            services.GetRequiredService<RingAgreement>().EnsureInboundAgreement(fingerprint, peer);
            var call = new BinaryRequestContext { Path = fullPath, RequestAborted = cancellationToken, RequestServices = services, TraceParent = parent, TraceState = state };
            var request = new BinaryDispatch.Request(call, DateTime.MaxValue, parent, state, Task.CompletedTask, true);
            return await services.GetRequiredService<BinaryDispatch>().ExecuteAsync(operation, body, in request).ConfigureAwait(false);
        }

        if (!_replication || !path.StartsWith("replication/", StringComparison.Ordinal))
            return new BinaryResponse(FrameStatus.Unimplemented);
        var context = new PeerCallContext { ClientCertificate = certificate, InternalListener = true, LocalPort = _port, RequestAborted = cancellationToken };
        using var activity = ServerCallCorrelation.StartActivity(fullPath, parent, state);
        using var log = services.GetRequiredService<ServerCallCorrelation>().BeginLogScope(fullPath);
        return await PeerDispatch.DispatchAsync(path["replication/".Length..], body, context, services.GetRequiredService<SquirixReplicationServiceAdapter>()).ConfigureAwait(false);
    }
}
