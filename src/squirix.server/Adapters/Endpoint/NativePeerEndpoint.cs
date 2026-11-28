using System;
using System.Buffers;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Composition;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Codec;

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
        _verifier = services.Require<VerifiedPeerCertificates>();
        _port = port;
        _replication = replication;
    }

    internal static async ValueTask<bool> TryReadHeaderAsync(Stream stream, Memory<byte> header, TimeProvider clock, CancellationToken graceful)
    {
        using var idle = new TimedCancellation(PeerFrames.IdleTimeout, clock, graceful);
        var read = await stream.ReadAtLeastAsync(header, header.Length, false, idle.Token).ConfigureAwait(false);
        return read switch
        {
            0 => false,
            _ when read == header.Length => true,
            _ => throw new EndOfStreamException($"Peer connection ended after {read} of {header.Length} request header bytes."),
        };
    }

    internal static async Task WriteAsync(Stream stream, BinaryResponse reply, TimeProvider clock, CancellationToken abort)
    {
        using var writing = new TimedCancellation(RpcAttemptTimeout.OperationDeadline, clock, abort);
        var length = reply.Payload?.CalculateSize() ?? 0;
        if (length > Frames.MaxExtendedBodyBytes)
        {
            reply = new BinaryResponse(FrameStatus.ResourceExhausted);
            length = 0;
        }

        if (length > EntryLimits.MaxSendMessageSizeBytes)
        {
            using var message = new WireSegments();
            const int headerSize = PeerFrames.ReplyBytes + Frames.ExtendedReplyBytes;
            var header = message.GetMemory(headerSize);
            PeerFrames.WriteReply(header.Span, Unsafe.BitCast<FrameStatus, int>(reply.Status), Frames.ExtendedSize);
            Frames.WriteExtendedSize(header.Span[PeerFrames.ReplyBytes..], length);
            message.Advance(headerSize);
            reply.Payload!.WriteTo(message);
            await message.SendAsync(stream, writing.Token).ConfigureAwait(false);
            return;
        }

        var packet = ArrayPool<byte>.Shared.Rent(PeerFrames.ReplyBytes + length);
        try
        {
            PeerFrames.WriteReply(packet, Unsafe.BitCast<FrameStatus, int>(reply.Status), length);
            reply.Payload?.WriteTo(packet.AsSpan(PeerFrames.ReplyBytes, length));
            await stream.WriteAsync(packet.AsMemory(0, PeerFrames.ReplyBytes + length), writing.Token).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(packet, true);
        }
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
            if (!await TryReadHeaderAsync(stream, header, TimeProvider.System, graceful).ConfigureAwait(false))
                return;

            if (!_verifier.TryVerify(certificate, out _))
                return;
            var frame = PeerFrames.ReadRequest(header.Span);
            var processed = frame.Body == Frames.ExtendedSize
                ? await ProcessExtendedAsync(stream, header, frame, certificate, graceful, abort).ConfigureAwait(false)
                : await ProcessAsync(stream, frame, certificate, graceful, abort).ConfigureAwait(false);
            if (!processed)
                return;
        }
    }

    private static string? ReadTraceParent(in PeerFrames.Request frame, byte[] metadata)
    {
        if (frame.Parent == 0)
            return null;

        var bytes = metadata.AsSpan(frame.Fingerprint, frame.Parent);
        return frame.TraceParentIsBinary ? WireTraceParent.Read(bytes) : PeerFrames.Text.GetString(bytes);
    }

    private static void ReturnFrameBuffers(byte[] metadata, byte[] body, bool extended)
    {
        if (!extended)
            ArrayPool<byte>.Shared.Return(body, true);
        ArrayPool<byte>.Shared.Return(metadata, true);
    }

    private async Task<bool> ProcessExtendedAsync(SslStream stream, Memory<byte> header, PeerFrames.Request frame, X509Certificate2 certificate, CancellationToken graceful, CancellationToken abort)
    {
        using var extensionDeadline = new TimedCancellation(PeerFrames.IdleTimeout, TimeProvider.System, graceful);
        await stream.ReadExactlyAsync(header[..sizeof(int)], extensionDeadline.Token).ConfigureAwait(false);
        var size = Frames.ReadExtendedSize(header.Span);
        var admission = _services.Require<LargeCacheRequests>();
        if (!admission.TryReservePeerFrame(size))
            return false;

        try
        {
            return await ProcessAsync(stream, frame with { Body = size }, certificate, graceful, abort).ConfigureAwait(false);
        }
        finally
        {
            admission.ReleasePeerFrame(size);
        }
    }

    private async Task<bool> ProcessAsync(SslStream stream, PeerFrames.Request frame, X509Certificate2 certificate, CancellationToken graceful, CancellationToken abort)
    {
        var clock = _services.Require<TimeProvider>();
        var started = clock.GetTimestamp();
        TimeSpan? budget = frame.Budget < 0 ? null : TimeSpan.FromTicks(frame.Budget);
        using var deadlineScope = budget is { } time ? ServerRpcDeadlineContext.Push(new ServerDeadlineBudget(clock, time, started)) : null;
        using var deadline = new TimedCancellation(budget ?? Timeout.InfiniteTimeSpan, clock, abort);
        var meta = ArrayPool<byte>.Shared.Rent(frame.Metadata);

        // Extended frames already hold a reservation. Avoid retaining those buffers in the shared pool.
        var extended = frame.Body > EntryLimits.MaxReceiveMessageSizeBytes;
        var body = extended ? GC.AllocateUninitializedArray<byte>(frame.Body) : ArrayPool<byte>.Shared.Rent(frame.Body);
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
        catch (InvalidDataException)
        {
            reply = new BinaryResponse(FrameStatus.InvalidArgument);
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested && !abort.IsCancellationRequested)
        {
            await WriteAsync(stream, new BinaryResponse(FrameStatus.DeadlineExceeded), TimeProvider.System, abort).ConfigureAwait(false);
            return false;
        }
        finally
        {
            ReturnFrameBuffers(meta, body, extended);
        }

        await WriteAsync(stream, reply, TimeProvider.System, abort).ConfigureAwait(false);
        return true;
    }

    private async Task<BinaryResponse> DispatchAsync(PeerFrames.Request frame, byte[] meta, ReadOnlyMemory<byte> body, X509Certificate2 certificate, string peer, CancellationToken cancellationToken)
    {
        if (frame.Operation == PeerOperation.Ping)
            return new BinaryResponse(body.IsEmpty ? FrameStatus.Ok : FrameStatus.InvalidArgument);
        var (fullPath, ownerOperation) = PeerProtocol.Route(frame.Operation);
        if (fullPath == null)
            return new BinaryResponse(FrameStatus.Unimplemented);
        var parent = ReadTraceParent(in frame, meta);
        var state = frame.State == 0 ? null : PeerFrames.Text.GetString(meta, frame.Fingerprint + frame.Parent, frame.State);
        if (ownerOperation is { } operation)
        {
            var agreement = _services.Require<RingAgreement>();
            if (frame.FingerprintIsDigest)
                agreement.EnsureInboundDigestAgreement(meta.AsSpan(0, frame.Fingerprint), peer);
            else
                agreement.EnsureInboundAgreement(frame.Fingerprint == 0 ? null : PeerFrames.Text.GetString(meta, 0, frame.Fingerprint), peer);
            var call = new BinaryRequestContext { Path = fullPath, RequestAborted = cancellationToken, TraceParent = parent, TraceState = state };
            var request = new BinaryDispatch.Request(call, default, Task.CompletedTask, true);
            var accessor = _services.Require<BinaryRequestAccessor>();
            accessor.Current = call;
            try
            {
                return await _services.Require<BinaryDispatch>().ExecuteAsync(operation, body, in request).ConfigureAwait(false);
            }
            finally
            {
                accessor.Current = null;
            }
        }

        if (!_replication)
            return new BinaryResponse(FrameStatus.Unimplemented);
        var context = new PeerCallContext { ClientCertificate = certificate, InternalListener = true, LocalPort = _port, RequestAborted = cancellationToken };
        using var activity = ServerCallCorrelation.StartActivity(fullPath, parent, state);
        using var log = _services.Require<ServerCallCorrelation>().BeginLogScope(fullPath);
        return await PeerDispatch.DispatchAsync(frame.Operation, body, context, _services.Require<SquirixReplicationServiceAdapter>()).ConfigureAwait(false);
    }
}
