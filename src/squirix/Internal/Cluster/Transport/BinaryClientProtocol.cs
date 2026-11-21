using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Transport;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Internal.Cluster.Transport;

internal static class BinaryClientProtocol
{
    internal static async Task<(TcpClient Socket, SslStream Tls, bool SupportsMutationReplay)> ConnectAsync(
        Uri endpoint, Func<CancellationToken, ValueTask<string>>? provider, SslClientAuthenticationOptions? tlsOptions, CancellationToken token)
    {
        var socket = new TcpClient { NoDelay = true };
        SslStream? tls = null;
        try
        {
            await socket.ConnectAsync(endpoint.IdnHost, endpoint.Port, token).ConfigureAwait(false);
            tls = new SslStream(socket.GetStream());
            await tls.AuthenticateAsClientAsync(CreateTlsOptions(endpoint, tlsOptions), token).ConfigureAwait(false);
            var supportsMutationReplay = await NegotiateAsync(tls, provider, token).ConfigureAwait(false);
            return (socket, tls, supportsMutationReplay);
        }
        catch
        {
            if (tls != null)
                await tls.DisposeAsync().ConfigureAwait(false);
            socket.Dispose();
            throw;
        }
    }

    internal static SetDistributedAsyncRequest CreateSetRequest(string cacheName, string key, byte[] ownedValue, long? expiresAtUnixMs, long? ttlTicks, long? slidingTicks)
    {
        var request = new SetDistributedAsyncRequest
        {
            CacheName = cacheName,
            Key = key,
            OperationId = RpcOperationIdentity.New(),
            Value = UnsafeByteOperations.UnsafeWrap(ownedValue),
        };
        if (expiresAtUnixMs is { } cap)
            request.ExpiresAtUnixMs = cap;
        if (ttlTicks is { } ttl)
            request.TtlTicks = ttl;
        if (slidingTicks is { } interval)
            request.SlidingTicks = interval;

        return request;
    }

    internal static (int Control, IMessage Message, int Size) PrepareRequest(BinaryOperation operation, IMessage request)
    {
        var bodySize = request.CalculateSize();
        if (bodySize > Frames.MaxRequestBytes)
            throw new SquirixTransportException(new CallStatus(CacheTransportStatusCode.ResourceExhausted, "Binary cache request is too large."));

        var control = Unsafe.BitCast<BinaryOperation, int>(operation) << 16;
        if (Activity.Current is { IdFormat: ActivityIdFormat.W3C, Id: { Length: <= Frames.MaxTraceParentLength } parent } activity)
        {
            var state = activity.TraceStateString;
            request = new BinaryTracedRequest
            {
                TraceParent = parent,
                TraceState = state is { Length: <= Frames.MaxTraceStateLength } ? state : string.Empty,
                Payload = request.ToByteString(),
            };
            bodySize = request.CalculateSize();
            if (bodySize > Frames.MaxRequestBytes)
                throw new SquirixTransportException(new CallStatus(CacheTransportStatusCode.ResourceExhausted, "Binary cache request is too large."));
            control |= Frames.TracedRequest;
        }

        return (control, request, bodySize);
    }

    internal static bool IsOutcomeUnconfirmed(SquirixTransportException? failure) =>
        failure == null ||
        ((failure.StatusCode is CacheTransportStatusCode.Unavailable or CacheTransportStatusCode.DeadlineExceeded or CacheTransportStatusCode.Cancelled or CacheTransportStatusCode.Internal)
         && !string.Equals(failure.Status.Detail, Frames.OwnerUnreachableDetail, StringComparison.Ordinal));

    internal static SquirixTransportException UnknownOutcome(Exception? cause = null) =>
        new(new CallStatus(CacheTransportStatusCode.Unavailable, CommitOutcomeUnknownDetail.Value, cause));

    internal static void ValidateResponseStatus(int status)
    {
        if (status < 0 || status > Unsafe.BitCast<CacheTransportStatusCode, int>(CacheTransportStatusCode.Unauthenticated))
            throw new InvalidDataException("Unexpected binary cache reply status.");
    }

    internal static BinaryClientReply ParseReply(MessageParser parser, int status, ReadOnlySpan<byte> body)
    {
        return status == 0
            ? new BinaryClientReply(parser.ParseFrom(body), null)
            : new BinaryClientReply(null, new SquirixTransportException(
                new CallStatus(Unsafe.BitCast<int, CacheTransportStatusCode>(status), BinaryCallError.Parser.ParseFrom(body).Detail)));
    }

    private static SslClientAuthenticationOptions CreateTlsOptions(Uri endpoint, SslClientAuthenticationOptions? tlsOptions)
    {
        return new SslClientAuthenticationOptions
        {
            TargetHost = endpoint.IdnHost,
            RemoteCertificateValidationCallback = tlsOptions?.RemoteCertificateValidationCallback,
            CertificateRevocationCheckMode = tlsOptions?.CertificateRevocationCheckMode ?? System.Security.Cryptography.X509Certificates.X509RevocationMode.NoCheck,
        };
    }

    private static bool SupportsMutationReplay(byte preface) => preface switch
    {
        Frames.Accepted => false,
        Frames.AcceptedWithMutationReplay => true,
        Frames.Rejected => throw new ConnectionRejectedException(),
        _ => throw new SquirixTransportException(new CallStatus(CacheTransportStatusCode.FailedPrecondition, "The endpoint does not speak the Squirix cache protocol.")),
    };

    private static async Task<bool> NegotiateAsync(SslStream tls, Func<CancellationToken, ValueTask<string>>? provider, CancellationToken token)
    {
        var preface = ArrayPool<byte>.Shared.Rent(Frames.PrefaceBytes);
        try
        {
            BinaryPrimitives.WriteUInt32BigEndian(preface, Frames.Preface);
            const byte noCredentials = 0;
            const byte bearerCredentials = 1;
            preface[4] = provider == null ? noCredentials : bearerCredentials;
            await tls.WriteAsync(preface.AsMemory(0, Frames.PrefaceBytes), token).ConfigureAwait(false);
            await tls.ReadExactlyAsync(preface.AsMemory(0, 1), token).ConfigureAwait(false);
            var replay = SupportsMutationReplay(preface[0]);
            if (provider != null)
                _ = await ConnectionAuthentication.AuthenticateAsync(tls, provider, token).ConfigureAwait(false);
            return replay;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(preface);
        }
    }
}
