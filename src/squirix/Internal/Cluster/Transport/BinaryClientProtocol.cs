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
using Squirix.Internal.Cluster.Reliability;
using Squirix.Transport;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;

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
            Value = ownedValue,
        };
        if (expiresAtUnixMs is { } cap)
            request.ExpiresAtUnixMs = cap;
        if (ttlTicks is { } ttl)
            request.TtlTicks = ttl;
        if (slidingTicks is { } interval)
            request.SlidingTicks = interval;

        return request;
    }

    internal static string? PrepareMutationIdentity(WireMessage request, bool supportsMutationReplay)
    {
        if (request is not CacheMutationRequest mutation)
            return null;
        if (!supportsMutationReplay)
            return string.Empty;

        if (mutation.OperationId.Length == 0)
            mutation.OperationId = RpcOperationIdentity.New();
        return mutation.OperationId;
    }

    internal static BinaryClientRequest PrepareRequest(BinaryOperation operation, WireMessage request, bool supportsMutationReplay)
    {
        // The send worker can still serialize an earlier attempt after a retry starts.
        var mutationOperationId = PrepareMutationIdentity(request, supportsMutationReplay);
        var control = Unsafe.BitCast<BinaryOperation, int>(operation) << 16;
        if (Activity.Current is { IdFormat: ActivityIdFormat.W3C } activity)
        {
            var state = activity.TraceStateString;
            var parent = activity.Context;
            request = BinaryTracedRequest.Create(in parent, state is { Length: <= Frames.MaxTraceStateLength } ? state : string.Empty, request);
            control |= Frames.TracedRequest;
        }

        var bodySize = request.CalculateSize(mutationOperationId);
        return bodySize <= Frames.MaxRequestBytes
            ? new BinaryClientRequest(control, request, bodySize, mutationOperationId)
            : throw new SquirixTransportException(new CallStatus(CacheTransportStatusCode.ResourceExhausted, "Binary cache request is too large."));
    }

    internal static bool IsOutcomeUnconfirmed(SquirixTransportException? failure) =>
        failure == null ||
        ((failure.StatusCode is CacheTransportStatusCode.Unavailable or CacheTransportStatusCode.DeadlineExceeded or CacheTransportStatusCode.Cancelled or CacheTransportStatusCode.Internal)
         && !string.Equals(failure.Status.Detail, Frames.OwnerUnreachableDetail, StringComparison.Ordinal));

    internal static SquirixTransportException UnknownOutcome(Exception? cause = null) =>
        new(new CallStatus(CacheTransportStatusCode.Unavailable, CommitOutcomeUnknownDetail.Value, cause));

    internal static BinaryClientReply ParseReply(IWireParser parser, int status, ReadOnlySpan<byte> body)
    {
        return status == 0
            ? new BinaryClientReply(parser.ParseFrom(body), null)
            : new BinaryClientReply(null, new SquirixTransportException(
                new CallStatus(Unsafe.BitCast<int, CacheTransportStatusCode>(status), BinaryCallError.Parser.ParseFrom(body).Detail)));
    }

    internal static BinaryClientReply ReadBlockingReply(Stream stream, IWireParser? parser, int status, int size, byte[] scratch)
    {
        if (parser == null)
        {
            while (size != 0)
            {
                var length = Math.Min(size, scratch.Length);
                stream.ReadExactly(scratch.AsSpan(0, length));
                size -= length;
            }

            return default;
        }

        if (size > scratch.Length && HasOwnedDistributedPayload(parser, status))
        {
            stream.ReadExactly(scratch.AsSpan(0, 1));
            var found = DecodeDistributedFlag(scratch[0]);
            var payload = GC.AllocateUninitializedArray<byte>(size - 1);
            stream.ReadExactly(payload);
            return new BinaryClientReply(GetDistributedAsyncResponse.FromOwnedPayload(found, payload), null);
        }

        var body = size <= scratch.Length ? scratch : ArrayPool<byte>.Shared.Rent(size);
        try
        {
            stream.ReadExactly(body.AsSpan(0, size));
            return ParseReply(parser, status, body.AsSpan(0, size));
        }
        finally
        {
            if (!ReferenceEquals(body, scratch))
                ArrayPool<byte>.Shared.Return(body);
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<BinaryClientReply> ReadLargeLookupReplyAsync(Stream stream, IWireParser parser, int size, byte[] scratch, CancellationToken cancellationToken)
    {
        await stream.ReadExactlyAsync(scratch.AsMemory(0, 1), cancellationToken).ConfigureAwait(false);
        var flags = scratch[0];
        var distributed = ReferenceEquals(parser, GetDistributedAsyncResponse.Parser);
        if (distributed || GetValueAsyncResponse.HasBinaryPayload(flags))
        {
            var found = distributed && DecodeDistributedFlag(flags);
            var payload = GC.AllocateUninitializedArray<byte>(size - 1);
            await stream.ReadExactlyAsync(payload, cancellationToken).ConfigureAwait(false);
            WireMessage response = distributed
                ? GetDistributedAsyncResponse.FromOwnedPayload(found, payload)
                : GetValueAsyncResponse.FromOwnedBinary(flags, payload);
            return new BinaryClientReply(response, null);
        }

        var body = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            body[0] = flags;
            await stream.ReadExactlyAsync(body.AsMemory(1, size - 1), cancellationToken).ConfigureAwait(false);
            return ParseReply(parser, 0, body.AsSpan(0, size));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(body);
        }
    }

    internal static bool CanReadLargeLookupReply(IWireParser parser, int status) =>
        status == 0 && (ReferenceEquals(parser, GetDistributedAsyncResponse.Parser) || ReferenceEquals(parser, GetValueAsyncResponse.Parser));

    private static bool HasOwnedDistributedPayload(IWireParser parser, int status) =>
        status == 0 && ReferenceEquals(parser, GetDistributedAsyncResponse.Parser);

    private static bool DecodeDistributedFlag(byte flag) => flag switch
    {
        0 => false,
        1 => true,
        _ => throw new InvalidDataException("Malformed binary message."),
    };

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
