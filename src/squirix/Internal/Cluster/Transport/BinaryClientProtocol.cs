using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
    internal static async Task<(TcpClient Socket, SslStream Tls, bool Replay, bool Extended)> ConnectAsync(
        Uri endpoint, Func<CancellationToken, ValueTask<string>>? provider, SslClientAuthenticationOptions? tlsOptions, CancellationToken token)
    {
        try
        {
            return await ConnectAsync(endpoint, provider, tlsOptions, true, token).ConfigureAwait(false);
        }
        catch (ConnectionRejectedException exception) when (exception.PrefaceRejected)
        {
            return await ConnectAsync(endpoint, provider, tlsOptions, false, token).ConfigureAwait(false);
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

    internal static BinaryClientRequest PrepareRequest(BinaryOperation operation, WireMessage request, bool supportsMutationReplay, bool supportsExtendedFrames = true)
    {
        // The send worker can still serialize an earlier attempt after a retry starts.
        var mutationOperationId = PrepareMutationIdentity(request, supportsMutationReplay);
        var original = request;
        var control = Unsafe.BitCast<BinaryOperation, int>(operation) << 16;
        if (Activity.Current is { IdFormat: ActivityIdFormat.W3C } activity)
        {
            var state = activity.TraceStateString;
            var parent = activity.Context;
            request = BinaryTracedRequest.Create(in parent, state is { Length: <= Frames.MaxTraceStateLength } ? state : string.Empty, request);
            control |= Frames.TracedRequest;
        }

        var bodySize = request.CalculateSize(mutationOperationId);
        var frameLimit = supportsExtendedFrames ? Frames.MaxExtendedBodyBytes : Frames.MaxRequestBytes;
        if (bodySize > frameLimit)
            throw RequestTooLarge();

        var valueSize = bodySize <= Frames.MaxRequestBytes ? 0 : CacheWritePayload.Length(original);
        return valueSize is { } size
            ? new BinaryClientRequest(control, request, bodySize, mutationOperationId, size)
            : throw RequestTooLarge();
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
            : new BinaryClientReply(null, ReadFailure(status, body));
    }

    internal static BinaryClientReply ReadBlockingReply(Stream stream, IWireParser? parser, int status, int size, byte[] scratch, int buffered = 0)
    {
        if (parser == null)
        {
            size -= buffered;
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
            if (buffered == 0)
                stream.ReadExactly(scratch.AsSpan(0, 1));
            var found = DecodeDistributedFlag(scratch[0]);
            var payload = GC.AllocateUninitializedArray<byte>(size - 1);
            var prefix = Math.Max(0, buffered - 1);
            scratch.AsSpan(1, prefix).CopyTo(payload);
            stream.ReadExactly(payload.AsSpan(prefix));
            return new BinaryClientReply(GetDistributedAsyncResponse.FromOwnedPayload(found, payload), null);
        }

        var body = size <= scratch.Length ? scratch : ArrayPool<byte>.Shared.Rent(size);
        try
        {
            if (!ReferenceEquals(body, scratch))
                scratch.AsSpan(0, buffered).CopyTo(body);
            stream.ReadExactly(body.AsSpan(buffered, size - buffered));
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

    private static async Task<(TcpClient Socket, SslStream Tls, bool Replay, bool Extended)> ConnectAsync(
        Uri endpoint, Func<CancellationToken, ValueTask<string>>? provider, SslClientAuthenticationOptions? tlsOptions, bool extended, CancellationToken token)
    {
        var socket = new TcpClient { NoDelay = true };
        SslStream? tls = null;
        try
        {
            await socket.ConnectAsync(endpoint.IdnHost, endpoint.Port, token).ConfigureAwait(false);
            tls = new SslStream(socket.GetStream());
            await tls.AuthenticateAsClientAsync(CreateTlsOptions(endpoint, tlsOptions), token).ConfigureAwait(false);
            var supportsMutationReplay = await NegotiateAsync(tls, provider, extended, token).ConfigureAwait(false);
            return (socket, tls, supportsMutationReplay, extended);
        }
        catch
        {
            if (tls != null)
                await tls.DisposeAsync().ConfigureAwait(false);
            socket.Dispose();
            throw;
        }
    }

    private static SquirixTransportException ReadFailure(int status, ReadOnlySpan<byte> body)
    {
        var error = BinaryCallError.Parser.ParseFrom(body);
        var code = Unsafe.BitCast<int, CacheTransportStatusCode>(status);
        foreach (var trailer in CollectionsMarshal.AsSpan(error.Trailers))
        {
            if (string.Equals(trailer.Key, Frames.ErrorCodeTrailer, StringComparison.Ordinal) &&
                string.Equals(trailer.Text, Frames.PayloadTooLargeCode, StringComparison.Ordinal))
                return new SquirixTransportException(new CallStatus(code, error.Detail, PayloadTooLarge: true));
        }

        return new SquirixTransportException(new CallStatus(code, error.Detail));
    }

    private static SquirixTransportException RequestTooLarge() =>
        new(new CallStatus(CacheTransportStatusCode.ResourceExhausted, "Binary cache request is too large.", PayloadTooLarge: true));

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
        Frames.Rejected => throw new ConnectionRejectedException { PrefaceRejected = true },
        _ => throw new SquirixTransportException(new CallStatus(CacheTransportStatusCode.FailedPrecondition, "The endpoint does not speak the Squirix cache protocol.")),
    };

    private static async Task<bool> NegotiateAsync(SslStream tls, Func<CancellationToken, ValueTask<string>>? provider, bool extended, CancellationToken token)
    {
        var preface = ArrayPool<byte>.Shared.Rent(Frames.PrefaceBytes);
        try
        {
            BinaryPrimitives.WriteUInt32BigEndian(preface, Frames.Preface);
            const byte extendedBearerCredentials = Frames.ExtendedMode | 1;
            const byte legacyAnonymous = 0;
            var anonymous = extended ? Frames.ExtendedMode : legacyAnonymous;
            const byte legacyBearer = 1;
            var bearer = extended ? extendedBearerCredentials : legacyBearer;
            preface[4] = provider == null ? anonymous : bearer;
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
