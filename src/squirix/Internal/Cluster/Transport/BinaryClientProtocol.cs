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
    internal static async Task<(TcpClient Socket, Stream Stream, bool Replay, bool Extended)> ConnectAsync(
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

    internal static BinaryClientRequest PrepareRequest(BinaryOperation operation, WireMessage request, bool supportsMutationReplay, bool supportsExtendedFrames = true, byte[]? capture = null)
    {
        var captured = Capture(operation, request, supportsMutationReplay);
        return captured.Measure(supportsExtendedFrames, capture, capture);
    }

    internal static BinaryClientRequest Capture(BinaryOperation operation, WireMessage request, bool supportsMutationReplay)
    {
        // Freeze call-specific trace and mutation identity before handing the immutable request to the writer.
        var mutationOperationId = PrepareMutationIdentity(request, supportsMutationReplay);
        var control = Unsafe.BitCast<BinaryOperation, int>(operation) << 16;
        if (Activity.Current is { IdFormat: ActivityIdFormat.W3C } activity)
        {
            var state = activity.TraceStateString;
            var parent = activity.Context;
            request = BinaryTracedRequest.Create(in parent, state is { Length: <= Frames.MaxTraceStateLength } ? state : string.Empty, request);
            control |= Frames.TracedRequest;
        }

        return new BinaryClientRequest(control, request, -1, mutationOperationId, 0, false);
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

    internal static BinaryClientReply ReadBlockingReply(Stream stream, IWireParser? parser, int status, int size, byte[] scratch, ReadOnlySpan<byte> prefix = default)
    {
        if (parser == null)
        {
            size -= prefix.Length;
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
            if (prefix.IsEmpty)
            {
                stream.ReadExactly(scratch.AsSpan(0, 1));
                prefix = scratch.AsSpan(0, 1);
            }

            var found = DecodeDistributedFlag(prefix[0]);
            var payload = GC.AllocateUninitializedArray<byte>(size - 1);
            var bufferedPayload = prefix[1..];
            bufferedPayload.CopyTo(payload);
            stream.ReadExactly(payload.AsSpan(bufferedPayload.Length));
            return new BinaryClientReply(GetDistributedAsyncResponse.FromOwnedPayload(found, payload), null);
        }

        var body = size <= scratch.Length ? scratch : ArrayPool<byte>.Shared.Rent(size);
        try
        {
            prefix.CopyTo(body);
            stream.ReadExactly(body.AsSpan(prefix.Length, size - prefix.Length));
            return ParseReply(parser, status, body.AsSpan(0, size));
        }
        finally
        {
            if (!ReferenceEquals(body, scratch))
                ArrayPool<byte>.Shared.Return(body);
        }
    }

    internal static async ValueTask<BinaryClientReply> ReadPartialReplyAsync(Stream stream, IWireParser parser, int status, int size, byte[] scratch, ReadOnlyMemory<byte> prefix, CancellationToken cancellationToken)
    {
        if (CanReadLargeLookupReply(parser, status))
            return await ReadLargeLookupReplyAsync(stream, parser, size, scratch, cancellationToken, prefix).ConfigureAwait(false);

        var body = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            prefix.CopyTo(body);
            await stream.ReadExactlyAsync(body.AsMemory(prefix.Length, size - prefix.Length), cancellationToken).ConfigureAwait(false);
            return ParseReply(parser, status, body.AsSpan(0, size));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(body);
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<BinaryClientReply> ReadLargeLookupReplyAsync(Stream stream, IWireParser parser, int size, byte[] scratch, CancellationToken cancellationToken, ReadOnlyMemory<byte> prefix = default)
    {
        if (prefix.IsEmpty)
        {
            await stream.ReadExactlyAsync(scratch.AsMemory(0, 1), cancellationToken).ConfigureAwait(false);
            prefix = scratch.AsMemory(0, 1);
        }

        var flags = prefix.Span[0];
        var distributed = ReferenceEquals(parser, GetDistributedAsyncResponse.Parser);
        if (distributed || GetValueAsyncResponse.HasRawPayload(flags))
        {
            var found = distributed && DecodeDistributedFlag(flags);
            var payload = GC.AllocateUninitializedArray<byte>(size - 1);
            var received = prefix[1..];
            received.CopyTo(payload);
            await stream.ReadExactlyAsync(payload.AsMemory(received.Length), cancellationToken).ConfigureAwait(false);
            WireMessage response = distributed
                ? GetDistributedAsyncResponse.FromOwnedPayload(found, payload)
                : GetValueAsyncResponse.FromOwnedRawPayload(flags, payload);
            return new BinaryClientReply(response, null);
        }

        var body = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            prefix.CopyTo(body);
            await stream.ReadExactlyAsync(body.AsMemory(prefix.Length, size - prefix.Length), cancellationToken).ConfigureAwait(false);
            return ParseReply(parser, 0, body.AsSpan(0, size));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(body);
        }
    }

    internal static bool CanReadLargeLookupReply(IWireParser parser, int status) =>
        status == 0 && (ReferenceEquals(parser, GetDistributedAsyncResponse.Parser) || ReferenceEquals(parser, GetValueAsyncResponse.Parser));

    private static async Task<(TcpClient Socket, Stream Stream, bool Replay, bool Extended)> ConnectAsync(
        Uri endpoint, Func<CancellationToken, ValueTask<string>>? provider, SslClientAuthenticationOptions? tlsOptions, bool extended, CancellationToken token)
    {
        var socket = new TcpClient { NoDelay = true };
        Stream? stream = null;
        try
        {
            await socket.ConnectAsync(endpoint.IdnHost, endpoint.Port, token).ConfigureAwait(false);
            stream = socket.GetStream();
            if (string.Equals(endpoint.Scheme, Uri.UriSchemeHttps, StringComparison.Ordinal))
            {
                var tls = new SslStream(stream);
                stream = tls;
                await tls.AuthenticateAsClientAsync(CreateTlsOptions(endpoint, tlsOptions), token).ConfigureAwait(false);
            }

            var supportsMutationReplay = await NegotiateAsync(stream, provider, extended, token).ConfigureAwait(false);
            return (socket, stream, supportsMutationReplay, extended);
        }
        catch
        {
            if (stream != null)
                await stream.DisposeAsync().ConfigureAwait(false);
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

    private static async Task<bool> NegotiateAsync(Stream tls, Func<CancellationToken, ValueTask<string>>? provider, bool extended, CancellationToken token)
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
