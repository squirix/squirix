using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Internal.Cluster.Transport;
using Squirix.TestKit;
using Squirix.Transport;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests.Cluster;

/// <summary>Exercises shared asynchronous request completion over a single TLS connection.</summary>
public sealed class AsyncBinaryCompletionTests
{
    internal static readonly LifetimeOperation<GetDistributedAsyncRequest, GetDistributedAsyncResponse> ReadLifetime = new(
        BinaryOperation.GetDistributed,
        new GetDistributedAsyncRequest { CacheName = "default", Key = "read" },
        new GetDistributedAsyncResponse(),
        new GetDistributedAsyncResponse { Found = true, Value = Encoding.UTF8.GetBytes("cached") },
        static (peer, request, options) => peer.GetDistributedAsync(request, in options).AsTask(),
        false);

    internal static readonly LifetimeOperation<RemoveAsyncRequest, RemoveAsyncResponse> MutationLifetime = new(
        BinaryOperation.Remove,
        new RemoveAsyncRequest { CacheName = "default", Key = "mutation" },
        new RemoveAsyncResponse(),
        new RemoveAsyncResponse { Removed = true },
        static (peer, request, options) => peer.RemoveAsync(request, in options).AsTask(),
        true);

    /// <summary>Checks every asynchronous wire operation against its request and response type.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task RoundTripsAllAsyncOperations(CancellationToken cancellationToken) =>
        WithPeerAsync(VerifyAllOperationsAsync, cancellationToken);

    /// <summary>Reads replies across both buffer boundaries without changing a previously returned payload.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task FragmentedRepliesSurviveBufferGrowth(CancellationToken cancellationToken) =>
        WithPeerAsync(VerifyFragmentedRepliesAsync, cancellationToken);

    /// <summary>Observes mutation identity on the actual connection and checks that retries reuse it.</summary>
    /// <param name="replay">Whether the server advertises mutation replay.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public Task MutationIdentityFollowsHandshake(bool replay, CancellationToken cancellationToken) =>
        WithPeerAsync((client, stream, token) => VerifyMutationIdentityAsync(client, stream, replay, token), cancellationToken, replay);

    /// <summary>Keeps prepared frames valid when a later attempt negotiates a different replay capability.</summary>
    /// <param name="traced">Whether the prepared frame contains an activity envelope.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task MutationFrameKeepsIdentityAcrossRetries(bool traced)
    {
        using var activity = new Activity(nameof(MutationFrameKeepsIdentityAcrossRetries));
        _ = activity.SetIdFormat(traced ? ActivityIdFormat.W3C : ActivityIdFormat.Hierarchical);
        _ = activity.Start();

        byte[] value = [0, 255, 1];
        var request = BinaryClientProtocol.CreateSetRequest("cache", "key", value, null, 100, 20);
        var singleCopy = BinaryClientProtocol.PrepareRequest(BinaryOperation.SetDistributed, request, false);
        _ = await Assert.That(request.OperationId).IsEmpty();
        var replicated = BinaryClientProtocol.PrepareRequest(BinaryOperation.SetDistributed, request, true);
        var singleCopyRetry = BinaryClientProtocol.PrepareRequest(BinaryOperation.SetDistributed, request, false);
        var replicatedRetry = BinaryClientProtocol.PrepareRequest(BinaryOperation.SetDistributed, request, true);

        _ = await Assert.That((replicated.Control & Frames.TracedRequest) != 0).IsEqualTo(traced);
        _ = await Assert.That(replicated.Size - singleCopy.Size).IsEqualTo(17);
        _ = await Assert.That(singleCopyRetry.Size).IsEqualTo(singleCopy.Size);
        _ = await Assert.That(Guid.TryParseExact(request.OperationId, "N", out _)).IsTrue();
        await AssertDistributedSetFrameAsync(singleCopy, string.Empty, value);
        await AssertDistributedSetFrameAsync(replicated, request.OperationId, value);
        await AssertDistributedSetFrameAsync(singleCopyRetry, string.Empty, value);
        await AssertDistributedSetFrameAsync(replicatedRetry, request.OperationId, value);
    }

    /// <summary>Checks that inline encoding preserves UTF-8, lengths and binary tails at the frame boundary.</summary>
    /// <param name="length">The size of the inline destination relative to the encoded message.</param>
    [Test]
    [Arguments(-1)]
    [Arguments(0)]
    [Arguments(1)]
    public async Task InlineFramesMatchTheFullEncoding(int length)
    {
        var payload = new byte[] { 11, 19, 0, 255 };
        var request = new SetDistributedAsyncRequest { CacheName = "кэш", Key = "項目😀", Value = payload };
        var expected = request.ToByteArray();
        var destination = new byte[expected.Length + length];
        var measured = request.WriteOrMeasure(destination, null, out var borrowsPayload);
        _ = await Assert.That(measured).IsEqualTo(expected.Length);
        _ = await Assert.That(borrowsPayload).IsTrue();
        if (length >= 0)
            _ = await Assert.That(destination.AsSpan(0, measured).SequenceEqual(expected)).IsTrue();
    }

    /// <summary>Checks cancellation, deadline outcomes, and connection reuse after a late reply.</summary>
    /// <param name="read">Whether the operation can be retried after a deadline.</param>
    /// <param name="callerCancels">Whether cancellation comes directly from the caller.</param>
    /// <param name="keepOtherCall">Whether another request still needs the connection.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false, true, true)]
    [Arguments(false, false, true)]
    [Arguments(false, false, false)]
    [Arguments(true, true, true)]
    [Arguments(true, false, true)]
    [Arguments(true, false, false)]
    public Task CompletionPreservesWireLifetime(bool read, bool callerCancels, bool keepOtherCall, CancellationToken cancellationToken) =>
        WithPeerAsync(
            (client, stream, token) => read
                ? VerifyLifetimeAsync(client, stream, ReadLifetime, callerCancels, keepOtherCall, token)
                : VerifyLifetimeAsync(client, stream, MutationLifetime, callerCancels, keepOtherCall, token),
            cancellationToken);

    internal static async Task WithPeerAsync(Func<BinaryCacheClient, SslStream, CancellationToken, Task> scenario, CancellationToken cancellationToken, bool replay = false)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(20));
        var token = timeout.Token;
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var ephemeral = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddMinutes(20));
        using var certificate = X509CertificateLoader.LoadPkcs12(ephemeral.Export(X509ContentType.Pfx), null, X509KeyStorageFlags.Exportable);
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        if (listener.LocalEndpoint is not IPEndPoint endpoint)
            throw new InvalidOperationException("The TLS listener has no IP endpoint.");

        var uri = new UriBuilder(Uri.UriSchemeHttps, "localhost", endpoint.Port).Uri;
        var thumbprint = certificate.Thumbprint;
        var tls = new SslClientAuthenticationOptions
        {
            RemoteCertificateValidationCallback = (_, remote, _, _) => string.Equals(remote?.GetCertHashString(), thumbprint, StringComparison.Ordinal),
        };
        await using var client = new BinaryCacheClient(uri, null, tls);
        var connecting = client.ConnectAsync(token);
        using var socket = await listener.AcceptTcpClientAsync(token);
        await using var stream = new SslStream(socket.GetStream());
        await NegotiateAsync(stream, certificate, replay, token);
        await connecting;
        await scenario(client, stream, token);
    }

    internal static async Task<int> ReadRequestAsync(SslStream stream, BinaryOperation operation, WireMessage expected, CancellationToken token)
    {
        var header = GC.AllocateUninitializedArray<byte>(Frames.RequestBytes);
        await stream.ReadExactlyAsync(header, token);
        var (id, size, control) = Frames.ReadRequest(header);
        _ = await Assert.That(Frames.ReadOperationId(control)).IsEqualTo(Unsafe.BitCast<BinaryOperation, int>(operation));
        var body = GC.AllocateUninitializedArray<byte>(size);
        await stream.ReadExactlyAsync(body, token);
        var payload = (control & Frames.TracedRequest) == 0 ? body : BinaryTracedRequest.Parser.ParseFrom(body).Payload;
        _ = await Assert.That(payload.Span.SequenceEqual(expected.ToByteArray())).IsTrue();
        return id;
    }

    internal static ValueTask ReplyAsync(SslStream stream, int id, WireMessage reply, CancellationToken token)
    {
        var size = reply.CalculateSize();
        var frame = GC.AllocateUninitializedArray<byte>(Frames.ReplyBytes + size);
        Frames.WriteReplyMessage(frame, id, size, 0, reply);
        return stream.WriteAsync(frame, token);
    }

    private static async Task NegotiateAsync(SslStream stream, X509Certificate2 certificate, bool replay, CancellationToken token)
    {
        await stream.AuthenticateAsServerAsync(new SslServerAuthenticationOptions { ServerCertificate = certificate }, token);
        var preface = GC.AllocateUninitializedArray<byte>(Frames.PrefaceBytes);
        await stream.ReadExactlyAsync(preface, token);
        _ = await Assert.That(BinaryPrimitives.ReadUInt32BigEndian(preface)).IsEqualTo(Frames.Preface);
        preface[0] = replay ? Frames.AcceptedWithMutationReplay : Frames.Accepted;
        await stream.WriteAsync(preface.AsMemory(0, 1), token);
    }

    private static async Task VerifyFragmentedRepliesAsync(BinaryCacheClient client, SslStream stream, CancellationToken token)
    {
        var request = new GetDistributedAsyncRequest { CacheName = "default", Key = "buffer" };
        var sizes = new[] { 255, 256, Frames.MaxBatchBytes - 1, Frames.MaxBatchBytes, 31 };
        var values = new byte[sizes.Length][];
        var replies = new GetDistributedAsyncResponse[sizes.Length];
        for (var i = 0; i < sizes.Length; i++)
        {
            var pending = client.GetDistributedAsync(request, LiveCallOptions(token)).AsTask();
            var id = await ReadRequestAsync(stream, BinaryOperation.GetDistributed, request, token);
            var expected = new byte[sizes[i]];
            Array.Fill(expected, byte.CreateChecked(i + 1));
            values[i] = expected;
            var message = new GetDistributedAsyncResponse { Found = true, Value = expected };
            var size = message.CalculateSize();
            var frame = new byte[Frames.ReplyBytes + size];
            Frames.WriteReplyMessage(frame, id, size, 0, message);
            var split = Frames.ReplyBytes + (size / 2);
            await stream.WriteAsync(frame.AsMemory(0, split), token);
            await stream.WriteAsync(frame.AsMemory(split), token);
            replies[i] = await pending.WaitAsync(token);
        }

        for (var i = 0; i < sizes.Length; i++)
        {
            _ = await Assert.That(replies[i].Found).IsTrue();
            _ = await Assert.That(replies[i].Value.Span.SequenceEqual(values[i])).IsTrue();
        }
    }

    private static async Task VerifyAllOperationsAsync(BinaryCacheClient client, SslStream stream, CancellationToken token)
    {
        await VerifyNativeReadsAsync(client, stream, token);
        await VerifyNativeMutationsAsync(client, stream, token);
        await VerifyDistributedOperationsAsync(client, stream, token);
    }

    private static async Task VerifyMutationIdentityAsync(BinaryCacheClient client, SslStream stream, bool replay, CancellationToken token)
    {
        var request = new RemoveAsyncRequest { CacheName = "default", Key = "mutation" };
        var reply = new RemoveAsyncResponse { Removed = true };
        await RoundTripAsync(client, stream, BinaryOperation.Remove, request, reply, static (peer, wire, options) => peer.RemoveAsync(wire, in options).AsTask(), token);

        var firstIdentity = request.OperationId;
        if (replay)
            _ = await Assert.That(Guid.TryParseExact(firstIdentity, "N", out _)).IsTrue();
        else
            _ = await Assert.That(firstIdentity).IsEmpty();

        await RoundTripAsync(client, stream, BinaryOperation.Remove, request, reply, static (peer, wire, options) => peer.RemoveAsync(wire, in options).AsTask(), token);
        _ = await Assert.That(request.OperationId).IsEqualTo(firstIdentity);
    }

    private static async Task AssertDistributedSetFrameAsync(BinaryClientRequest request, string operationId, byte[] value)
    {
        var frame = new byte[Frames.RequestBytes + request.Size];
        request.WriteTo(frame, 5);
        ReadOnlyMemory<byte> payload = frame.AsMemory(Frames.RequestBytes);
        if ((request.Control & Frames.TracedRequest) != 0)
            payload = BinaryTracedRequest.Parser.ParseFrom(payload.Span).Payload;

        var decoded = SetDistributedAsyncRequest.Parser.ParseFrom(payload.Span);
        _ = await Assert.That(decoded.OperationId).IsEqualTo(operationId);
        _ = await Assert.That(decoded.TtlTicks).IsEqualTo(100L);
        _ = await Assert.That(decoded.SlidingTicks).IsEqualTo(20L);
        _ = await Assert.That(decoded.Value.Span.SequenceEqual(value)).IsTrue();
    }

    private static async Task VerifyNativeReadsAsync(BinaryCacheClient client, SslStream stream, CancellationToken token)
    {
        await RoundTripAsync(
            client,
            stream,
            BinaryOperation.GetEntry,
            new GetEntryAsyncRequest { CacheName = "default", Key = "round-trip" },
            new GetEntryAsyncResponse { Found = true },
            static (peer, request, options) => peer.GetEntryAsync(request, in options).AsTask(),
            token);
        await RoundTripAsync(
            client,
            stream,
            BinaryOperation.GetValue,
            new GetValueAsyncRequest { CacheName = "default", Key = "round-trip" },
            new GetValueAsyncResponse { Found = true },
            static (peer, request, options) => peer.GetValueAsync(request, in options).AsTask(),
            token);
        await RoundTripAsync(
            client,
            stream,
            BinaryOperation.GetExpiration,
            new GetExpirationAsyncRequest { CacheName = "default", Key = "round-trip" },
            new GetExpirationAsyncResponse { Found = true },
            static (peer, request, options) => peer.GetExpirationAsync(request, in options).AsTask(),
            token);
    }

    private static async Task VerifyNativeMutationsAsync(BinaryCacheClient client, SslStream stream, CancellationToken token)
    {
        await RoundTripAsync(
            client,
            stream,
            BinaryOperation.SetEntry,
            new SetEntryAsyncRequest { CacheName = "default", Key = "round-trip" },
            new SetAsyncResponse(),
            static (peer, request, options) => peer.SetEntryAsync(request, in options).AsTask(),
            token);
        await RoundTripAsync(
            client,
            stream,
            BinaryOperation.Remove,
            new RemoveAsyncRequest { CacheName = "default", Key = "round-trip" },
            new RemoveAsyncResponse { Removed = true },
            static (peer, request, options) => peer.RemoveAsync(request, in options).AsTask(),
            token);
        await RoundTripAsync(
            client,
            stream,
            BinaryOperation.TryAddEntry,
            new TryAddEntryAsyncRequest { CacheName = "default", Key = "round-trip" },
            new TryAddAsyncResponse { Added = true },
            static (peer, request, options) => peer.AddEntryIfMissingAsync(request, in options).AsTask(),
            token);
        await RoundTripAsync(
            client,
            stream,
            BinaryOperation.Touch,
            new TouchAsyncRequest { CacheName = "default", Key = "round-trip" },
            new TouchAsyncResponse { Found = true },
            static (peer, request, options) => peer.TouchAsync(request, in options).AsTask(),
            token);
        await RoundTripAsync(
            client,
            stream,
            BinaryOperation.RemoveExpiration,
            new RemoveExpirationAsyncRequest { CacheName = "default", Key = "round-trip" },
            new RemoveExpirationAsyncResponse { Found = true },
            static (peer, request, options) => peer.RemoveExpirationAsync(request, in options).AsTask(),
            token);
        await RoundTripAsync(
            client,
            stream,
            BinaryOperation.Update,
            new UpdateAsyncRequest { CacheName = "default", Key = "round-trip" },
            new UpdateAsyncResponse { Updated = true },
            static (peer, request, options) => peer.UpdateAsync(request, in options).AsTask(),
            token);
        await RoundTripAsync(
            client,
            stream,
            BinaryOperation.GetOrAdd,
            new GetOrAddAsyncRequest { CacheName = "default", Key = "round-trip" },
            new GetOrAddAsyncResponse { Found = true, Added = true },
            static (peer, request, options) => peer.GetOrAddAsync(request, in options).AsTask(),
            token);
    }

    private static async Task VerifyDistributedOperationsAsync(BinaryCacheClient client, SslStream stream, CancellationToken token)
    {
        await RoundTripAsync(
            client,
            stream,
            BinaryOperation.SetDistributed,
            new SetDistributedAsyncRequest { CacheName = "default", Key = "round-trip" },
            new SetAsyncResponse(),
            static (peer, request, options) => peer.SetDistributedAsync(request, in options).AsTask(),
            token);
        await RoundTripAsync(
            client,
            stream,
            BinaryOperation.GetDistributed,
            new GetDistributedAsyncRequest { CacheName = "default", Key = "round-trip" },
            new GetDistributedAsyncResponse { Found = true },
            static (peer, request, options) => peer.GetDistributedAsync(request, in options).AsTask(),
            token);
        await RoundTripAsync(
            client,
            stream,
            BinaryOperation.RefreshDistributed,
            new RefreshDistributedAsyncRequest { CacheName = "default", Key = "round-trip" },
            new RefreshDistributedAsyncResponse(),
            static (peer, request, options) => peer.RefreshDistributedAsync(request, in options).AsTask(),
            token);
    }

    private static async Task VerifyLifetimeAsync<TRequest, TResponse>(
        BinaryCacheClient client,
        SslStream stream,
        LifetimeOperation<TRequest, TResponse> operation,
        bool callerCancels,
        bool keepOtherCall,
        CancellationToken token)
        where TRequest : WireMessage
        where TResponse : WireMessage
    {
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(token);
        using var attempt = CancellationTokenSource.CreateLinkedTokenSource(caller.Token);
        var deadline = DateTime.UtcNow.AddSeconds(3);
        var first = operation.Send(client, operation.Request, new BinaryCallOptions(deadline, attempt.Token, caller.Token));
        var firstId = await ReadRequestAsync(stream, operation.Code, operation.Request, token);
        var second = keepOtherCall ? operation.Send(client, operation.Request, LiveCallOptions(token)) : null;
        var secondId = second == null ? -1 : await ReadRequestAsync(stream, operation.Code, operation.Request, token);

        if (callerCancels)
        {
            await caller.CancelAsync();
        }
        else
        {
            var remaining = deadline - DateTime.UtcNow;
            attempt.CancelAfter(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
        }

        if (callerCancels || !operation.UnknownOnDeadline)
        {
            var failure = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, TResponse>(
                new ValueTask<TResponse>(first), TimeSpan.FromSeconds(5), TimeProvider.System, token);
            _ = await Assert.That(failure.CancellationToken).IsEqualTo(attempt.Token);
        }
        else
        {
            var failure = await AsyncAssert.ThrowsAsync<SquirixTransportException, TResponse>(
                new ValueTask<TResponse>(first), TimeSpan.FromSeconds(5), TimeProvider.System, token);
            _ = await Assert.That(failure.Status.Detail).IsEqualTo(CommitOutcomeUnknownException.StableDetail);
        }

        if (second == null)
        {
            var endOfFile = await stream.ReadAsync(new byte[1], token);
            _ = await Assert.That(endOfFile).IsEqualTo(0);
            return;
        }

        _ = await Assert.That(secondId).IsNotEqualTo(firstId);
        _ = await Assert.That(second.IsCompleted).IsFalse();
        await ReplyAsync(stream, firstId, operation.LateReply, token);
        await ReplyAsync(stream, secondId, operation.Reply, token);
        var result = await second.WaitAsync(token);
        _ = await Assert.That(result.ToByteArray().AsSpan().SequenceEqual(operation.Reply.ToByteArray())).IsTrue();
        await RoundTripAsync(client, stream, operation.Code, operation.Request, operation.Reply, operation.Send, token);
    }

    private static async Task RoundTripAsync<TRequest, TResponse>(
        BinaryCacheClient client,
        SslStream stream,
        BinaryOperation operation,
        TRequest request,
        TResponse reply,
        Func<BinaryCacheClient, TRequest, BinaryCallOptions, Task<TResponse>> send,
        CancellationToken token)
        where TRequest : WireMessage
        where TResponse : WireMessage
    {
        var pending = send(client, request, LiveCallOptions(token));
        var id = await ReadRequestAsync(stream, operation, request, token);
        await ReplyAsync(stream, id, reply, token);
        var result = await pending.WaitAsync(token);
        _ = await Assert.That(result.ToByteArray().AsSpan().SequenceEqual(reply.ToByteArray())).IsTrue();
    }

    private static BinaryCallOptions LiveCallOptions(CancellationToken token) => new(DateTime.UtcNow.AddSeconds(15), token, token);

    internal sealed record LifetimeOperation<TRequest, TResponse>(
        BinaryOperation Code,
        TRequest Request,
        TResponse LateReply,
        TResponse Reply,
        Func<BinaryCacheClient, TRequest, BinaryCallOptions, Task<TResponse>> Send,
        bool UnknownOnDeadline);
}
