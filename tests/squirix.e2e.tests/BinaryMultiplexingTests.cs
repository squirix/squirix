using System;
using System.Buffers.Binary;
using System.Net;
using System.Net.Http;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Mtls;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>Covers reply pairing on a real SDK connection with an explicitly scheduled TLS peer.</summary>
public sealed class BinaryMultiplexingTests
{
    /// <summary>A late reply for a cancelled Get leaves the next replies intact on the same socket.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The test listener did not bind an IP endpoint.</exception>
    [Test]
    public async Task CancelledGetKeepsConnectionUsable(CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(20));
        var token = timeout.Token;
        using var certificate = CreateTlsCertificate();
        var thumbprint = certificate.Thumbprint;
        using var handler = new SocketsHttpHandler();
        handler.SslOptions.RemoteCertificateValidationCallback = (_, remote, _, _) =>
            string.Equals(remote?.GetCertHashString(), thumbprint, StringComparison.Ordinal);
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        if (listener.LocalEndpoint is not IPEndPoint endpoint)
            throw new InvalidOperationException("The TLS listener has no IP endpoint.");

        var uri = new UriBuilder(Uri.UriSchemeHttps, "localhost", endpoint.Port).Uri;
        var options = new SquirixClientOptions();
        options.Endpoints.Add(uri);

        var connecting = SquirixClient.ConnectAsync(options, handler, token);
        using var socket = await listener.AcceptTcpClientAsync(token);
        await using var stream = new SslStream(socket.GetStream());
        await NegotiateAsync(stream, certificate, token);

        await using var client = await connecting;
        var cache = await client.GetCacheAsync<string>("default", token);
        using var cancelled = CancellationTokenSource.CreateLinkedTokenSource(token);
        var first = cache.GetValueAsync("first", cancelled.Token);
        var (firstId, firstKey) = await ReadGetAsync(stream, token);
        _ = await Assert.That(firstKey).IsEqualTo("first");
        await cancelled.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(
            first, TimeSpan.FromSeconds(10), TimeProvider.System, token);

        var second = cache.GetValueAsync("second", token);
        var (secondId, secondKey) = await ReadGetAsync(stream, token);
        _ = await Assert.That(secondKey).IsEqualTo("second");
        _ = await Assert.That(secondId).IsNotEqualTo(firstId);
        await ReplyAsync(stream, secondId, "second value", token);
        _ = await Assert.That((await second).Value).IsEqualTo("second value");

        var third = cache.GetValueAsync("third", token);
        var (thirdId, thirdKey) = await ReadGetAsync(stream, token);
        _ = await Assert.That(thirdKey).IsEqualTo("third");
        _ = await Assert.That(thirdId).IsNotEqualTo(firstId);
        await ReplyAsync(stream, firstId, "late first value", token);
        await ReplyAsync(stream, thirdId, "third value", token);
        _ = await Assert.That((await third).Value).IsEqualTo("third value");
    }

    /// <summary>A mutation whose reply is lost on a single-copy connection is reported as ambiguous.</summary>
    /// <param name="closeConnection">Whether to close the connection or leave the request unanswered.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The test listener did not bind an IP endpoint.</exception>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task UnconfirmedMutationDoesNotRetry(bool closeConnection, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(15));
        var token = timeout.Token;
        using var certificate = CreateTlsCertificate();
        var thumbprint = certificate.Thumbprint;
        using var handler = new SocketsHttpHandler();
        handler.SslOptions.RemoteCertificateValidationCallback = (_, remote, _, _) =>
            string.Equals(remote?.GetCertHashString(), thumbprint, StringComparison.Ordinal);
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        if (listener.LocalEndpoint is not IPEndPoint endpoint)
            throw new InvalidOperationException("The TLS listener has no IP endpoint.");

        var options = new SquirixClientOptions();
        options.Endpoints.Add(new UriBuilder(Uri.UriSchemeHttps, "localhost", endpoint.Port).Uri);
        var connecting = SquirixClient.ConnectAsync(options, handler, token);
        using var socket = await listener.AcceptTcpClientAsync(token);
        await using var stream = new SslStream(socket.GetStream());
        await NegotiateAsync(stream, certificate, token);
        await using var client = await connecting;
        var cache = await client.GetCacheAsync<string>("default", token);

        var write = cache.SetAsync("lost-reply", "written", cancellationToken: token);
        var header = GC.AllocateUninitializedArray<byte>(Frames.HeaderBytes);
        await stream.ReadExactlyAsync(header, token);
        var (_, size, control) = Frames.Read(header, Frames.MaxRequestBytes);
        _ = await Assert.That(Frames.ReadOperationId(control)).IsEqualTo(Unsafe.BitCast<BinaryOperation, int>(BinaryOperation.SetEntry));
        var body = GC.AllocateUninitializedArray<byte>(size);
        await stream.ReadExactlyAsync(body, token);
        var request = (control & Frames.TracedRequest) == 0
            ? SetEntryAsyncRequest.Parser.ParseFrom(body)
            : SetEntryAsyncRequest.Parser.ParseFrom(BinaryTracedRequest.Parser.ParseFrom(body).Payload);
        _ = await Assert.That(request.Key).IsEqualTo("lost-reply");
        if (closeConnection)
            socket.Close();

        _ = await NodeAsyncAssert.ThrowsAsync<CommitOutcomeUnknownException>(write, TimeSpan.FromSeconds(5), TimeProvider.System, token);
        _ = await Assert.That(listener.Pending()).IsFalse();
    }

    private static X509Certificate2 CreateTlsCertificate()
    {
        using var authority = TestCertificates.CreateStandaloneCertificateAuthority();
        using var issued = TestCertificates.CreatePeerCertificate(authority, "localhost");
        return TestCertificates.LoadExportableCertificate(issued);
    }

    private static async Task NegotiateAsync(SslStream stream, X509Certificate2 certificate, CancellationToken cancellationToken)
    {
        await stream.AuthenticateAsServerAsync(new SslServerAuthenticationOptions { ServerCertificate = certificate }, cancellationToken);
        var preface = GC.AllocateUninitializedArray<byte>(Frames.PrefaceBytes);
        await stream.ReadExactlyAsync(preface, cancellationToken);
        _ = await Assert.That(BinaryPrimitives.ReadUInt32BigEndian(preface)).IsEqualTo(Frames.Preface);
        preface[0] = Frames.Accepted;
        await stream.WriteAsync(preface.AsMemory(0, 1), cancellationToken);
    }

    private static async Task<(int Id, string Key)> ReadGetAsync(SslStream stream, CancellationToken cancellationToken)
    {
        var header = GC.AllocateUninitializedArray<byte>(Frames.HeaderBytes);
        await stream.ReadExactlyAsync(header, cancellationToken);
        var (id, size, control) = Frames.Read(header, Frames.MaxRequestBytes);
        var operation = Unsafe.BitCast<int, BinaryOperation>((control & ~Frames.TracedRequest) >>> 16);
        _ = await Assert.That(operation).IsEqualTo(BinaryOperation.GetValue);
        var body = GC.AllocateUninitializedArray<byte>(size);
        await stream.ReadExactlyAsync(body, cancellationToken);
        var request = (control & Frames.TracedRequest) == 0
            ? GetValueAsyncRequest.Parser.ParseFrom(body)
            : GetValueAsyncRequest.Parser.ParseFrom(BinaryTracedRequest.Parser.ParseFrom(body).Payload);
        return (id, request.Key);
    }

    private static Task ReplyAsync(SslStream stream, int id, string value, CancellationToken cancellationToken)
    {
        var reply = new GetValueAsyncResponse { Found = true, Value = new CacheValue { StringValue = value } };
        var size = reply.CalculateSize();
        var frame = GC.AllocateUninitializedArray<byte>(Frames.HeaderBytes + size);
        Frames.WriteMessage(frame, id, size, 0, reply);
        return stream.WriteAsync(frame, cancellationToken).AsTask();
    }
}
