using System;
using System.Buffers.Binary;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Internal.Cluster.Transport;
using Squirix.TestKit;
using Squirix.Transport;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests.Cluster;

/// <summary>Verifies that completing a caller does not retire the wire slot prematurely.</summary>
public sealed class AsyncBinaryRemoveTests
{
    /// <summary>Completes the caller promptly and preserves a live call across a late reply.</summary>
    /// <param name="callerCancels">Whether cancellation comes directly from the caller.</param>
    /// <param name="keepOtherCall">Whether another request still needs the connection.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The listener has no IP endpoint.</exception>
    [Test]
    [Arguments(true, true)]
    [Arguments(false, true)]
    [Arguments(false, false)]
    public async Task RemoveCompletionPreservesWireLifetime(bool callerCancels, bool keepOtherCall, CancellationToken cancellationToken)
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
        await NegotiateAsync(stream, certificate, token);
        await connecting;
        await VerifyLifetimeAsync(client, stream, callerCancels, keepOtherCall, token);
    }

    private static async Task VerifyLifetimeAsync(BinaryCacheClient client, SslStream stream, bool callerCancels, bool keepOtherCall, CancellationToken token)
    {
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(token);
        using var attempt = CancellationTokenSource.CreateLinkedTokenSource(caller.Token);
        var deadline = DateTime.UtcNow.AddSeconds(3);
        var first = RemoveAsync(client, "first", deadline, attempt.Token, caller.Token);
        var firstId = await ReadRemoveAsync(stream, "first", token);
        var second = keepOtherCall ? RemoveAsync(client, "second", DateTime.UtcNow.AddSeconds(15), token, token) : null;
        var secondId = second == null ? -1 : await ReadRemoveAsync(stream, "second", token);

        if (callerCancels)
        {
            await caller.CancelAsync();
            var failure = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, RemoveAsyncResponse>(new ValueTask<RemoveAsyncResponse>(first), TimeSpan.FromSeconds(5), TimeProvider.System, token);
            _ = await Assert.That(failure.CancellationToken).IsEqualTo(attempt.Token);
        }
        else
        {
            var remaining = deadline - DateTime.UtcNow;
            attempt.CancelAfter(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
            var failure = await AsyncAssert.ThrowsAsync<SquirixTransportException, RemoveAsyncResponse>(new ValueTask<RemoveAsyncResponse>(first), TimeSpan.FromSeconds(5), TimeProvider.System, token);
            _ = await Assert.That(failure.Status.Detail).IsEqualTo(CommitOutcomeUnknownException.StableDetail);
        }

        if (second == null)
        {
            var endOfFile = await stream.ReadAsync(new byte[1], token);
            _ = await Assert.That(endOfFile).IsEqualTo(0);
            return;
        }

        _ = await Assert.That(second.IsCompleted).IsFalse();
        await ReplyRemoveAsync(stream, firstId, false, token);
        await ReplyRemoveAsync(stream, secondId, true, token);
        _ = await Assert.That((await second.WaitAsync(token)).Removed).IsTrue();

        var followUp = RemoveAsync(client, "follow-up", DateTime.UtcNow.AddSeconds(15), token, token);
        var followUpId = await ReadRemoveAsync(stream, "follow-up", token);
        await ReplyRemoveAsync(stream, followUpId, true, token);
        _ = await Assert.That((await followUp.WaitAsync(token)).Removed).IsTrue();
    }

    private static Task<RemoveAsyncResponse> RemoveAsync(BinaryCacheClient client, string key, DateTime deadline, CancellationToken attempt, CancellationToken caller)
    {
        var request = new RemoveAsyncRequest { CacheName = "default", Key = key };
        var options = new BinaryCallOptions(deadline, attempt, caller);
        return client.RemoveAsync(request, in options);
    }

    private static async Task<int> ReadRemoveAsync(SslStream stream, string key, CancellationToken token)
    {
        var header = GC.AllocateUninitializedArray<byte>(Frames.HeaderBytes);
        await stream.ReadExactlyAsync(header, token);
        var (id, size, control) = Frames.Read(header, Frames.MaxRequestBytes);
        _ = await Assert.That(Frames.ReadOperationId(control)).IsEqualTo(Unsafe.BitCast<BinaryOperation, int>(BinaryOperation.Remove));
        var body = GC.AllocateUninitializedArray<byte>(size);
        await stream.ReadExactlyAsync(body, token);
        var request = (control & Frames.TracedRequest) == 0
            ? RemoveAsyncRequest.Parser.ParseFrom(body)
            : RemoveAsyncRequest.Parser.ParseFrom(BinaryTracedRequest.Parser.ParseFrom(body).Payload);
        _ = await Assert.That(request.Key).IsEqualTo(key);
        return id;
    }

    private static ValueTask ReplyRemoveAsync(SslStream stream, int id, bool removed, CancellationToken token)
    {
        var reply = new RemoveAsyncResponse { Removed = removed };
        var size = reply.CalculateSize();
        var frame = GC.AllocateUninitializedArray<byte>(Frames.HeaderBytes + size);
        Frames.WriteMessage(frame, id, size, 0, reply);
        return stream.WriteAsync(frame, token);
    }

    private static async Task NegotiateAsync(SslStream stream, X509Certificate2 certificate, CancellationToken token)
    {
        await stream.AuthenticateAsServerAsync(new SslServerAuthenticationOptions { ServerCertificate = certificate }, token);
        var preface = GC.AllocateUninitializedArray<byte>(Frames.PrefaceBytes);
        await stream.ReadExactlyAsync(preface, token);
        _ = await Assert.That(BinaryPrimitives.ReadUInt32BigEndian(preface)).IsEqualTo(Frames.Preface);
        preface[0] = Frames.Accepted;
        await stream.WriteAsync(preface.AsMemory(0, 1), token);
    }
}
