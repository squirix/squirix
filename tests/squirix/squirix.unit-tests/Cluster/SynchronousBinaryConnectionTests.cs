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
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests.Cluster;

/// <summary>Exercises blocking request lifetimes against a local TLS peer.</summary>
public sealed class SynchronousBinaryConnectionTests
{
    /// <summary>Retires a stalled synchronous connection only when every active caller has timed out.</summary>
    /// <param name="cancelBoth">Whether the two pending callers share the expiring attempt token.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The test listener did not bind an IP endpoint.</exception>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SynchronousTimeoutPreservesLiveCalls(bool cancelBoth, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(20));
        var token = timeout.Token;
        using var privateKey = RSA.Create(2048);
        var certificateRequest = new CertificateRequest("CN=localhost", privateKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var ephemeral = certificateRequest.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddMinutes(20));
        using var certificate = X509CertificateLoader.LoadPkcs12(ephemeral.Export(X509ContentType.Pfx), null, X509KeyStorageFlags.Exportable);
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        if (listener.LocalEndpoint is not IPEndPoint endpoint)
            throw new InvalidOperationException("The TLS listener has no IP endpoint.");

        var uri = new UriBuilder(Uri.UriSchemeHttps, "localhost", endpoint.Port).Uri;
        var connecting = ConnectSynchronouslyAsync(uri, certificate.Thumbprint, token);
        using var socket = await listener.AcceptTcpClientAsync(token);
        await using var stream = new SslStream(socket.GetStream());
        await NegotiateAsync(stream, certificate, token);
        using var connection = await connecting;
        await CheckTimeoutsAsync(connection, stream, cancelBoth, timeout);
    }

    private static async Task CheckTimeoutsAsync(SynchronousBinaryConnection connection, SslStream stream, bool cancelBoth, CancellationTokenSource timeout)
    {
        var token = timeout.Token;
        using var expired = CancellationTokenSource.CreateLinkedTokenSource(token);
        Task<GetDistributedAsyncResponse>? first = null;
        Task<GetDistributedAsyncResponse>? second = null;
        try
        {
            first = SendSynchronousGetAsync(connection, "first", expired.Token, token);
            var firstId = await ReadDistributedGetIdAsync(stream, token);
            second = SendSynchronousGetAsync(connection, "second", cancelBoth ? expired.Token : token, token);
            var secondId = await ReadDistributedGetIdAsync(stream, token);
            await expired.CancelAsync();

            _ = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, GetDistributedAsyncResponse>(new ValueTask<GetDistributedAsyncResponse>(first), TimeSpan.FromSeconds(5), TimeProvider.System, token);
            if (cancelBoth)
            {
                _ = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, GetDistributedAsyncResponse>(new ValueTask<GetDistributedAsyncResponse>(second), TimeSpan.FromSeconds(5), TimeProvider.System, token);
                _ = await Assert.That(connection.IsAvailable).IsFalse();
                return;
            }

            _ = await Assert.That(connection.IsAvailable).IsTrue();
            await ReplyDistributedGetAsync(stream, firstId, false, token);
            await ReplyDistributedGetAsync(stream, secondId, true, token);
            _ = await Assert.That((await second.WaitAsync(token)).Found).IsTrue();
        }
        finally
        {
            await timeout.CancelAsync();
            await DrainAsync(first);
            await DrainAsync(second);
        }
    }

    private static async Task DrainAsync(Task? call)
    {
        if (call != null)
            await call.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    private static Task<SynchronousBinaryConnection> ConnectSynchronouslyAsync(Uri uri, string thumbprint, CancellationToken token)
    {
        var tls = new SslClientAuthenticationOptions
        {
            RemoteCertificateValidationCallback = (_, remote, _, _) => string.Equals(remote?.GetCertHashString(), thumbprint, StringComparison.Ordinal),
        };
        return Task.Factory.StartNew(() => SynchronousBinaryConnection.Connect(uri, null, tls, token), token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }

    private static Task<GetDistributedAsyncResponse> SendSynchronousGetAsync(SynchronousBinaryConnection connection, string key, CancellationToken attempt, CancellationToken lifetime)
    {
        var request = new GetDistributedAsyncRequest { CacheName = "default", Key = key };
        var prepared = BinaryClientProtocol.PrepareRequest(BinaryOperation.GetDistributed, request);
        return Task.Factory.StartNew(
            () => connection.Send(BinaryOperation.GetDistributed, prepared, GetDistributedAsyncResponse.Parser, DateTimeOffset.UtcNow.AddSeconds(20), attempt, lifetime),
            lifetime,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
    }

    private static async Task<int> ReadDistributedGetIdAsync(SslStream stream, CancellationToken cancellationToken)
    {
        var header = GC.AllocateUninitializedArray<byte>(Frames.HeaderBytes);
        await stream.ReadExactlyAsync(header, cancellationToken);
        var (id, size, control) = Frames.Read(header, Frames.MaxRequestBytes);
        _ = await Assert.That(Frames.ReadOperationId(control)).IsEqualTo(Unsafe.BitCast<BinaryOperation, int>(BinaryOperation.GetDistributed));
        var body = GC.AllocateUninitializedArray<byte>(size);
        await stream.ReadExactlyAsync(body, cancellationToken);
        return id;
    }

    private static ValueTask ReplyDistributedGetAsync(SslStream stream, int id, bool found, CancellationToken cancellationToken)
    {
        var reply = new GetDistributedAsyncResponse { Found = found };
        var size = reply.CalculateSize();
        var frame = GC.AllocateUninitializedArray<byte>(Frames.HeaderBytes + size);
        Frames.WriteMessage(frame, id, size, 0, reply);
        return stream.WriteAsync(frame, cancellationToken);
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
}
