using System;
using System.IO;
using System.Net.Http;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies the TLS handshake of the internal mTLS listener against a real Kestrel listener.</summary>
[Immutable]
public sealed class InboundPeerHandshakeTests : ServerUnitTestBase
{
    private const int InboundEventId = 5009;
    private const int LogTimeoutSeconds = 30;
    private const string LoopbackEndpointFragment = "from 127.0.0.1:";
    private static readonly string[] ConfiguredPeers = ["node-b"];

    /// <summary>Verifies a client certificate of a configured peer completes the handshake and nothing is logged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConfiguredPeerCompletesHandshake(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var client = LoadExportable(MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b"));
        using var server = LoadExportable(MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a"));
        var log = new EventRecordingLogger();
        var held = ListenPortPool.ServerUnitTests.HoldPort();
        try
        {
            await using var application = await StartAsync(bundle, server, held, log, cancellationToken);

            using var tcpClient = new TcpClient();
            await tcpClient.ConnectAsync("127.0.0.1", held.Port, cancellationToken);
            await using var sslStream = new SslStream(tcpClient.GetStream(), false);
            await sslStream.AuthenticateAsClientAsync(CreateClientOptions(client, CreateChains(bundle)), cancellationToken);

            _ = await Assert.That(sslStream.IsMutuallyAuthenticated).IsTrue();
            _ = await Assert.That(log.Count(InboundEventId)).IsEqualTo(0);
        }
        finally
        {
            held.Dispose();
        }
    }

    /// <summary>Verifies a refused client certificate is logged with the remote endpoint of the connection.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedClientIsLoggedWithRemoteEndpoint(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var unknown = LoadExportable(MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-x"));
        using var server = LoadExportable(MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a"));
        var log = new EventRecordingLogger();
        var held = ListenPortPool.ServerUnitTests.HoldPort();
        try
        {
            await using var application = await StartAsync(bundle, server, held, log, cancellationToken);

            using var tcpClient = new TcpClient();
            await tcpClient.ConnectAsync("127.0.0.1", held.Port, cancellationToken);
            await using var sslStream = new SslStream(tcpClient.GetStream(), false);
            _ = await AuthenticateAsync(sslStream, CreateClientOptions(unknown, CreateChains(bundle)), cancellationToken);
            await log.WhenLoggedAsync(InboundEventId).WaitAsync(TimeSpan.FromSeconds(LogTimeoutSeconds), TimeProvider.System, cancellationToken);

            _ = await Assert.That(log.Count(InboundEventId, LoopbackEndpointFragment)).IsEqualTo(1);
            _ = await Assert.That(log.Count(InboundEventId, "'node-x'")).IsEqualTo(1);
        }
        finally
        {
            held.Dispose();
        }
    }

    /// <summary>Verifies the listener negotiates HTTP/2 although the handshake options name no application protocols.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ListenerNegotiatesHttp2(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var client = LoadExportable(MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b"));
        using var server = LoadExportable(MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a"));
        var held = ListenPortPool.ServerUnitTests.HoldPort();
        try
        {
            await using var application = await StartAsync(bundle, server, held, new EventRecordingLogger(), cancellationToken);

            using var tcpClient = new TcpClient();
            await tcpClient.ConnectAsync("127.0.0.1", held.Port, cancellationToken);
            await using var sslStream = new SslStream(tcpClient.GetStream(), false);
            await sslStream.AuthenticateAsClientAsync(CreateClientOptions(client, CreateChains(bundle)), cancellationToken);

            _ = await Assert.That(sslStream.NegotiatedApplicationProtocol).IsEqualTo(SslApplicationProtocol.Http2);
        }
        finally
        {
            held.Dispose();
        }
    }

    /// <summary>Verifies a client that presents no certificate is refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ClientWithoutCertificateIsRefused(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var server = LoadExportable(MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a"));
        var held = ListenPortPool.ServerUnitTests.HoldPort();
        try
        {
            await using var application = await StartAsync(bundle, server, held, new EventRecordingLogger(), cancellationToken);

            using var tcpClient = new TcpClient();
            await tcpClient.ConnectAsync("127.0.0.1", held.Port, cancellationToken);
            await using var sslStream = new SslStream(tcpClient.GetStream(), false);
            var completed = await AuthenticateAsync(sslStream, CreateClientOptions(null, CreateChains(bundle)), cancellationToken);

            _ = await Assert.That(completed).IsFalse();
        }
        finally
        {
            held.Dispose();
        }
    }

    /// <summary>Verifies a client certificate of another authority is refused and logged with the chain reason.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ClientOfOtherAuthorityIsRefused(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var other = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var untrusted = LoadExportable(MtlsTestCertificateFactory.CreatePeerCertificate(other.Ca, "node-b"));
        using var server = LoadExportable(MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a"));
        var log = new EventRecordingLogger();
        var held = ListenPortPool.ServerUnitTests.HoldPort();
        try
        {
            await using var application = await StartAsync(bundle, server, held, log, cancellationToken);

            using var tcpClient = new TcpClient();
            await tcpClient.ConnectAsync("127.0.0.1", held.Port, cancellationToken);
            await using var sslStream = new SslStream(tcpClient.GetStream(), false);
            _ = await AuthenticateAsync(sslStream, CreateClientOptions(untrusted, CreateChains(bundle)), cancellationToken);
            await log.WhenLoggedAsync(InboundEventId).WaitAsync(TimeSpan.FromSeconds(LogTimeoutSeconds), TimeProvider.System, cancellationToken);

            _ = await Assert.That(log.Count(InboundEventId, "cluster trust root")).IsEqualTo(1);
            _ = await Assert.That(log.Count(InboundEventId, LoopbackEndpointFragment)).IsEqualTo(1);
        }
        finally
        {
            held.Dispose();
        }
    }

    /// <summary>Verifies the client certificate of an accepted connection reaches the request pipeline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AcceptedClientCertificateReachesRequest(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var client = LoadExportable(MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b"));
        using var server = LoadExportable(MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a"));
        var serverChains = CreateChains(bundle);
        var held = ListenPortPool.ServerUnitTests.HoldPort();
        try
        {
            await using var application = await StartAsync(bundle, server, held, new EventRecordingLogger(), cancellationToken);

            using var handler = new SocketsHttpHandler();
            handler.SslOptions.ClientCertificates = [client];
            handler.SslOptions.RemoteCertificateValidationCallback = (_, certificate, _, _) => certificate != null && TrustsServer(serverChains, certificate);
            using var http = new HttpClient(handler);
            var subject = await http.GetStringAsync(new Uri($"https://localhost:{held.Port}/client"), cancellationToken);

            _ = await Assert.That(subject).IsEqualTo(client.Subject);
        }
        finally
        {
            held.Dispose();
        }
    }

    /// <summary>Verifies every connection of a handshake shares one server certificate context, so the server chain is not rebuilt per handshake.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ServerCertificateContextIsShared(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var server = LoadExportable(MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a"));
        var verifier = new PeerHandshakeVerifier(CreateChains(bundle), TimeProvider.System, new EventRecordingLogger());
        var handshake = new InboundPeerHandshake(verifier, server, ConfiguredPeers);

        var first = handshake.CreateServerOptions(null);
        var second = handshake.CreateServerOptions(null);

        _ = await Assert.That(first.ServerCertificateContext).IsNotNull();
        _ = await Assert.That(ReferenceEquals(first.ServerCertificateContext, second.ServerCertificateContext)).IsTrue();
        _ = await Assert.That(first.ClientCertificateRequired).IsTrue();
    }

    private static PeerChainCache CreateChains(MtlsTestCertificateBundle bundle) => new(bundle.Ca, PeerChainCache.DefaultCapacity, PeerChainCache.DefaultMaxAge);

    private static SslClientAuthenticationOptions CreateClientOptions(X509Certificate2? client, PeerChainCache serverChains) => new()
    {
        TargetHost = "localhost",
        ClientCertificates = client == null ? null : [client],
        ApplicationProtocols = [SslApplicationProtocol.Http2],
        EnabledSslProtocols = SslProtocols.None,
        RemoteCertificateValidationCallback = (_, certificate, _, _) => certificate != null && TrustsServer(serverChains, certificate),
    };

    private static bool TrustsServer(PeerChainCache serverChains, X509Certificate certificate)
    {
        using var copy = new X509Certificate2(certificate);
        return serverChains.TryVerify(copy, TimeProvider.System.GetUtcNow(), out _);
    }

    private static X509Certificate2 LoadExportable(X509Certificate2 certificate)
    {
        using (certificate)
            return X509CertificateLoader.LoadPkcs12(certificate.Export(X509ContentType.Pfx), null, X509KeyStorageFlags.Exportable);
    }

    private static async Task<WebApplication> StartAsync(MtlsTestCertificateBundle bundle, X509Certificate2 serverCertificate, HeldPort held, EventRecordingLogger log, CancellationToken cancellationToken)
    {
        var verifier = new PeerHandshakeVerifier(new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, PeerChainCache.DefaultMaxAge), TimeProvider.System, log);
        var handshake = InboundPeerHandshake.Create(verifier, serverCertificate, ConfiguredPeers);
        var builder = WebApplication.CreateBuilder();
        _ = builder.WebHost.ConfigureKestrel(kestrel => kestrel.ListenLocalhost(held.Port, listenOptions => listenOptions.UseHttps(handshake)));
        var application = builder.Build();
        _ = application.MapGet("/client", static context => context.Response.WriteAsync(context.Connection.ClientCertificate?.Subject ?? "none", context.RequestAborted));
        try
        {
            ListenPortPool.ServerUnitTests.ReleasePort(held.Port);
            await application.StartAsync(cancellationToken);
            return application;
        }
        catch
        {
            await application.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static async Task<bool> AuthenticateAsync(SslStream sslStream, SslClientAuthenticationOptions options, CancellationToken cancellationToken)
    {
        try
        {
            await sslStream.AuthenticateAsClientAsync(options, cancellationToken);
            _ = await sslStream.ReadAsync(new byte[1], cancellationToken);
            return true;
        }
        catch (Exception error) when (error is AuthenticationException or IOException)
        {
            return false;
        }
    }
}
