using System;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies the TLS handshake of the internal mTLS listener against a native listener.</summary>
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
            await using var application = Start(bundle, server, held, log, cancellationToken);

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
            await using var application = Start(bundle, server, held, log, cancellationToken);

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

    /// <summary>Verifies the listener negotiates the versioned peer protocol.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ListenerNegotiatesPeerProtocol(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var client = LoadExportable(MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b"));
        using var server = LoadExportable(MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a"));
        var held = ListenPortPool.ServerUnitTests.HoldPort();
        try
        {
            await using var application = Start(bundle, server, held, new EventRecordingLogger(), cancellationToken);

            using var tcpClient = new TcpClient();
            await tcpClient.ConnectAsync("127.0.0.1", held.Port, cancellationToken);
            await using var sslStream = new SslStream(tcpClient.GetStream(), false);
            await sslStream.AuthenticateAsClientAsync(CreateClientOptions(client, CreateChains(bundle)), cancellationToken);

            _ = await Assert.That(sslStream.NegotiatedApplicationProtocol).IsEqualTo(PeerFrames.Protocol);
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
            await using var application = Start(bundle, server, held, new EventRecordingLogger(), cancellationToken);

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
            await using var application = Start(bundle, server, held, log, cancellationToken);

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
            await using var application = Start(bundle, server, held, new EventRecordingLogger(), cancellationToken);

            using var tcp = new TcpClient();
            await tcp.ConnectAsync(IPAddress.Loopback, held.Port, cancellationToken);
            await using var stream = new SslStream(tcp.GetStream());
            await stream.AuthenticateAsClientAsync(CreateClientOptions(client, serverChains), cancellationToken);
            using var reader = new StreamReader(stream, Encoding.UTF8);
            var subject = await reader.ReadToEndAsync(cancellationToken);

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
        ApplicationProtocols = [PeerFrames.Protocol],
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

    private static TestListener Start(MtlsTestCertificateBundle bundle, X509Certificate2 serverCertificate, HeldPort held, EventRecordingLogger log, CancellationToken cancellationToken)
    {
        var verifier = new PeerHandshakeVerifier(CreateChains(bundle), TimeProvider.System, log);
        var handshake = new InboundPeerHandshake(verifier, serverCertificate, ConfiguredPeers);
        ListenPortPool.ServerUnitTests.ReleasePort(held.Port);
        return new TestListener(held.Port, handshake, log, cancellationToken);
    }

    private static async Task<bool> AuthenticateAsync(SslStream sslStream, SslClientAuthenticationOptions options, CancellationToken cancellationToken)
    {
        try
        {
            await sslStream.AuthenticateAsClientAsync(options, cancellationToken);

            // Under TLS 1.3 the client finishes its handshake before the server checks the client certificate, so a refusal can also
            // arrive as a closed stream; an accepted connection sends the authenticated subject.
            return await sslStream.ReadAsync(new byte[1], cancellationToken) > 0;
        }
        catch (Exception error) when (error is AuthenticationException or IOException)
        {
            return false;
        }
    }

    private sealed class TestListener : IAsyncDisposable
    {
        private readonly CancellationTokenSource _stop;
        private readonly Task _run;
        private readonly Socket _socket = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

        internal TestListener(int port, InboundPeerHandshake handshake, EventRecordingLogger log, CancellationToken cancellationToken)
        {
            _stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _socket.Bind(new IPEndPoint(IPAddress.Loopback, port));
            _socket.Listen(4);
            var listener = new NativeTlsListener(_socket, handshake.CreateServerOptions, SendSubjectAsync, log, 4);
            _run = listener.RunAsync(_stop.Token, _stop.Token);
        }

        public async ValueTask DisposeAsync()
        {
            await _stop.CancelAsync();
            await _run;
            _socket.Dispose();
            _stop.Dispose();
        }

        private static async Task SendSubjectAsync(SslStream stream, IPEndPoint remote, CancellationToken graceful, CancellationToken abort)
        {
            var bytes = Encoding.UTF8.GetBytes(stream.RemoteCertificate?.Subject ?? "none");
            await stream.WriteAsync(bytes, abort);
        }
    }
}
