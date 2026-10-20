using System;
using System.Collections.Generic;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies outbound cluster mTLS handlers complete TLS handshakes with Kestrel internal listeners.</summary>
[Immutable]
public sealed class MtlsKestrelHandshakeTests : ServerUnitTestBase
{
    /// <summary>Ensures a trusted peer client certificate can complete TLS against the internal mTLS listener.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OutboundMtlsHandshakesInternalListener(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        var held = ListenPortPool.ServerUnitTests.HoldPort();
        try
        {
            await using var host = await MtlsInternalListenerHost.StartAsync(bundle, held.Port, "node-b", "node-a", cancellationToken);

            using var tcpClient = new TcpClient();
            await tcpClient.ConnectAsync("127.0.0.1", held.Port, cancellationToken);
            await using var sslStream = new SslStream(tcpClient.GetStream(), false);
            await host.AuthenticateClientAsync(sslStream, cancellationToken);

            _ = await Assert.That(sslStream.IsAuthenticated).IsTrue();
            _ = await Assert.That(sslStream.RemoteCertificate).IsNotNull();
        }
        finally
        {
            held.Dispose();
        }
    }

    /// <summary>Ensures many new connections between the same two nodes build the peer chain once on each side.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepeatedHandshakesBuildOneChainPerSide(CancellationToken cancellationToken)
    {
        const int connectionCount = 4;
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        var held = ListenPortPool.ServerUnitTests.HoldPort();
        try
        {
            await using var host = await MtlsInternalListenerHost.StartAsync(bundle, held.Port, "node-b", "node-a", cancellationToken);

            for (var i = 0; i < connectionCount; i++)
            {
                using var tcpClient = new TcpClient();
                await tcpClient.ConnectAsync("127.0.0.1", held.Port, cancellationToken);
                await using var sslStream = new SslStream(tcpClient.GetStream(), false);
                await host.AuthenticateClientAsync(sslStream, cancellationToken);
                _ = await Assert.That(sslStream.IsAuthenticated).IsTrue();

                // TLS 1.3 completes the client side before the server validates the client certificate; wait for it so handshakes do not overlap.
                await host.WaitForInboundValidationAsync(cancellationToken);
            }

            _ = await Assert.That(host.InboundChains.ChainBuilds).IsEqualTo(1L);
            _ = await Assert.That(host.OutboundChains.ChainBuilds).IsEqualTo(1L);
        }
        finally
        {
            held.Dispose();
        }
    }

    private sealed class MtlsInternalListenerHost : IAsyncDisposable
    {
        private static readonly List<SslApplicationProtocol> ClientApplicationProtocols =
        [
            SslApplicationProtocol.Http2,
            SslApplicationProtocol.Http11,
        ];

        private static readonly string[] ExpectedInboundPeerNodeIds = ["node-a"];

        private readonly X509CertificateCollection _clientCertificates;
        private readonly SemaphoreSlim _inboundValidations = new(0);
        private readonly RemoteCertificateValidationCallback _validateRemoteServer;
        private WebApplication? _application;

        private MtlsInternalListenerHost(X509Certificate2 serverCertificate, X509Certificate2 clientCertificate, X509Certificate2 trustAnchor, string serverNodeId)
        {
            ServerCertificate = serverCertificate;
            ClientCertificate = clientCertificate;
            TrustAnchor = trustAnchor;
            ServerNodeId = serverNodeId;
            _clientCertificates = [clientCertificate];
            _validateRemoteServer = ValidateRemoteServer;
            InboundChains = new PeerChainCache(trustAnchor, PeerChainCache.DefaultCapacity, PeerChainCache.DefaultMaxAge);
            OutboundChains = new PeerChainCache(trustAnchor, PeerChainCache.DefaultCapacity, PeerChainCache.DefaultMaxAge);
        }

        internal PeerChainCache InboundChains { get; }

        internal PeerChainCache OutboundChains { get; }

        private X509Certificate2 ClientCertificate { get; }

        private X509Certificate2 ServerCertificate { get; }

        private string ServerNodeId { get; }

        private X509Certificate2 TrustAnchor { get; }

        public async ValueTask DisposeAsync()
        {
            if (_application != null)
            {
                await _application.DisposeAsync();
                _application = null;
            }

            _inboundValidations.Dispose();
            ServerCertificate.Dispose();
            ClientCertificate.Dispose();
            TrustAnchor.Dispose();
        }

        internal static async Task<MtlsInternalListenerHost> StartAsync(
            MtlsTestCertificateBundle bundle,
            int internalPort,
            string serverNodeId,
            string clientNodeId,
            CancellationToken cancellationToken)
        {
            using var peerServerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, serverNodeId);
            using var peerClientCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, clientNodeId);
            var host = new MtlsInternalListenerHost(
                LoadExportableCertificate(peerServerCertificate),
                LoadExportableCertificate(peerClientCertificate),
                X509CertificateLoader.LoadCertificateFromFile(bundle.CaPath),
                serverNodeId);
            var kestrelConfigurator = new KestrelListenConfigurator(internalPort, host);

            var builder = WebApplication.CreateBuilder();
            _ = builder.WebHost.ConfigureKestrel(kestrelConfigurator.Apply);
            var application = builder.Build();
            try
            {
                ListenPortPool.ServerUnitTests.ReleasePort(internalPort);
                await application.StartAsync(cancellationToken);
                host._application = application;
                return host;
            }
            catch
            {
                await application.DisposeAsync().ConfigureAwait(false);
                await host.DisposeAsync().ConfigureAwait(false);
                throw;
            }

            static X509Certificate2 LoadExportableCertificate(X509Certificate2 certificate)
            {
                return X509CertificateLoader.LoadPkcs12(certificate.Export(X509ContentType.Pfx), null, X509KeyStorageFlags.Exportable);
            }
        }

        internal Task AuthenticateClientAsync(SslStream sslStream, CancellationToken cancellationToken) => sslStream.AuthenticateAsClientAsync(
            new SslClientAuthenticationOptions
            {
                TargetHost = ServerNodeId,
                ClientCertificates = _clientCertificates,
                ApplicationProtocols = ClientApplicationProtocols,
                EnabledSslProtocols = SslProtocols.None,
                RemoteCertificateValidationCallback = _validateRemoteServer,
            },
            cancellationToken);

        internal Task WaitForInboundValidationAsync(CancellationToken cancellationToken) => _inboundValidations.WaitAsync(cancellationToken);

        private void ConfigureHttps(HttpsConnectionAdapterOptions https)
        {
            https.ServerCertificate = ServerCertificate;
            https.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
            https.ClientCertificateValidation = ValidateInboundClient;
        }

        private void ConfigureListenOptions(ListenOptions listenOptions)
        {
            listenOptions.Protocols = HttpProtocols.Http1AndHttp2;
            _ = listenOptions.UseHttps(ConfigureHttps);
        }

        private bool ValidateInboundClient(X509Certificate2? certificate, X509Chain? chain, SslPolicyErrors errors)
        {
            try
            {
                return certificate != null
                       && InboundChains.TryVerify(certificate, TimeProvider.System.GetUtcNow(), out var verdict)
                       && MtlsTopology.IsRemotePeer(ExpectedInboundPeerNodeIds, verdict.NodeId);
            }
            finally
            {
                _ = _inboundValidations.Release();
            }
        }

        private bool ValidateRemoteServer(object sender, X509Certificate? certificate, X509Chain? chain, SslPolicyErrors errors)
        {
            if (certificate == null)
                return false;

            using var copy = new X509Certificate2(certificate);
            return OutboundChains.TryVerify(copy, TimeProvider.System.GetUtcNow(), out var verdict) && string.Equals(verdict.NodeId, ServerNodeId, StringComparison.Ordinal);
        }

        [Immutable]
        private sealed class KestrelListenConfigurator
        {
            private readonly MtlsInternalListenerHost _host;
            private readonly int _port;

            internal KestrelListenConfigurator(int port, MtlsInternalListenerHost host)
            {
                _port = port;
                _host = host;
                Apply = ApplyCore;
            }

            internal Action<KestrelServerOptions> Apply { get; }

            private void ApplyCore(KestrelServerOptions kestrel) => kestrel.ListenLocalhost(_port, _host.ConfigureListenOptions);
        }
    }
}
