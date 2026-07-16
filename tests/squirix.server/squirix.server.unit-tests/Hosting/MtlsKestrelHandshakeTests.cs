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
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies outbound cluster mTLS handlers complete TLS handshakes with Kestrel internal listeners.</summary>
public sealed class MtlsKestrelHandshakeTests : ServerUnitTestBase
{
    /// <summary>Ensures a trusted peer client certificate can complete TLS against the internal mTLS listener.</summary>
    [Fact]
    public async Task OutboundMtlsHandlerHandshakeInternalListener()
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(DefaultCancellationToken);
        var internalPort = ListenPortPool.ServerUnitTests.AllocatePort();
        await using var host = await MtlsInternalListenerHost.StartAsync(bundle, internalPort, "node-b", "node-a", DefaultCancellationToken);

        using var tcpClient = new TcpClient();
        await tcpClient.ConnectAsync("127.0.0.1", internalPort, DefaultCancellationToken);
        await using var sslStream = new SslStream(tcpClient.GetStream(), false);
        await host.AuthenticateClientAsync(sslStream, DefaultCancellationToken);

        Assert.True(sslStream.IsAuthenticated);
        Assert.True(sslStream.RemoteCertificate is not null);
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
        }

        private X509Certificate2 ClientCertificate { get; }

        private X509Certificate2 TrustAnchor { get; }

        private X509Certificate2 ServerCertificate { get; }

        private string ServerNodeId { get; }

        private X509Certificate2 TrustAnchor { get; }

        public async ValueTask DisposeAsync()
        {
            if (_application is not null)
            {
                await _application.DisposeAsync();
                _application = null;
            }

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
            var kestrelConfigurer = new KestrelListenConfigurer(internalPort, host);

            var builder = WebApplication.CreateBuilder();
            _ = builder.WebHost.ConfigureKestrel(kestrelConfigurer.Apply);
            var application = builder.Build();
            await application.StartAsync(cancellationToken);
            host._application = application;
            return host;

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

        private bool ValidateInboundClient(X509Certificate2? certificate, X509Chain? chain, SslPolicyErrors errors) =>
            MtlsClientCertificateValidator.ValidateForConfiguredRemotePeer(certificate, TrustAnchor, ["node-a"]);

        private bool ValidateRemoteServer(object sender, X509Certificate? certificate, X509Chain? chain, SslPolicyErrors errors) =>
            MtlsTestCertificates.ValidatePeerServerCertificate(certificate, TrustAnchor, ServerNodeId);
    }
}
