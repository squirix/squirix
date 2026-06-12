using System;
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
using Squirix.Server.Node.Hosting;
using Squirix.Server.TestKit.Http;
using Squirix.Server.UnitTests.Cluster.Transport;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Hosting;

/// <summary>
/// Verifies outbound cluster mTLS handlers complete TLS handshakes with Kestrel internal listeners.
/// </summary>
public sealed class MtlsKestrelHandshakeTests
{
    /// <summary>
    /// Ensures a trusted peer client certificate can complete TLS against the internal mTLS listener.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task OutboundMtlsHandlerCompletesTlsHandshakeWithInternalListener()
    {
        using var bundle = MtlsTestCertificateFactory.Create();
        var internalPort = new PortAllocator(35000, 35999).Allocate();
        await using var host = await MtlsInternalListenerHost.StartAsync(bundle, internalPort, "node-b", "node-a", TestContext.Current.CancellationToken);

        Assert.True(SquirixKestrelConfiguration.ValidateClientCertificate(host.ClientCertificate, host.TrustAnchor));

        using var tcpClient = new TcpClient();
        await tcpClient.ConnectAsync("127.0.0.1", internalPort, TestContext.Current.CancellationToken);
        await using var sslStream = new SslStream(tcpClient.GetStream(), false);
        await host.AuthenticateClientAsync(sslStream, TestContext.Current.CancellationToken);

        Assert.True(sslStream.IsAuthenticated);
        Assert.True(sslStream.RemoteCertificate is not null);
    }

    private static X509Certificate2 LoadExportableCertificate(X509Certificate2 certificate) =>
        X509CertificateLoader.LoadPkcs12(certificate.Export(X509ContentType.Pfx), null, X509KeyStorageFlags.Exportable);

    private sealed class MtlsInternalListenerHost : IAsyncDisposable
    {
        private WebApplication? _application;

        private MtlsInternalListenerHost(X509Certificate2 serverCertificate, X509Certificate2 clientCertificate, X509Certificate2 trustAnchor, string serverNodeId)
        {
            ServerCertificate = serverCertificate;
            ClientCertificate = clientCertificate;
            TrustAnchor = trustAnchor;
            ServerNodeId = serverNodeId;
        }

        public X509Certificate2 ClientCertificate { get; }

        public X509Certificate2 TrustAnchor { get; }

        private X509Certificate2 ServerCertificate { get; }

        private string ServerNodeId { get; }

        public static async Task<MtlsInternalListenerHost> StartAsync(
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

            var builder = WebApplication.CreateBuilder();
            _ = builder.WebHost.ConfigureKestrel(kestrel => kestrel.ListenLocalhost(internalPort, host.ConfigureListenOptions));
            var application = builder.Build();
            await application.StartAsync(cancellationToken);
            host._application = application;
            return host;
        }

        public Task AuthenticateClientAsync(SslStream sslStream, CancellationToken cancellationToken) => sslStream.AuthenticateAsClientAsync(
            new SslClientAuthenticationOptions
            {
                TargetHost = ServerNodeId,
                ClientCertificates = [ClientCertificate],
                ApplicationProtocols = [SslApplicationProtocol.Http2, SslApplicationProtocol.Http11],
                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                RemoteCertificateValidationCallback = ValidateRemoteServer,
            },
            cancellationToken);

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
            SquirixKestrelConfiguration.ValidateClientCertificate(certificate, TrustAnchor);

        private bool ValidateRemoteServer(object sender, X509Certificate? certificate, X509Chain? chain, SslPolicyErrors errors) =>
            GrpcTransportEndpoints.ValidatePeerServerCertificate(certificate, TrustAnchor);
    }
}
