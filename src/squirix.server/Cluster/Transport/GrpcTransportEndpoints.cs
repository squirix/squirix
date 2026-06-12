using System;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Squirix.Server.Cluster.Transport;

/// <summary>
/// Validates and configures gRPC transport endpoints for server-to-server transport.
/// </summary>
internal static class GrpcTransportEndpoints
{
    /// <summary>
    /// Creates the default HTTP handler for HTTPS gRPC channels.
    /// </summary>
    /// <returns>A handler suitable for secure gRPC transport.</returns>
    public static HttpMessageHandler CreateChannelHandler() => new SocketsHttpHandler();

    /// <summary>
    /// Creates an outbound cluster mTLS HTTP handler that presents the local node certificate.
    /// </summary>
    /// <param name="material">Loaded cluster mTLS certificate material.</param>
    /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
    /// <returns>A handler configured for inter-node mutual TLS.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="material" /> is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when cluster mTLS material is not loaded.</exception>
    public static SocketsHttpHandler CreateMtlsHandler(MtlsCertificateMaterial material, string expectedPeerNodeId)
    {
        ArgumentNullException.ThrowIfNull(material);
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedPeerNodeId);
        if (!material.Enabled || material.NodeCertificate is null || material.TrustAnchor is null)
            throw new InvalidOperationException("Cluster mTLS material must be loaded before creating the outbound handler.");

        return CreateMtlsHandler(material.NodeCertificate, material.TrustAnchor, expectedPeerNodeId);
    }

    /// <summary>
    /// Creates an outbound cluster mTLS HTTP handler with explicit client certificate material.
    /// </summary>
    /// <param name="clientCertificate">Client certificate presented to the peer.</param>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
    /// <returns>A handler configured for inter-node mutual TLS.</returns>
    public static SocketsHttpHandler CreateMtlsHandler(X509Certificate2 clientCertificate, X509Certificate2 trustAnchor, string expectedPeerNodeId)
    {
        ArgumentNullException.ThrowIfNull(clientCertificate);
        ArgumentNullException.ThrowIfNull(trustAnchor);
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedPeerNodeId);

        return new SocketsHttpHandler
        {
            UseProxy = false,
            EnableMultipleHttp2Connections = true,
            SslOptions = new SslClientAuthenticationOptions
            {
                ClientCertificates = [clientCertificate],
                ApplicationProtocols = [SslApplicationProtocol.Http2, SslApplicationProtocol.Http11],
                RemoteCertificateValidationCallback = (_, certificate, _, _) => ValidatePeerServerCertificate(certificate, trustAnchor, expectedPeerNodeId),
            },
        };
    }

    /// <summary>
    /// Validates a peer server certificate against the configured cluster trust root.
    /// </summary>
    /// <param name="serverCertificate">The presented peer server certificate.</param>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
    /// <returns><see langword="true" /> when the certificate is trusted for inter-node traffic.</returns>
    internal static bool ValidatePeerServerCertificate(X509Certificate? serverCertificate, X509Certificate2 trustAnchor, string expectedPeerNodeId)
    {
        if (serverCertificate is null)
            return false;

        using var certificate = new X509Certificate2(serverCertificate);
        return MtlsClientCertificateValidator.ValidateForExpectedNodeId(certificate, trustAnchor, expectedPeerNodeId);
    }
}
