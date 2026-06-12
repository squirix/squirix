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
    /// <param name="material">Optional loaded cluster mTLS material.</param>
    /// <returns>A handler suitable for secure gRPC transport.</returns>
    public static HttpMessageHandler CreateChannelHandler(MtlsCertificateMaterial? material = null) =>
        material is { Enabled: true } ? CreateMtlsHandler(material) : new SocketsHttpHandler();

    /// <summary>
    /// Creates an outbound cluster mTLS HTTP handler that presents the local node certificate.
    /// </summary>
    /// <param name="material">Loaded cluster mTLS certificate material.</param>
    /// <returns>A handler configured for inter-node mutual TLS.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="material" /> is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when cluster mTLS material is not loaded.</exception>
    public static SocketsHttpHandler CreateMtlsHandler(MtlsCertificateMaterial material)
    {
        ArgumentNullException.ThrowIfNull(material);
        if (!material.Enabled || material.NodeCertificate is null || material.TrustAnchor is null)
            throw new InvalidOperationException("Cluster mTLS material must be loaded before creating the outbound handler.");

        var nodeCertificate = material.NodeCertificate;
        var trustAnchor = material.TrustAnchor;

        return new SocketsHttpHandler
        {
            SslOptions = new SslClientAuthenticationOptions
            {
                ClientCertificates = [nodeCertificate],
                RemoteCertificateValidationCallback = (_, certificate, chain, _) => ValidatePeerServerCertificate(certificate, chain, trustAnchor),
            },
        };
    }

    /// <summary>
    /// Validates a peer server certificate against the configured cluster trust root.
    /// </summary>
    /// <param name="serverCertificate">The presented peer server certificate.</param>
    /// <param name="chain">Optional chain supplied by the TLS stack.</param>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <returns><see langword="true" /> when the certificate is trusted for inter-node traffic.</returns>
    internal static bool ValidatePeerServerCertificate(X509Certificate? serverCertificate, X509Chain? chain, X509Certificate2 trustAnchor)
    {
        if (serverCertificate is null)
            return false;

        using var certificate = new X509Certificate2(serverCertificate);
        return MtlsClientCertificateValidator.Validate(certificate, chain, trustAnchor);
    }
}
