using System;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

/// <summary>
/// Centralizes Kestrel listen options and transport security for the squirix node process.
/// Invariants here affect TLS and optional mutual TLS — review carefully.
/// </summary>
internal static class SquirixKestrelConfiguration
{
    /// <summary>
    /// Configures Kestrel listeners: primary HTTPS (HTTP/1.1 and HTTP/2) and optional mTLS when <c>SQUIRIX_MTLS</c> is set.
    /// </summary>
    /// <param name="builder">The web application builder.</param>
    /// <param name="uri">The primary HTTPS listen URI.</param>
    public static void ConfigureKestrel(WebApplicationBuilder builder, Uri uri)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(uri);

        _ = builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.AddServerHeader = false;
            kestrel.ConfigureEndpointDefaults(static options => options.Protocols = HttpProtocols.Http1AndHttp2);

            var mtlsEnabled = EnvVariables.ReadBool("SQUIRIX_MTLS");

            var isLoopbackHost = SquirixExternalAccessSecurity.IsLoopbackHost(uri.Host);
            if (isLoopbackHost)
                kestrel.ListenLocalhost(uri.Port, ConfigurePrimaryEndpoint);
            else
                kestrel.ListenAnyIP(uri.Port, ConfigurePrimaryEndpoint);

            return;

            void ConfigurePrimaryEndpoint(ListenOptions listenOptions)
            {
                listenOptions.Protocols = HttpProtocols.Http1AndHttp2;

                if (!mtlsEnabled)
                {
                    _ = listenOptions.UseHttps();
                    return;
                }

                _ = listenOptions.UseHttps(ConfigureMutualTls);
            }
        });
        return;

        static void ConfigureMutualTls(HttpsConnectionAdapterOptions https)
        {
            https.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
            https.ClientCertificateValidation = static (cert, chain, _) => ValidateMutualTlsClientCertificate(cert, chain);
        }
    }

    /// <summary>
    /// Ensures the node URL uses HTTPS gRPC transport.
    /// </summary>
    /// <param name="cluster">Cluster configuration including the node URL.</param>
    /// <exception cref="InvalidOperationException">Thrown when the node URL uses plaintext HTTP.</exception>
    [SuppressMessage("ReSharper", "RedundantEmptySwitchSection", Justification = "Switch is used to throw exception")]
    public static void EnsureHttpsTransport(ClusterConfig cluster)
    {
        ArgumentNullException.ThrowIfNull(cluster);

        if (!Uri.TryCreate(cluster.Url, UriKind.Absolute, out var uri) ||
            !uri.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Squirix transport requires HTTPS. Plaintext 'http://' is not supported. Provided URL: {cluster.Url}");
        }
    }

    /// <summary>
    /// Validates an inbound mTLS client certificate against the platform trust store.
    /// </summary>
    /// <param name="cert">The presented client certificate.</param>
    /// <param name="chain">The certificate chain built by the TLS stack.</param>
    /// <returns><see langword="true" /> when the certificate chains to a trusted root; otherwise <see langword="false" />.</returns>
    internal static bool ValidateMutualTlsClientCertificate(X509Certificate2 cert, X509Chain? chain)
    {
        ArgumentNullException.ThrowIfNull(cert);

        try
        {
            if (chain is null)
                return false;

            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            return chain.Build(cert);
        }
        catch (CryptographicException)
        {
            return false;
        }
    }
}
